"""Silver dim_country: REST Countries bronze -> SCD Type 2 Delta table.

Mirrors sql/silver/create_dim_country.sql:
1) stage latest bronze snapshot per country_cca2
2) close current rows whose row_hash changed
3) insert new countries and new versions of changed ones
"""

import os
import re
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Local runs can use .env; Databricks usually has no dotenv / no __file__.
if load_dotenv is not None:
    try:
        project_root = Path(__file__).resolve().parents[2]
        load_dotenv(project_root / ".env")
    except NameError:
        pass


def get_config():
    bronze_base_path = os.getenv("BRONZE_BASE_PATH")
    target_table = os.getenv("SILVER_DIM_COUNTRY_TABLE")

    if not bronze_base_path:
        raise ValueError("BRONZE_BASE_PATH must be configured.")

    if not target_table:
        raise ValueError("SILVER_DIM_COUNTRY_TABLE must be configured.")

    identifier = r"[A-Za-z_][A-Za-z0-9_]*"
    if not re.fullmatch(
        rf"{identifier}\.{identifier}\.{identifier}",
        target_table,
    ):
        raise ValueError(
            "SILVER_DIM_COUNTRY_TABLE must be catalog.schema.table."
        )

    bronze_path = (
        f"{bronze_base_path.rstrip('/')}/"
        "source=restcountries/entity=countries/"
    )
    return bronze_path, target_table


def assert_no_rows(df, check_name):
    if df.limit(1).count() > 0:
        raise ValueError(f"Data quality check failed: {check_name}")


def read_bronze_countries(spark, bronze_path):
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
        .filter(F.col("api_version") == "v5")
    )


def build_country_staging(df_bronze):
    """Explode, clean, keep latest row per country_cca2, add row_hash."""
    # Primary capital if present, else first. get() is 0-based and returns NULL
    # on empty arrays (element_at throws INVALID_ARRAY_INDEX).
    capital_name = F.expr(
        """
        nullif(
          trim(
            coalesce(
              get(filter(c.capitals, x -> x.attributes.primary = true), 0).name,
              get(c.capitals, 0).name
            )
          ),
          ''
        )
        """
    )
    capital_lat = F.expr(
        """
        cast(
          coalesce(
            get(filter(c.capitals, x -> x.attributes.primary = true), 0).coordinates.lat,
            get(c.capitals, 0).coordinates.lat
          ) as double
        )
        """
    )
    capital_lng = F.expr(
        """
        cast(
          coalesce(
            get(filter(c.capitals, x -> x.attributes.primary = true), 0).coordinates.lng,
            get(c.capitals, 0).coordinates.lng
          ) as double
        )
        """
    )

    df_exploded = (
        df_bronze.select(
            F.to_timestamp("fetched_at_utc").alias("source_fetched_at_utc"),
            F.explode("countries").alias("c"),
        )
        .filter(
            F.col("c.codes.alpha_2").isNotNull()
            & (F.trim(F.col("c.codes.alpha_2")) != "")
        )
        .select(
            "source_fetched_at_utc",
            F.nullif(
                F.upper(F.trim(F.col("c.codes.alpha_2"))),
                F.lit(""),
            ).alias("country_cca2"),
            F.upper(F.trim(F.col("c.codes.alpha_3"))).alias("country_cca3"),
            F.nullif(F.trim(F.col("c.names.common")), F.lit("")).alias(
                "name_common"
            ),
            F.nullif(F.trim(F.col("c.names.official")), F.lit("")).alias(
                "name_official"
            ),
            F.nullif(F.trim(F.col("c.region")), F.lit("")).alias("region"),
            F.nullif(F.trim(F.col("c.subregion")), F.lit("")).alias(
                "subregion"
            ),
            capital_name.alias("capital"),
            capital_lat.alias("capital_latitude"),
            capital_lng.alias("capital_longitude"),
            F.col("c.coordinates.lat").cast("double").alias("country_latitude"),
            F.col("c.coordinates.lng").cast("double").alias("country_longitude"),
            F.col("c.population").cast("bigint").alias("population"),
            F.col("c.area.kilometers").cast("double").alias("area_km2"),
        )
        .filter(F.col("country_cca2").isNotNull())
    )

    window = Window.partitionBy("country_cca2").orderBy(
        F.col("source_fetched_at_utc").desc()
    )

    df_latest = (
        df_exploded.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    hash_payload = F.concat_ws(
        "||",
        F.coalesce(F.col("country_cca3"), F.lit("")),
        F.coalesce(F.col("name_common"), F.lit("")),
        F.coalesce(F.col("name_official"), F.lit("")),
        F.coalesce(F.col("region"), F.lit("")),
        F.coalesce(F.col("subregion"), F.lit("")),
        F.coalesce(F.col("capital"), F.lit("")),
        F.coalesce(F.col("capital_latitude").cast("string"), F.lit("")),
        F.coalesce(F.col("capital_longitude").cast("string"), F.lit("")),
        F.coalesce(F.col("country_latitude").cast("string"), F.lit("")),
        F.coalesce(F.col("country_longitude").cast("string"), F.lit("")),
        F.coalesce(F.col("population").cast("string"), F.lit("")),
        F.coalesce(F.col("area_km2").cast("string"), F.lit("")),
    )

    return df_latest.select(
        "country_cca2",
        "country_cca3",
        "name_common",
        "name_official",
        "region",
        "subregion",
        "capital",
        "capital_latitude",
        "capital_longitude",
        "country_latitude",
        "country_longitude",
        "population",
        "area_km2",
        F.md5(hash_payload).alias("row_hash"),
        F.col("source_fetched_at_utc").cast("date").alias("load_date"),
        "source_fetched_at_utc",
    )


def validate_staging(df_staging):
    if df_staging.limit(1).count() == 0:
        raise ValueError("Data quality check failed: empty dim_country staging")

    missing = df_staging.filter(
        F.col("country_cca2").isNull()
        | (F.trim("country_cca2") == "")
        | F.col("row_hash").isNull()
        | F.col("load_date").isNull()
        | F.col("source_fetched_at_utc").isNull()
    )
    assert_no_rows(missing, "Missing required dim_country staging fields")

    duplicate_keys = (
        df_staging.groupBy("country_cca2")
        .count()
        .filter(F.col("count") > 1)
    )
    assert_no_rows(duplicate_keys, "Duplicate country_cca2 in staging")


def create_dim_country_table(spark, target_table):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            country_cca2 STRING,
            country_cca3 STRING,
            name_common STRING,
            name_official STRING,
            region STRING,
            subregion STRING,
            capital STRING,
            capital_latitude DOUBLE,
            capital_longitude DOUBLE,
            country_latitude DOUBLE,
            country_longitude DOUBLE,
            population BIGINT,
            area_km2 DOUBLE,
            row_hash STRING,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            source_fetched_at_utc TIMESTAMP
        )
        USING DELTA
        """
    )


def close_changed_current_rows(spark, df_staging, target_table):
    """SCD2 step 1: close current rows when attributes (row_hash) changed."""
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(
            df_staging.alias("s"),
            """
            t.country_cca2 = s.country_cca2
            AND t.is_current = true
            """,
        )
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={
                "valid_to": "s.load_date",
                "is_current": F.lit(False),
            },
        )
        .execute()
    )


def insert_new_and_changed_versions(spark, df_staging, target_table):
    """SCD2 step 2: insert rows with no current version (new or re-opened)."""
    df_current = (
        spark.table(target_table)
        .filter(F.col("is_current"))
        .select("country_cca2")
    )

    df_to_insert = (
        df_staging.alias("s")
        .join(
            df_current.alias("t"),
            F.col("s.country_cca2") == F.col("t.country_cca2"),
            how="left_anti",
        )
        .select(
            F.col("s.country_cca2"),
            F.col("s.country_cca3"),
            F.col("s.name_common"),
            F.col("s.name_official"),
            F.col("s.region"),
            F.col("s.subregion"),
            F.col("s.capital"),
            F.col("s.capital_latitude"),
            F.col("s.capital_longitude"),
            F.col("s.country_latitude"),
            F.col("s.country_longitude"),
            F.col("s.population"),
            F.col("s.area_km2"),
            F.col("s.row_hash"),
            F.col("s.load_date").alias("valid_from"),
            F.lit("9999-12-31").cast("date").alias("valid_to"),
            F.lit(True).alias("is_current"),
            F.col("s.source_fetched_at_utc"),
        )
    )

    (
        df_to_insert.write.format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    bronze_path, target_table = get_config()

    df_bronze = read_bronze_countries(spark, bronze_path)
    if df_bronze.limit(1).count() == 0:
        raise ValueError(
            "Data quality check failed: empty REST Countries bronze input"
        )

    df_staging = build_country_staging(df_bronze)
    df_staging = df_staging.localCheckpoint(eager=True)
    validate_staging(df_staging)

    create_dim_country_table(spark, target_table)
    close_changed_current_rows(spark, df_staging, target_table)
    insert_new_and_changed_versions(spark, df_staging, target_table)

    print(f"dim_country SCD2 completed. table={target_table}")


if __name__ == "__main__":
    main()
