"""One-off backfill: set silver.opensky_states.country_cca2 from origin_country.

Env (same as OpenSky silver transform):
  SILVER_OPENSKY_TABLE
  SILVER_DIM_COUNTRY_TABLE
  ORIGIN_COUNTRY_OVERRIDES_PATH
  TRANSFORMATION_CODE_PATH  — folder containing country_cca2_mapping.py
"""

import os
import re
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    try:
        project_root = Path(__file__).resolve().parents[2]
        load_dotenv(project_root / ".env")
    except NameError:
        pass

# Folder with country_cca2_mapping.py (Volume / ADLS / local).
_code_path = os.getenv("TRANSFORMATION_CODE_PATH")
if not _code_path:
    raise ValueError("TRANSFORMATION_CODE_PATH must be configured.")
sys.path.insert(0, _code_path.rstrip("/"))

from country_cca2_mapping import (  # noqa: E402
    load_origin_country_overrides,
    with_country_cca2,
)


def get_config():
    target_table = os.getenv("SILVER_OPENSKY_TABLE")
    dim_country_table = os.getenv("SILVER_DIM_COUNTRY_TABLE")
    overrides_path = os.getenv("ORIGIN_COUNTRY_OVERRIDES_PATH")

    if not target_table:
        raise ValueError("SILVER_OPENSKY_TABLE must be configured.")
    if not dim_country_table:
        raise ValueError("SILVER_DIM_COUNTRY_TABLE must be configured.")
    if not overrides_path:
        raise ValueError("ORIGIN_COUNTRY_OVERRIDES_PATH must be configured.")

    identifier = r"[A-Za-z_][A-Za-z0-9_]*"
    table_name_re = rf"{identifier}\.{identifier}\.{identifier}"
    if not re.fullmatch(table_name_re, target_table):
        raise ValueError("SILVER_OPENSKY_TABLE must be catalog.schema.table.")
    if not re.fullmatch(table_name_re, dim_country_table):
        raise ValueError(
            "SILVER_DIM_COUNTRY_TABLE must be catalog.schema.table."
        )

    return (
        target_table,
        dim_country_table,
        load_origin_country_overrides(overrides_path),
    )


def ensure_country_cca2_column(spark, target_table):
    if "country_cca2" not in spark.table(target_table).columns:
        spark.sql(
            f"ALTER TABLE {target_table} ADD COLUMNS (country_cca2 STRING)"
        )
        print(f"Added column country_cca2 to {target_table}")


def main():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    target_table, dim_country_table, overrides = get_config()
    ensure_country_cca2_column(spark, target_table)

    df_existing = spark.table(target_table).select(
        "snapshot_time_unix",
        "icao24",
        "origin_country",
    )
    df_mapped = with_country_cca2(
        df_existing,
        spark,
        dim_country_table,
        overrides=overrides,
    ).select(
        "snapshot_time_unix",
        "icao24",
        "country_cca2",
    )

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(
            df_mapped.alias("s"),
            """
            t.snapshot_time_unix = s.snapshot_time_unix
            AND t.icao24 = s.icao24
            """,
        )
        .whenMatchedUpdate(
            set={"country_cca2": "s.country_cca2"},
        )
        .execute()
    )

    coverage = (
        spark.table(target_table)
        .agg(
            F.count("*").alias("rows"),
            F.sum(
                F.when(
                    F.col("origin_country").isNotNull()
                    & F.col("country_cca2").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("mapped_with_origin"),
            F.sum(
                F.when(
                    F.col("origin_country").isNotNull()
                    & F.col("country_cca2").isNull(),
                    1,
                ).otherwise(0)
            ).alias("unmapped_with_origin"),
            F.sum(
                F.when(F.col("origin_country").isNull(), 1).otherwise(0)
            ).alias("null_origin"),
        )
        .collect()[0]
    )
    print(
        "Backfill country_cca2 completed. "
        f"rows={coverage['rows']} "
        f"mapped_with_origin={coverage['mapped_with_origin']} "
        f"unmapped_with_origin={coverage['unmapped_with_origin']} "
        f"null_origin={coverage['null_origin']}"
    )

    still_null = (
        spark.table(target_table)
        .filter(
            F.col("origin_country").isNotNull() & F.col("country_cca2").isNull()
        )
        .select("origin_country")
        .distinct()
        .orderBy("origin_country")
        .collect()
    )
    if still_null:
        names = ", ".join(row["origin_country"] for row in still_null)
        print(f"WARNING: still unmapped origin_country values: {names}")
    else:
        print("All non-null origin_country values have country_cca2.")


if __name__ == "__main__":
    main()
