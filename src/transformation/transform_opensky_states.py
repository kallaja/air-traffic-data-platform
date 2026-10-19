import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Local runs can use .env; Databricks usually has no dotenv / no __file__.
# On the cluster, set env vars on the notebook or job instead.
if load_dotenv is not None:
    try:
        project_root = Path(__file__).resolve().parents[2]
        load_dotenv(project_root / ".env")
    except NameError:
        pass

# Matches destination_path from ingestion metadata (relative bronze path).
BRONZE_RELATIVE_PATH_RE = r"(source=opensky/entity=states/.*)"


def get_config():
    bronze_base_path = os.getenv("BRONZE_BASE_PATH")
    metadata_base_path = os.getenv("METADATA_BASE_PATH")
    target_table = os.getenv("SILVER_OPENSKY_TABLE")
    checkpoint_path = os.getenv("SILVER_OPENSKY_CHECKPOINT_PATH")

    if not bronze_base_path:
        raise ValueError("BRONZE_BASE_PATH must be configured.")

    if not metadata_base_path:
        raise ValueError("METADATA_BASE_PATH must be configured.")

    if not target_table:
        raise ValueError("SILVER_OPENSKY_TABLE must be configured.")

    if not checkpoint_path:
        raise ValueError("SILVER_OPENSKY_CHECKPOINT_PATH must be configured.")

    identifier = r"[A-Za-z_][A-Za-z0-9_]*"
    if not re.fullmatch(
        rf"{identifier}\.{identifier}\.{identifier}",
        target_table,
    ):
        raise ValueError(
            "SILVER_OPENSKY_TABLE must be catalog.schema.table."
        )

    bronze_path = (
        f"{bronze_base_path.rstrip('/')}/"
        "source=opensky/entity=states/"
    )
    metadata_path = (
        f"{metadata_base_path.rstrip('/')}/"
        "pipeline_runs/source=opensky/entity=states/"
    )
    checkpoint_root = checkpoint_path.rstrip("/")
    stream_checkpoint_path = f"{checkpoint_root}/stream"
    schema_location = f"{checkpoint_root}/schema"

    return (
        bronze_path,
        metadata_path,
        target_table,
        stream_checkpoint_path,
        schema_location,
    )


def assert_no_rows(df, check_name):
    if df.limit(1).count() > 0:
        raise ValueError(f"Data quality check failed: {check_name}")


def with_relative_bronze_path(df, file_path_col):
    return df.withColumn(
        "source_relative_path",
        F.regexp_extract(F.col(file_path_col), BRONZE_RELATIVE_PATH_RE, 1),
    )


def read_bronze_stream(spark, bronze_path, schema_location):
    # Keep file_path on the stream; _metadata may be gone inside foreachBatch.
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("multiLine", "true")
        .load(bronze_path)
        .select("*", F.col("_metadata.file_path").alias("source_file_path"))
    )


def read_ingestion_metadata(spark, metadata_path):
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(metadata_path)
        .select(
            F.col("run_id").alias("ingestion_run_id"),
            F.col("api_time_unix").cast("bigint"),
            F.col("destination_path"),
            F.col("load_datetime_utc")
            .cast("timestamp")
            .alias("bronze_load_datetime_utc"),
        )
        # Ingestion writes metadata even when bronze upload is skipped.
        .filter(
            F.col("destination_path").isNotNull()
            & (F.trim("destination_path") != "")
        )
    )


def validate_ingestion_metadata(df_metadata):
    if df_metadata.limit(1).count() == 0:
        raise ValueError(
            "Data quality check failed: empty ingestion metadata"
        )

    missing_fields = df_metadata.filter(
        F.col("ingestion_run_id").isNull()
        | (F.trim("ingestion_run_id") == "")
        | F.col("bronze_load_datetime_utc").isNull()
        | F.col("api_time_unix").isNull()
    )
    assert_no_rows(
        missing_fields,
        "Missing required ingestion metadata fields",
    )

    duplicate_paths = (
        df_metadata.groupBy("destination_path")
        .count()
        .filter(F.col("count") > 1)
    )
    assert_no_rows(
        duplicate_paths,
        "Duplicate destination_path in ingestion metadata",
    )


def validate_bronze_metadata_coverage(df_bronze, df_metadata):
    invalid_paths = df_bronze.filter(
        F.col("source_relative_path").isNull()
        | (F.col("source_relative_path") == "")
    )
    assert_no_rows(invalid_paths, "Cannot extract relative Bronze file path")

    missing_metadata = (
        df_bronze.select("source_relative_path")
        .distinct()
        .join(
            df_metadata.select("destination_path"),
            F.col("source_relative_path") == F.col("destination_path"),
            how="left_anti",
        )
    )
    assert_no_rows(
        missing_metadata,
        "Bronze files without ingestion metadata",
    )


def join_bronze_and_metadata(df_bronze, df_metadata):
    df_joined = (
        df_bronze.alias("b")
        .join(
            df_metadata.alias("m"),
            F.col("b.source_relative_path") == F.col("m.destination_path"),
            how="inner",
        )
        .select(
            "b.*",
            "m.ingestion_run_id",
            "m.bronze_load_datetime_utc",
            "m.api_time_unix",
        )
    )

    df_exploded = df_joined.select(
        "time",
        F.explode("states").alias("state"),
        "source_file_path",
        "ingestion_run_id",
        "bronze_load_datetime_utc",
        "api_time_unix",
    )

    return df_exploded.select(
        "time",
        "state.*",
        "source_file_path",
        "ingestion_run_id",
        "bronze_load_datetime_utc",
        "api_time_unix",
    )


def build_silver_staging(
    df_bronze,
    df_metadata,
    transformation_run_id,
    silver_load_datetime_utc,
):
    df_flattened = join_bronze_and_metadata(df_bronze, df_metadata)

    return (
        df_flattened.filter(F.trim(F.col("icao24")) != "")
        .select(
            F.timestamp_seconds("time").alias("snapshot_time_utc"),
            F.col("time").cast("bigint").alias("snapshot_time_unix"),
            F.lower(F.trim("icao24")).alias("icao24"),
            F.when(F.trim("callsign") == "", None)
            .otherwise(F.trim("callsign"))
            .cast("string")
            .alias("callsign"),
            F.when(F.trim("origin_country") == "", None)
            .otherwise(F.trim("origin_country"))
            .cast("string")
            .alias("origin_country"),
            F.timestamp_seconds("time_position").alias("time_position_utc"),
            F.timestamp_seconds("last_contact").alias("last_contact_utc"),
            F.col("longitude").cast("double"),
            F.col("latitude").cast("double"),
            F.col("baro_altitude").cast("double").alias("baro_altitude_m"),
            F.col("on_ground").cast("boolean"),
            F.col("velocity").cast("double").alias("velocity_ms"),
            F.col("true_track").cast("double").alias("true_track_deg"),
            F.col("geo_altitude").cast("double").alias("geo_altitude_m"),
            F.col("squawk").cast("string"),
            F.col("spi").cast("boolean"),
            F.col("position_source").cast("int"),
            F.col("category").cast("int"),
            F.lit(transformation_run_id).alias("transformation_run_id"),
            F.col("source_file_path"),
            F.col("ingestion_run_id"),
            F.col("bronze_load_datetime_utc"),
            F.lit(silver_load_datetime_utc)
            .cast("timestamp")
            .alias("silver_load_datetime_utc"),
            F.col("api_time_unix"),
        )
    )


def validate_silver_staging(df_silver):
    if df_silver.limit(1).count() == 0:
        raise ValueError(
            "Data quality check failed: empty Silver staging"
        )

    missing_fields = df_silver.filter(
        F.col("snapshot_time_unix").isNull()
        | F.col("icao24").isNull()
        | (F.trim("icao24") == "")
        | F.col("source_file_path").isNull()
        | (F.trim("source_file_path") == "")
        | F.col("ingestion_run_id").isNull()
        | (F.trim("ingestion_run_id") == "")
        | F.col("bronze_load_datetime_utc").isNull()
        | F.col("transformation_run_id").isNull()
        | (F.trim("transformation_run_id") == "")
        | F.col("silver_load_datetime_utc").isNull()
        | F.col("api_time_unix").isNull()
    )
    assert_no_rows(missing_fields, "Missing required Silver fields")

    invalid_snapshot_time = df_silver.filter(
        ~F.col("snapshot_time_unix").eqNullSafe(F.col("api_time_unix"))
    )
    assert_no_rows(
        invalid_snapshot_time,
        "Snapshot time differs from ingestion metadata",
    )

    duplicate_keys = (
        df_silver.groupBy("snapshot_time_unix", "icao24")
        .count()
        .filter(F.col("count") > 1)
    )
    assert_no_rows(duplicate_keys, "Duplicate OpenSky business key")

    run_count = (
        df_silver.select("transformation_run_id").distinct().limit(2).count()
    )
    if run_count != 1:
        raise ValueError(
            "Data quality check failed: inconsistent transformation_run_id"
        )


def create_opensky_table_schema(spark, target_table):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            snapshot_time_utc TIMESTAMP,
            snapshot_time_unix BIGINT,
            icao24 STRING,
            callsign STRING,
            origin_country STRING,
            time_position_utc TIMESTAMP,
            last_contact_utc TIMESTAMP,
            longitude DOUBLE,
            latitude DOUBLE,
            baro_altitude_m DOUBLE,
            on_ground BOOLEAN,
            velocity_ms DOUBLE,
            true_track_deg DOUBLE,
            geo_altitude_m DOUBLE,
            squawk STRING,
            spi BOOLEAN,
            position_source INT,
            category INT,
            transformation_run_id STRING,
            source_file_path STRING,
            ingestion_run_id STRING,
            bronze_load_datetime_utc TIMESTAMP,
            silver_load_datetime_utc TIMESTAMP
        )
        USING DELTA
        """
    )


def merge_into_silver(spark, df_silver, target_table):
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(
            df_silver.alias("s"),
            """
            t.snapshot_time_unix = s.snapshot_time_unix
            AND t.icao24 = s.icao24
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_bronze_batch(
    spark,
    df_bronze_batch,
    metadata_path,
    target_table,
    transformation_run_id,
    silver_load_datetime_utc,
):
    if df_bronze_batch.limit(1).count() == 0:
        print(
            "OpenSky Silver skipped: no new Bronze files. "
            f"transformation_run_id={transformation_run_id}"
        )
        return

    df_bronze = with_relative_bronze_path(df_bronze_batch, "source_file_path")
    df_metadata = read_ingestion_metadata(spark, metadata_path)
    df_metadata_batch = df_metadata.join(
        df_bronze.select("source_relative_path").distinct(),
        F.col("destination_path") == F.col("source_relative_path"),
        how="inner",
    )

    validate_ingestion_metadata(df_metadata_batch)
    validate_bronze_metadata_coverage(df_bronze, df_metadata)

    df_silver = build_silver_staging(
        df_bronze,
        df_metadata_batch,
        transformation_run_id,
        silver_load_datetime_utc,
    )
    df_silver = df_silver.localCheckpoint(eager=True)
    validate_silver_staging(df_silver)

    merge_into_silver(spark, df_silver.drop("api_time_unix"), target_table)
    print(
        "OpenSky Silver MERGE completed. "
        f"transformation_run_id={transformation_run_id}"
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    (
        bronze_path,
        metadata_path,
        target_table,
        stream_checkpoint_path,
        schema_location,
    ) = get_config()

    transformation_run_id = str(uuid.uuid4())
    silver_load_datetime_utc = datetime.now(timezone.utc)

    create_opensky_table_schema(spark, target_table)

    df_bronze_stream = read_bronze_stream(
        spark,
        bronze_path,
        schema_location,
    )

    def foreach_batch(df_bronze_batch, _batch_id):
        process_bronze_batch(
            spark,
            df_bronze_batch,
            metadata_path,
            target_table,
            transformation_run_id,
            silver_load_datetime_utc,
        )

    (
        df_bronze_stream.writeStream.option(
            "checkpointLocation", stream_checkpoint_path
        )
        .trigger(availableNow=True)
        .foreachBatch(foreach_batch)
        .outputMode("append")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
