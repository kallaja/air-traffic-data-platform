import uuid
from pyspark.sql import functions as F

transformation_run_id = str(uuid.uuid4())
spark.conf.set("spark.sql.session.timeZone", "UTC")

bronze_base_path = "abfss://bronze@papstorage001.dfs.core.windows.net/"

bronze_path = (
    "abfss://bronze@papstorage001.dfs.core.windows.net/"
    "source=opensky/entity=states/")

metadata_path = (
    "abfss://metadata@papstorage001.dfs.core.windows.net/"
    "pipeline_runs/source=opensky/entity=states/")

def read_bronze(bronze_path):
    df_bronze = (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(bronze_path)
        .select("*", F.col("_metadata.file_path").alias("source_file_path"))
    )
    return df_bronze

def read_ingestion_metadata(metadata_path, bronze_base_path):
    df_metadata = (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(metadata_path)
        .select(
            F.col("run_id").alias("ingestion_run_id"),
            F.col("api_time_unix"),
            F.col("destination_path"),
            F.col("load_datetime_utc")
                .cast("timestamp")
                .alias("bronze_load_datetime_utc"))
    )

    df_metadata = df_metadata.withColumn(
                                    "metadata_source_file_path",
                                    F.concat(F.lit(bronze_base_path), F.col("destination_path")))
    return df_metadata

def join_bronze_and_metadata(df_bronze, df_metadata):
    df_joined = ( 
        df_bronze
        .alias("b")
        .join(
            df_metadata.alias("m"), 
            F.col("b.source_file_path") == F.col("m.metadata_source_file_path"), 
            how="left")
            .select(
                "b.*", 
                "m.ingestion_run_id", 
                "m.bronze_load_datetime_utc", 
                "m.api_time_unix")
    )
    df_exploded = df_joined.select(
        "time", 
        F.explode("states").alias("state"), 
        "source_file_path", 
        "ingestion_run_id", 
        "bronze_load_datetime_utc", 
        "api_time_unix")
    df_flattened = (
        df_exploded
        .select(
            "time", 
            "state.*", 
            "source_file_path", 
            "ingestion_run_id", 
            "bronze_load_datetime_utc", 
            "api_time_unix")
    )
    return df_flattened


def build_silver_staging(bronze_path, metadata_path, bronze_base_path):
    df_bronze = read_bronze(bronze_path)
    df_metadata = read_ingestion_metadata(metadata_path, bronze_base_path)
    df_flattened =join_bronze_and_metadata(df_bronze, df_metadata)

    df_silver = df_flattened.filter(F.trim(df_flattened.icao24) != "" # trim(NULL) != "" returns NULL, not true, so .isNotNull() is not needed
    ).select(
        F.timestamp_seconds(df_flattened.time).alias("snapshot_time_utc"),
        df_flattened.time.cast("bigint").alias("snapshot_time_unix"),
        F.lower(F.trim(df_flattened.icao24)).cast("string").alias("icao24"),

        F.when(
            F.trim(df_flattened.callsign).cast("string") == "", 
            None
            ).otherwise(
                F.trim(df_flattened.callsign)
            ).cast("string").alias("callsign"),

        F.when(
            F.trim(df_flattened.origin_country).cast("string") == "", 
            None
            ).otherwise(
                F.trim(df_flattened.origin_country)
            ).cast("string").alias("origin_country"),

        F.timestamp_seconds(df_flattened.time_position).alias("time_position_utc"),
        F.timestamp_seconds(df_flattened.last_contact).alias("last_contact_utc"),
        df_flattened.longitude.cast("double"),
        df_flattened.latitude.cast("double"),
        df_flattened.baro_altitude.cast("double").alias("baro_altitude_m"),
        df_flattened.on_ground.cast("boolean"),
        df_flattened.velocity.cast("double").alias("velocity_ms"),
        df_flattened.true_track.cast("double").alias("true_track_deg"),
        df_flattened.vertical_rate.cast("double").alias("vertical_rate_ms"),
        df_flattened.geo_altitude.cast("double").alias("geo_altitude_m"),
        df_flattened.squawk.cast("string"),
        df_flattened.spi.cast("boolean"),
        df_flattened.position_source.cast("int"),
        df_flattened.category.cast("int"),
        F.lit(transformation_run_id).alias("transformation_run_id"),
        df_flattened.source_file_path.alias("source_file_path"),
        df_flattened.ingestion_run_id.alias("ingestion_run_id"),
        df_flattened.bronze_load_datetime_utc.cast("timestamp").alias("bronze_load_datetime_utc"),
        F.current_timestamp().alias("silver_load_datetime_utc"),
    )
    return df_silver


df_silver = build_silver_staging(
    bronze_path,
    metadata_path,
    bronze_base_path
)
