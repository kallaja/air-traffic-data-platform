"""Map OpenSky origin_country to ISO country_cca2.

Strategy:
1) manual overrides from ORIGIN_COUNTRY_OVERRIDES_PATH JSON (exact origin_country)
2) exact match to dim_country name_common / name_official (is_current)
3) match after stripping common official prefixes
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Applied to lowercased origin names (same idea as the audit SQL).
OFFICIAL_PREFIX_RE = (
    r"^(islamic republic of|syrian arab|republic of the|republic of|"
    r"kingdom of the|kingdom of|state of|federal republic of)\s+"
)


def load_origin_country_overrides(path: str) -> dict[str, str]:
    """Load origin_country -> country_cca2 overrides from JSON file."""
    if not path:
        raise ValueError("ORIGIN_COUNTRY_OVERRIDES_PATH must be configured.")

    file_path = Path(path)
    if not file_path.is_file():
        raise ValueError(
            f"Overrides file not found: {file_path}. "
            "Set ORIGIN_COUNTRY_OVERRIDES_PATH to config/origin_country_overrides.json "
            "(repo) or the matching path on Volume/ADLS."
        )

    with file_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    file_overrides = payload.get("overrides") or {}
    if not isinstance(file_overrides, dict):
        raise ValueError(
            f"overrides in {file_path} must be a JSON object of string -> string"
        )

    overrides: dict[str, str] = {}
    for origin_country, country_cca2 in file_overrides.items():
        if not isinstance(origin_country, str) or not isinstance(country_cca2, str):
            raise ValueError(
                f"Invalid override entry in {file_path}: "
                f"{origin_country!r} -> {country_cca2!r}"
            )
        overrides[origin_country] = country_cca2.upper()

    return overrides


def normalize_origin_name(col):
    """lower(trim(...)) and normalize fancy apostrophes to ASCII '."""
    return F.lower(F.trim(F.regexp_replace(col, "[’`]", "'")))


def build_dim_name_lookup(spark: SparkSession, dim_country_table: str) -> DataFrame:
    """name_key (lower common/official) -> country_cca2 for current dim rows."""
    dim_current = spark.table(dim_country_table).filter(F.col("is_current"))

    by_common = dim_current.select(
        normalize_origin_name(F.col("name_common")).alias("name_key"),
        F.upper(F.trim("country_cca2")).alias("country_cca2"),
    ).filter(F.col("name_key").isNotNull() & (F.col("name_key") != ""))

    by_official = dim_current.select(
        normalize_origin_name(F.col("name_official")).alias("name_key"),
        F.upper(F.trim("country_cca2")).alias("country_cca2"),
    ).filter(F.col("name_key").isNotNull() & (F.col("name_key") != ""))

    return (
        by_common.unionByName(by_official)
        .dropDuplicates(["name_key"])
        .filter(F.col("country_cca2").isNotNull() & (F.col("country_cca2") != ""))
    )


def overrides_dataframe(spark: SparkSession, overrides: dict[str, str]) -> DataFrame:
    rows = [(origin, cca2.upper()) for origin, cca2 in overrides.items()]
    if not rows:
        return spark.createDataFrame(
            [],
            schema="origin_country STRING, country_cca2_override STRING",
        )
    return spark.createDataFrame(
        rows,
        schema="origin_country STRING, country_cca2_override STRING",
    )


def with_country_cca2(
    df: DataFrame,
    spark: SparkSession,
    dim_country_table: str,
    overrides: dict[str, str],
) -> DataFrame:
    """Add country_cca2 from overrides + dim_country name matching."""
    if "origin_country" not in df.columns:
        raise ValueError("DataFrame must contain origin_country to map country_cca2")

    df_overrides = overrides_dataframe(spark, overrides)
    df_dim_names = build_dim_name_lookup(spark, dim_country_table)

    df_prep = (
        df.withColumn("origin_n", normalize_origin_name(F.col("origin_country")))
        .withColumn(
            "origin_stripped",
            F.trim(F.regexp_replace(F.col("origin_n"), OFFICIAL_PREFIX_RE, "")),
        )
        .join(df_overrides, on="origin_country", how="left")
    )

    df_exact = df_dim_names.select(
        F.col("name_key").alias("exact_name_key"),
        F.col("country_cca2").alias("cca2_exact"),
    )
    df_stripped = df_dim_names.select(
        F.col("name_key").alias("stripped_name_key"),
        F.col("country_cca2").alias("cca2_stripped"),
    )

    return (
        df_prep.join(
            df_exact,
            df_prep["origin_n"] == df_exact["exact_name_key"],
            how="left",
        )
        .join(
            df_stripped,
            df_prep["origin_stripped"] == df_stripped["stripped_name_key"],
            how="left",
        )
        .withColumn(
            "country_cca2",
            F.coalesce(
                F.col("country_cca2_override"),
                F.col("cca2_exact"),
                F.col("cca2_stripped"),
            ),
        )
        .drop(
            "origin_n",
            "origin_stripped",
            "country_cca2_override",
            "exact_name_key",
            "stripped_name_key",
            "cca2_exact",
            "cca2_stripped",
        )
    )
