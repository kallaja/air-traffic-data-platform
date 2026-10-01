-- Load silver.dim_country with SCD Type 2 (history preserved)
-- Source: REST Countries API v5
--   (names.*, codes.*, capitals[], coordinates, population, area)
--
-- Pattern:
-- 1) Stage latest bronze snapshot
-- 2) MERGE: close current rows whose attributes changed
-- 3) INSERT: new countries + new versions of changed countries
--
-- As-of join: activity_date >= valid_from AND activity_date < valid_to

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.dim_country (
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
);

CREATE OR REPLACE TEMP VIEW stg_dim_country AS
WITH bronze_countries AS (
  SELECT
    to_timestamp(bronze.fetched_at_utc) AS source_fetched_at_utc,
    NULLIF(UPPER(TRIM(c.codes.alpha_2)), '') AS country_cca2,
    UPPER(TRIM(c.codes.alpha_3)) AS country_cca3,
    NULLIF(TRIM(c.names.common), '') AS name_common,
    NULLIF(TRIM(c.names.official), '') AS name_official,
    NULLIF(TRIM(c.region), '') AS region,
    NULLIF(TRIM(c.subregion), '') AS subregion,
    NULLIF(
      TRIM(
        coalesce(
          element_at(filter(c.capitals, x -> x.attributes.primary = true), 1).name,
          element_at(c.capitals, 1).name
        )
      ),
      ''
    ) AS capital,
    CAST(
      coalesce(
        element_at(filter(c.capitals, x -> x.attributes.primary = true), 1).coordinates.lat,
        element_at(c.capitals, 1).coordinates.lat
      ) AS DOUBLE
    ) AS capital_latitude,
    CAST(
      coalesce(
        element_at(filter(c.capitals, x -> x.attributes.primary = true), 1).coordinates.lng,
        element_at(c.capitals, 1).coordinates.lng
      ) AS DOUBLE
    ) AS capital_longitude,
    CAST(c.coordinates.lat AS DOUBLE) AS country_latitude,
    CAST(c.coordinates.lng AS DOUBLE) AS country_longitude,
    CAST(c.population AS BIGINT) AS population,
    CAST(c.area.kilometers AS DOUBLE) AS area_km2
  FROM read_files(
    'abfss://bronze@papstorage001.dfs.core.windows.net/source=restcountries/entity=countries/',
    format => 'json',
    schemaEvolutionMode => 'none',
    multiLine => true,
    recursiveFileLookup => true
  ) AS bronze
  LATERAL VIEW explode(bronze.countries) e AS c
  WHERE c.codes.alpha_2 IS NOT NULL
  AND TRIM(c.codes.alpha_2) <> ''
  AND bronze.api_version = 'v5'
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY country_cca2
      ORDER BY source_fetched_at_utc DESC
    ) AS rn
  FROM bronze_countries
)
SELECT
  country_cca2,
  country_cca3,
  name_common,
  name_official,
  region,
  subregion,
  capital,
  capital_latitude,
  capital_longitude,
  country_latitude,
  country_longitude,
  population,
  area_km2,
  md5(
    concat_ws(
      '||',
      coalesce(country_cca3, ''),
      coalesce(name_common, ''),
      coalesce(name_official, ''),
      coalesce(region, ''),
      coalesce(subregion, ''),
      coalesce(capital, ''),
      coalesce(CAST(capital_latitude AS STRING), ''),
      coalesce(CAST(capital_longitude AS STRING), ''),
      coalesce(CAST(country_latitude AS STRING), ''),
      coalesce(CAST(country_longitude AS STRING), ''),
      coalesce(CAST(population AS STRING), ''),
      coalesce(CAST(area_km2 AS STRING), '')
    )
  ) AS row_hash,
  CAST(source_fetched_at_utc AS DATE) AS load_date,
  source_fetched_at_utc
FROM ranked
WHERE rn = 1;

MERGE INTO silver.dim_country AS t
USING stg_dim_country AS s
ON t.country_cca2 = s.country_cca2
AND t.is_current = true
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
  t.valid_to = s.load_date,
  t.is_current = false;

INSERT INTO silver.dim_country (
  country_cca2,
  country_cca3,
  name_common,
  name_official,
  region,
  subregion,
  capital,
  capital_latitude,
  capital_longitude,
  country_latitude,
  country_longitude,
  population,
  area_km2,
  row_hash,
  valid_from,
  valid_to,
  is_current,
  source_fetched_at_utc
)
SELECT
  s.country_cca2,
  s.country_cca3,
  s.name_common,
  s.name_official,
  s.region,
  s.subregion,
  s.capital,
  s.capital_latitude,
  s.capital_longitude,
  s.country_latitude,
  s.country_longitude,
  s.population,
  s.area_km2,
  s.row_hash,
  s.load_date AS valid_from,
  DATE('9999-12-31') AS valid_to,
  true AS is_current,
  s.source_fetched_at_utc
FROM stg_dim_country AS s
LEFT JOIN silver.dim_country AS t
  ON s.country_cca2 = t.country_cca2
 AND t.is_current = true
WHERE t.country_cca2 IS NULL;
