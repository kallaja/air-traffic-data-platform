-- Gold mart: staging → hard-fail assert → promote published table.
-- One Job SQL task: if any assert_true fails, later statements do not run and
-- gold.opensky_activity_by_country_day stays at the previous good version.
-- Grain: one row per (activity_date, country_cca2).
-- Source: silver.opensky_states + silver.dim_country (current rows).
-- Unmapped / missing country_cca2 is reported as 'Unknown'.
-- Optional diagnostics (published table): validate_opensky_activity_by_country_day.sql

CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- 1) BUILD STAGING
-- ============================================================

CREATE OR REPLACE TABLE gold.opensky_activity_by_country_day_staging AS
SELECT
    CAST(s.snapshot_time_utc AS DATE) AS activity_date,
    COALESCE(s.country_cca2, 'Unknown') AS country_cca2,
    COALESCE(c.name_common, 'Unknown') AS country_name,
    COALESCE(c.region, 'Unknown') AS region,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT s.icao24) AS distinct_aircraft,
    AVG(s.baro_altitude_m) AS avg_baro_altitude_m,
    AVG(s.velocity_ms) AS avg_velocity_ms,
    COUNT(CASE WHEN s.on_ground = true THEN 1 END) AS on_ground_count,
    COUNT(CASE WHEN s.on_ground = false THEN 1 END) AS in_air_count,
    COUNT(CASE WHEN s.on_ground IS NULL THEN 1 END) AS unknown_on_ground_count
FROM silver.opensky_states AS s
LEFT JOIN silver.dim_country AS c
    ON c.country_cca2 = s.country_cca2
   AND c.is_current = true
GROUP BY
    CAST(s.snapshot_time_utc AS DATE),
    COALESCE(s.country_cca2, 'Unknown'),
    COALESCE(c.name_common, 'Unknown'),
    COALESCE(c.region, 'Unknown');

-- ============================================================
-- 2) ASSERT ON STAGING (hard-fail)
-- ============================================================

SELECT assert_true(
    COUNT(*) = 0,
    'Gold staging grain violated: duplicate (activity_date, country_cca2)'
)
FROM (
    SELECT
        activity_date,
        country_cca2
    FROM gold.opensky_activity_by_country_day_staging
    GROUP BY
        activity_date,
        country_cca2
    HAVING COUNT(*) > 1
) AS bad;

WITH silver_aggregated AS (
    SELECT
        CAST(snapshot_time_utc AS DATE) AS activity_date,
        COALESCE(country_cca2, 'Unknown') AS country_cca2,
        COUNT(*) AS expected_observation_count
    FROM silver.opensky_states
    GROUP BY
        CAST(snapshot_time_utc AS DATE),
        COALESCE(country_cca2, 'Unknown')
),
mismatches AS (
    SELECT
        COALESCE(g.activity_date, s.activity_date) AS activity_date,
        COALESCE(g.country_cca2, s.country_cca2) AS country_cca2,
        g.observation_count AS gold_observation_count,
        s.expected_observation_count AS silver_observation_count
    FROM gold.opensky_activity_by_country_day_staging AS g
    FULL OUTER JOIN silver_aggregated AS s
        ON g.activity_date = s.activity_date
       AND g.country_cca2 = s.country_cca2
    WHERE g.activity_date IS NULL
       OR s.activity_date IS NULL
       OR g.observation_count <> s.expected_observation_count
)
SELECT assert_true(
    COUNT(*) = 0,
    'Gold staging observation_count does not reconcile with silver.opensky_states'
)
FROM mismatches;

WITH silver_aggregated AS (
    SELECT
        CAST(snapshot_time_utc AS DATE) AS activity_date,
        COALESCE(country_cca2, 'Unknown') AS country_cca2,
        COUNT(DISTINCT icao24) AS expected_distinct_aircraft
    FROM silver.opensky_states
    GROUP BY
        CAST(snapshot_time_utc AS DATE),
        COALESCE(country_cca2, 'Unknown')
),
mismatches AS (
    SELECT
        COALESCE(g.activity_date, s.activity_date) AS activity_date,
        COALESCE(g.country_cca2, s.country_cca2) AS country_cca2,
        g.distinct_aircraft AS gold_distinct_aircraft,
        s.expected_distinct_aircraft AS silver_distinct_aircraft
    FROM gold.opensky_activity_by_country_day_staging AS g
    FULL OUTER JOIN silver_aggregated AS s
        ON g.activity_date = s.activity_date
       AND g.country_cca2 = s.country_cca2
    WHERE g.activity_date IS NULL
       OR s.activity_date IS NULL
       OR g.distinct_aircraft <> s.expected_distinct_aircraft
)
SELECT assert_true(
    COUNT(*) = 0,
    'Gold staging distinct_aircraft does not reconcile with silver.opensky_states'
)
FROM mismatches;

-- ============================================================
-- 3) PROMOTE
-- ============================================================

CREATE OR REPLACE TABLE gold.opensky_activity_by_country_day AS
SELECT *
FROM gold.opensky_activity_by_country_day_staging;
