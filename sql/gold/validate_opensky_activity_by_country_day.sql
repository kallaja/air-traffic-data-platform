-- Validate published gold.opensky_activity_by_country_day (manual / optional).
-- Job hard-fail (staging → assert → promote): create_opensky_activity_by_country_day.sql
-- Grain: one row per (activity_date, country_cca2).
-- Source: silver.opensky_states + silver.dim_country (current).

-- ============================================================
-- 1) ROW COUNT
-- Expect: > 0 after successful Gold refresh.

SELECT
    COUNT(*) AS row_count
FROM gold.opensky_activity_by_country_day;

-- ============================================================
-- 2) GRAIN / UNIQUENESS
-- Expect: 0 rows.

SELECT
    activity_date,
    country_cca2,
    COUNT(*) AS row_count
FROM gold.opensky_activity_by_country_day
GROUP BY
    activity_date,
    country_cca2
HAVING COUNT(*) > 1;

-- ============================================================
-- 3) REQUIRED / NULL FIELDS
-- activity_date / country_cca2 / country_name should be populated
-- (missing silver cca2 is coalesced to 'Unknown' in Gold).
-- Expect: all 0.

SELECT
    SUM(CASE WHEN activity_date IS NULL THEN 1 ELSE 0 END) AS null_activity_date,
    SUM(CASE WHEN country_cca2 IS NULL THEN 1 ELSE 0 END) AS null_country_cca2,
    SUM(CASE WHEN country_name IS NULL THEN 1 ELSE 0 END) AS null_country_name
FROM gold.opensky_activity_by_country_day;

-- ============================================================
-- 4) METRIC SANITY
-- Expect: 0 rows.

SELECT *
FROM gold.opensky_activity_by_country_day
WHERE observation_count <= 0
   OR distinct_aircraft <= 0
   OR distinct_aircraft > observation_count
   OR on_ground_count < 0
   OR in_air_count < 0
   OR unknown_on_ground_count < 0;

-- ============================================================
-- 5) ON-GROUND STATUS CONSISTENCY
-- observation_count = on_ground + in_air + unknown
-- Expect: 0 rows.

SELECT
    activity_date,
    country_cca2,
    observation_count,
    on_ground_count,
    in_air_count,
    unknown_on_ground_count
FROM gold.opensky_activity_by_country_day
WHERE observation_count <>
      on_ground_count
      + in_air_count
      + unknown_on_ground_count;

-- ============================================================
-- 6) RECONCILE OBSERVATION COUNT WITH SILVER
-- Expect: 0 rows.

WITH silver_aggregated AS (
    SELECT
        CAST(snapshot_time_utc AS DATE) AS activity_date,
        COALESCE(country_cca2, 'Unknown') AS country_cca2,
        COUNT(*) AS expected_observation_count
    FROM silver.opensky_states
    GROUP BY
        CAST(snapshot_time_utc AS DATE),
        COALESCE(country_cca2, 'Unknown')
)

SELECT
    g.activity_date,
    g.country_cca2,
    g.observation_count AS gold_observation_count,
    s.expected_observation_count AS silver_observation_count
FROM gold.opensky_activity_by_country_day AS g
LEFT JOIN silver_aggregated AS s
    ON g.activity_date = s.activity_date
   AND g.country_cca2 = s.country_cca2
WHERE s.activity_date IS NULL
   OR g.observation_count <> s.expected_observation_count;

-- ============================================================
-- 7) RECONCILE DISTINCT AIRCRAFT WITH SILVER
-- Expect: 0 rows.

WITH silver_aggregated AS (
    SELECT
        CAST(snapshot_time_utc AS DATE) AS activity_date,
        COALESCE(country_cca2, 'Unknown') AS country_cca2,
        COUNT(DISTINCT icao24) AS expected_distinct_aircraft
    FROM silver.opensky_states
    GROUP BY
        CAST(snapshot_time_utc AS DATE),
        COALESCE(country_cca2, 'Unknown')
)

SELECT
    g.activity_date,
    g.country_cca2,
    g.distinct_aircraft AS gold_distinct_aircraft,
    s.expected_distinct_aircraft AS silver_distinct_aircraft
FROM gold.opensky_activity_by_country_day AS g
LEFT JOIN silver_aggregated AS s
    ON g.activity_date = s.activity_date
   AND g.country_cca2 = s.country_cca2
WHERE s.activity_date IS NULL
   OR g.distinct_aircraft <> s.expected_distinct_aircraft;

-- ============================================================
-- 8) UNKNOWN COUNTRY SHARE (diagnostic)
SELECT
    activity_date,
    observation_count,
    distinct_aircraft
FROM gold.opensky_activity_by_country_day
WHERE country_cca2 = 'Unknown'
ORDER BY activity_date DESC;

-- ============================================================
-- 9) UNKNOWN ON-GROUND STATUS (diagnostic)

SELECT
    activity_date,
    country_cca2,
    country_name,
    observation_count,
    unknown_on_ground_count,
    ROUND(
        100.0 * unknown_on_ground_count / observation_count,
        2
    ) AS unknown_on_ground_pct
FROM gold.opensky_activity_by_country_day
WHERE unknown_on_ground_count > 0
ORDER BY
    unknown_on_ground_pct DESC,
    activity_date DESC;

-- ============================================================
-- 10) SAMPLE FOR MANUAL INSPECTION

SELECT *
FROM gold.opensky_activity_by_country_day
ORDER BY
    activity_date DESC,
    distinct_aircraft DESC
LIMIT 50;
