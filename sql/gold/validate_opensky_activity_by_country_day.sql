-- Validate gold.opensky_activity_by_country_day
-- Grain: One row per activity_date + origin_country.
-- Source: silver.opensky_states

-- ============================================================
-- 1) ROW COUNT
-- Expect: > 0 after successful Gold refresh.

SELECT
    COUNT(*) AS row_count
FROM gold.opensky_activity_by_country_day;

-- ============================================================
-- 2) GRAIN / UNIQUENESS
-- There should be at most one row for each: (activity_date, origin_country)
-- Expect: 0 rows.

SELECT
    activity_date,
    origin_country,
    COUNT(*) AS row_count
FROM gold.opensky_activity_by_country_day
GROUP BY
    activity_date,
    origin_country
HAVING COUNT(*) > 1;

-- ============================================================
-- 3) REQUIRED / NULL FIELDS
-- activity_date should always be populated.
-- origin_country may be NULL because OpenSky can providea missing origin_country and Silver intentionally preserves missing source values as NULL.

SELECT
    SUM(
        CASE WHEN activity_date IS NULL THEN 1 ELSE 0 END
    ) AS null_activity_date,

    SUM(
        CASE WHEN origin_country IS NULL THEN 1 ELSE 0 END
    ) AS null_origin_country

FROM gold.opensky_activity_by_country_day;

-- ============================================================
-- 4) METRIC SANITY
-- Counts cannot be negative.
-- Every Gold row should represent at least one observation.
-- distinct_aircraft cannot exceed observation_count.
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
-- Every observation must belong to exactly one status:
--   on ground
--   in air
--   unknown
--
-- Therefore:
-- observation_count =
--     on_ground_count
--   + in_air_count
--   + unknown_on_ground_count
--
-- Expect: 0 rows.

SELECT
    activity_date,
    origin_country,
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
-- Independently aggregate Silver using the same Gold grain and compare the result with Gold.
--
-- <=> is NULL-safe equality in Spark / Databricks SQL.
-- It allows NULL origin_country in Silver to match NULL origin_country in Gold.
-- Expect: 0 rows.

WITH silver_aggregated AS (
    SELECT
        CAST(snapshot_time_utc AS DATE) AS activity_date,
        origin_country,
        COUNT(*) AS expected_observation_count
    FROM silver.opensky_states
    GROUP BY
        CAST(snapshot_time_utc AS DATE),
        origin_country
)

SELECT
    g.activity_date,
    g.origin_country,
    g.observation_count AS gold_observation_count,
    s.expected_observation_count AS silver_observation_count
FROM gold.opensky_activity_by_country_day AS g
LEFT JOIN silver_aggregated AS s
    ON g.activity_date = s.activity_date
   AND g.origin_country <=> s.origin_country
WHERE s.activity_date IS NULL
   OR g.observation_count <> s.expected_observation_count;

-- ============================================================
-- 7) RECONCILE DISTINCT AIRCRAFT WITH SILVER
-- Recalculate distinct aircraft directly from Silver and compare it with the Gold metric.
-- Expect: 0 rows.

WITH silver_aggregated AS (
    SELECT
        CAST(snapshot_time_utc AS DATE) AS activity_date,
        origin_country,
        COUNT(DISTINCT icao24) AS expected_distinct_aircraft
    FROM silver.opensky_states
    GROUP BY
        CAST(snapshot_time_utc AS DATE),
        origin_country
)

SELECT
    g.activity_date,
    g.origin_country,
    g.distinct_aircraft AS gold_distinct_aircraft,
    s.expected_distinct_aircraft AS silver_distinct_aircraft
FROM gold.opensky_activity_by_country_day AS g
LEFT JOIN silver_aggregated AS s
    ON g.activity_date = s.activity_date
   AND g.origin_country <=> s.origin_country
WHERE s.activity_date IS NULL
   OR g.distinct_aircraft <> s.expected_distinct_aircraft;

-- ============================================================
-- 8) UNKNOWN ON-GROUND STATUS
-- Diagnostic query.
-- These rows are not necessarily errors.
-- They show where Silver contained observations with on_ground IS NULL.

SELECT
    activity_date,
    origin_country,
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
-- 9) SAMPLE FOR MANUAL INSPECTION

SELECT *
FROM gold.opensky_activity_by_country_day
ORDER BY
    activity_date DESC,
    distinct_aircraft DESC
LIMIT 50;
