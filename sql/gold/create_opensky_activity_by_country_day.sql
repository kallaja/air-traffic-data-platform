CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.opensky_activity_by_country_day AS
SELECT
    CAST(s.snapshot_time_utc AS DATE) AS activity_date,
    s.origin_country,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT s.icao24) AS distinct_aircraft,
    AVG(s.baro_altitude_m) AS avg_baro_altitude_m,
    AVG(s.velocity_ms) AS avg_velocity_ms,
    COUNT(CASE WHEN s.on_ground = true THEN 1 END) AS on_ground_count,
    COUNT(CASE WHEN s.on_ground = false THEN 1 END) AS in_air_count,
    COUNT(CASE WHEN s.on_ground IS NULL THEN 1 END) AS unknown_on_ground_count
FROM silver.opensky_states AS s
GROUP BY
    CAST(s.snapshot_time_utc AS DATE),
    s.origin_country;
    