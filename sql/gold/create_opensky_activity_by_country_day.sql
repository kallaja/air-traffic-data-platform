CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.opensky_activity_by_country_day AS
SELECT
    cast(s.snapshot_time_utc AS DATE) AS activity_date,
    s.origin_country,
    count(*) AS observation_count,
    count(distinct s.icao24) AS distinct_aircraft,
    avg(s.baro_altitude_m) AS avg_baro_altitude_m,
    avg(s.velocity_ms) AS avg_velocity_ms,
    count(CASE WHEN s.on_ground = true THEN 1 END) AS on_ground_count,
    count(CASE WHEN s.on_ground = false THEN 1 END) AS in_air_count
FROM
    silver.opensky_states AS s
GROUP BY
    activity_date,
    s.origin_country;
    