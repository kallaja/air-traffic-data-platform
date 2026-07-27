CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.opensky_states AS
SELECT
    to_timestamp(bronze.time) AS snapshot_time_utc,
    bronze.time AS snapshot_time_unix,
    s.icao24,
    NULLIF(TRIM(s.callsign), '') AS callsign,
    s.origin_country,
    to_timestamp(s.time_position) AS time_position_utc,
    to_timestamp(s.last_contact) AS last_contact_utc,
    CAST(s.longitude AS DOUBLE) AS longitude,
    CAST(s.latitude AS DOUBLE) AS latitude,
    CAST(s.baro_altitude AS DOUBLE) AS baro_altitude_m,
    s.on_ground,
    CAST(s.velocity AS DOUBLE) AS velocity_ms,
    CAST(s.true_track AS DOUBLE) AS true_track_deg,
    CAST(s.vertical_rate AS DOUBLE) AS vertical_rate_ms,
    CAST(s.geo_altitude AS DOUBLE) AS geo_altitude_m,
    s.squawk,
    s.spi,
    CAST(s.position_source AS INT) AS position_source,
    CAST(s.category AS INT) AS category
FROM read_files(
  'abfss://bronze@papstorage001.dfs.core.windows.net/source=opensky/entity=states/',
  format => 'json',
  schemaEvolutionMode => 'none',
  multiLine => true,
  recursiveFileLookup => true
) AS bronze
LATERAL VIEW explode(bronze.states) e AS s
WHERE s.icao24 IS NOT NULL;
