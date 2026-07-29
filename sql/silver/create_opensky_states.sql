-- Load silver.opensky_states incrementally (append-only fact).
-- Pattern (classic DWH):
-- 1) CREATE TABLE IF NOT EXISTS (keep history)
-- 2) Stage all bronze state vectors
-- 3) MERGE: INSERT only rows whose business key is new
--
-- Business key: (snapshot_time_unix, icao24)
-- Snapshots are immutable events → no SCD2 / no UPDATE on match.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.opensky_states (
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
  vertical_rate_ms DOUBLE,
  geo_altitude_m DOUBLE,
  squawk STRING,
  spi BOOLEAN,
  position_source INT,
  category INT
);

CREATE OR REPLACE TEMP VIEW stg_opensky_states AS
SELECT
  to_timestamp(bronze.time) AS snapshot_time_utc,
  CAST(bronze.time AS BIGINT) AS snapshot_time_unix,
  LOWER(TRIM(s.icao24)) AS icao24,
  NULLIF(TRIM(s.callsign), '') AS callsign,
  NULLIF(TRIM(s.origin_country), '') AS origin_country,
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

MERGE INTO silver.opensky_states AS t
USING stg_opensky_states AS s
ON t.snapshot_time_unix = s.snapshot_time_unix
AND t.icao24 = s.icao24
WHEN NOT MATCHED THEN INSERT (
  snapshot_time_utc,
  snapshot_time_unix,
  icao24,
  callsign,
  origin_country,
  time_position_utc,
  last_contact_utc,
  longitude,
  latitude,
  baro_altitude_m,
  on_ground,
  velocity_ms,
  true_track_deg,
  vertical_rate_ms,
  geo_altitude_m,
  squawk,
  spi,
  position_source,
  category
) VALUES (
  s.snapshot_time_utc,
  s.snapshot_time_unix,
  s.icao24,
  s.callsign,
  s.origin_country,
  s.time_position_utc,
  s.last_contact_utc,
  s.longitude,
  s.latitude,
  s.baro_altitude_m,
  s.on_ground,
  s.velocity_ms,
  s.true_track_deg,
  s.vertical_rate_ms,
  s.geo_altitude_m,
  s.squawk,
  s.spi,
  s.position_source,
  s.category
);
