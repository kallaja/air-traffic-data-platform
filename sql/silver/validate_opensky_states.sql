-- Validate silver.opensky_states (MVP checks)
-- Run in Databricks SQL after creating / refreshing the silver table.

-- 1) Row count (expect > 0 after a successful bronze load)
SELECT COUNT(*) AS row_count
FROM silver.opensky_states;

-- 2) Grain / uniqueness: one aircraft per snapshot
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT snapshot_time_unix, icao24) AS distinct_snapshot_aircraft
FROM silver.opensky_states;
-- Expect: total_rows ≈ distinct_snapshot_aircraft (no duplicate keys)

-- 3) Required fields
SELECT
  SUM(CASE WHEN icao24 IS NULL THEN 1 ELSE 0 END) AS null_icao24,
  SUM(CASE WHEN snapshot_time_unix IS NULL THEN 1 ELSE 0 END) AS null_snapshot_time
FROM silver.opensky_states;
-- Expect: both = 0

-- 4) Plausible geo ranges (Europe bbox-ish; nulls allowed)
SELECT
  SUM(CASE WHEN latitude IS NOT NULL AND (latitude < 35 OR latitude > 72) THEN 1 ELSE 0 END) AS lat_out_of_bbox,
  SUM(CASE WHEN longitude IS NOT NULL AND (longitude < -12 OR longitude > 42) THEN 1 ELSE 0 END) AS lon_out_of_bbox
FROM silver.opensky_states;

-- 5) Callsign padding check (should be trimmed in silver)
SELECT COUNT(*) AS callsigns_with_trailing_space
FROM silver.opensky_states
WHERE callsign LIKE '% ';
-- Expect: 0

-- 6) Sample for manual inspection
SELECT
  snapshot_time_utc,
  icao24,
  callsign,
  origin_country,
  latitude,
  longitude,
  baro_altitude_m,
  velocity_ms,
  on_ground
FROM silver.opensky_states
ORDER BY snapshot_time_utc DESC, origin_country, icao24
LIMIT 50;

-- 7) Simple distribution (sanity)
SELECT origin_country, COUNT(*) AS n
FROM silver.opensky_states
GROUP BY origin_country
ORDER BY n DESC;
