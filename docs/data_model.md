# Data model

## Overview

Air traffic platform: bronze (ADLS) → silver (Databricks SQL) → gold

## Current sources

- OpenSky `/states/all` — daily snapshot for Europe bbox  
  Docs: https://openskynetwork.github.io/opensky-api/rest.html

## Grain (OpenSky silver)

**1 row = one aircraft (`icao24`) in one API snapshot** (`time` / one ingestion run).

## Layers

### Bronze

- Raw JSON from OpenSky (response object as stored by ingestion).
- Path:

```text
bronze/source=opensky/entity=states/year=YYYY/month=MM/day=DD/opensky_states_YYYYMMDDTHHMMSSZ.json
```

#### Response-level fields (file root)

| Field | Type (API) | Description |
|-------|------------|-------------|
| `time` | integer | Unix time associated with the state vectors in this response. Vectors represent vehicle state in the interval `[time - 1, time]`. |
| `states` | array | List of state vectors (one object per aircraft in our bronze JSON). |

#### State vector fields (each element of `states`)

Source: OpenSky REST API — All State Vectors response.

| Index | Field | Type (API) | Nullable | Description |
|-------|-------|------------|----------|-------------|
| 0 | `icao24` | string | no* | Unique ICAO 24-bit transponder address (hex string). |
| 1 | `callsign` | string | yes | Callsign (up to 8 chars, often space-padded). Null if not received. |
| 2 | `origin_country` | string | no* | Country name inferred from the ICAO 24-bit address. |
| 3 | `time_position` | int | yes | Unix timestamp (seconds) of last position update. Null if no position report within ~15s. |
| 4 | `last_contact` | int | no* | Unix timestamp (seconds) of last update in general (any valid transponder message). |
| 5 | `longitude` | float | yes | WGS-84 longitude in decimal degrees. |
| 6 | `latitude` | float | yes | WGS-84 latitude in decimal degrees. |
| 7 | `baro_altitude` | float | yes | Barometric altitude in **meters**. |
| 8 | `on_ground` | boolean | no* | `true` if position came from a surface position report. |
| 9 | `velocity` | float | yes | Velocity over ground in **m/s**. |
| 10 | `true_track` | float | yes | True track in decimal degrees clockwise from north (north = 0°). |
| 11 | `vertical_rate` | float | yes | Vertical rate in **m/s**. Positive = climbing, negative = descending. |
| 12 | `sensors` | int[] / null | yes | IDs of receivers that contributed to this vector. Null if no sensor filter was used (typical for our requests). |
| 13 | `geo_altitude` | float | yes | Geometric altitude in **meters**. |
| 14 | `squawk` | string | yes | Transponder code (squawk). |
| 15 | `spi` | boolean | no* | Whether flight status indicates Special Purpose Indicator. |
| 16 | `position_source` | int | no* | Origin of position: `0` = ADS-B, `1` = ASTERIX, `2` = MLAT, `3` = FLARM. |
| 17 | `category` | int | no* | Aircraft category (0–20). See OpenSky docs (0 = no information, 2 = Light, 6 = Heavy, 8 = Rotorcraft, …). |

\*Usually present in practice; treat carefully in silver if null appears.

### Silver: `silver.opensky_states`

Flattened state vectors + pipeline context. One row per aircraft per snapshot.

| Column | Type (SQL) | Source field | Nullable | Description |
|--------|------------|--------------|----------|-------------|
| `run_id` | STRING | metadata.`run_id` | no | Ingestion run UUID. |
| `load_datetime_utc` | TIMESTAMP | metadata.`load_datetime_utc` | no | When the ingestion job started (UTC). |
| `snapshot_time_utc` | TIMESTAMP | root.`time` | no | API snapshot time (`time`) converted from Unix seconds to UTC timestamp. |
| `snapshot_time_unix` | BIGINT | root.`time` | no | Raw Unix `time` from the bronze file. |
| `icao24` | STRING | `icao24` | no | ICAO 24-bit address. |
| `callsign` | STRING | `callsign` | yes | Callsign; in silver: `TRIM(callsign)`, empty string → null. |
| `origin_country` | STRING | `origin_country` | yes | Country inferred from ICAO address. |
| `time_position_utc` | TIMESTAMP | `time_position` | yes | Last position update (Unix → UTC). |
| `last_contact_utc` | TIMESTAMP | `last_contact` | yes | Last contact (Unix → UTC). |
| `longitude` | DOUBLE | `longitude` | yes | WGS-84 longitude (degrees). |
| `latitude` | DOUBLE | `latitude` | yes | WGS-84 latitude (degrees). |
| `baro_altitude_m` | DOUBLE | `baro_altitude` | yes | Barometric altitude (meters). |
| `on_ground` | BOOLEAN | `on_ground` | yes | Surface / on-ground flag. |
| `velocity_ms` | DOUBLE | `velocity` | yes | Ground speed (m/s). |
| `true_track_deg` | DOUBLE | `true_track` | yes | Track angle degrees from north. |
| `vertical_rate_ms` | DOUBLE | `vertical_rate` | yes | Climb/descent rate (m/s). |
| `sensors` | STRING | `sensors` | yes | Serialized sensor IDs if present; usually null for our pulls. |
| `geo_altitude_m` | DOUBLE | `geo_altitude` | yes | Geometric altitude (meters). |
| `squawk` | STRING | `squawk` | yes | Squawk code. |
| `spi` | BOOLEAN | `spi` | yes | Special Purpose Indicator. |
| `position_source` | INT | `position_source` | yes | 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM. |
| `category` | INT | `category` | yes | Aircraft category code (0–20). |
| `year` | INT | partition / `load_datetime_utc` | no | Partition helper from load date. |
| `month` | INT | partition / `load_datetime_utc` | no | Partition helper from load date. |
| `day` | INT | partition / `load_datetime_utc` | no | Partition helper from load date. |

### Keys / uniqueness

- Business key: `(snapshot_time_unix, icao24)` or `(run_id, icao24)`
- Expected: one row per aircraft per successful daily run (same snapshot)

### Silver cleaning rules (MVP)

- Drop rows with null `icao24`
- `TRIM` callsign; blank → null
- Keep API nulls for optional numeric fields
- Do not invent values for missing altitude / position

### Not in scope yet

- Staging as a separate table (optional later)
- Open-Meteo / other APIs
- Gold marts / Power BI models

## Example (bronze → conceptual silver row)

From bronze state:

```json
{
  "icao24": "39de4e",
  "callsign": "TVF85JA ",
  "origin_country": "France",
  "longitude": 7.9727,
  "latitude": 45.1353,
  "baro_altitude": 11582.4,
  "on_ground": false,
  "velocity": 220.01
}
```

Silver intent:

| icao24 | callsign | origin_country | latitude | longitude | baro_altitude_m | velocity_ms | on_ground |
|--------|----------|----------------|----------|-----------|-----------------|-------------|-----------|
| 39de4e | TVF85JA | France | 45.1353 | 7.9727 | 11582.4 | 220.01 | false |
