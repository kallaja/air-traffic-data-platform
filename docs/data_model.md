# Data model

## Overview

Air traffic platform:

```text
bronze (ADLS / Volume JSON, hive-style folders)
  → silver.opensky_states (PySpark Auto Loader + insert-only MERGE)
  → silver.dim_country (PySpark batch + SCD Type 2)
    → gold (Databricks SQL marts)
```

Classic DWH analogy: bronze ≈ landing; OpenSky silver ≈ fact; dim_country ≈ SCD2 dimension; gold ≈ marts for Power BI.

## Current sources

- OpenSky `/states/all` — daily snapshot for Europe bbox  
  Docs: https://openskynetwork.github.io/opensky-api/rest.html
- REST Countries API v5 — country reference (names, codes, capitals, geo, population)  
  Docs: https://restcountries.com/

## Grain (OpenSky silver)

1 row = one aircraft (`icao24`) in one API snapshot (`time` from the bronze file / one ingestion run).

Business key (MVP): `(snapshot_time_unix, icao24)`.

## Grain (dim_country)

Natural key: `country_cca2` (ISO 3166-1 alpha-2).

SCD Type 2: many rows per country over time; at most one row with `is_current = true`.  
As-of use: `activity_date >= valid_from AND activity_date < valid_to`.

## Layers

### Bronze (ADLS or UC Volume)

Raw JSON from ingestion scripts. Paths:

```text
bronze/source=opensky/entity=states/year=YYYY/month=MM/day=DD/opensky_states_….json
bronze/source=restcountries/entity=countries/year=YYYY/month=MM/day=DD/restcountries_countries_….json
```

Silver does not write cleaned rows back into bronze. Delta tables can be Unity Catalog managed (workspace) or later external on ADLS. OpenSky Auto Loader checkpoints live outside bronze.

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
| 7 | `baro_altitude` | float | yes | Barometric altitude in meters. |
| 8 | `on_ground` | boolean | no* | `true` if position came from a surface position report. |
| 9 | `velocity` | float | yes | Velocity over ground in m/s. |
| 10 | `true_track` | float | yes | True track in decimal degrees clockwise from north (north = 0°). |
| 11 | `vertical_rate` | float | yes | Vertical rate in m/s. Positive = climbing, negative = descending. |
| 12 | `sensors` | int[] / null | yes | IDs of receivers that contributed to this vector. Null if no sensor filter was used (typical for our requests). |
| 13 | `geo_altitude` | float | yes | Geometric altitude in meters. |
| 14 | `squawk` | string | yes | Transponder code (squawk). |
| 15 | `spi` | boolean | no* | Whether flight status indicates Special Purpose Indicator. |
| 16 | `position_source` | int | no* | Origin of position: `0` = ADS-B, `1` = ASTERIX, `2` = MLAT, `3` = FLARM. |
| 17 | `category` | int | no* | Aircraft category (0–20). See OpenSky docs. |

\*Usually present in practice; treat carefully in silver if null appears.

### Silver: `silver.opensky_states` (implemented)

Built in PySpark: Auto Loader reads new bronze JSON files, `explode(states)`, join ingestion metadata, insert-only Delta `MERGE`.

Script: `src/transformation/transform_opensky_states.py`  
Validation: `sql/silver/validate_opensky_states.sql`  
Older SQL version kept for reference: `sql/silver/create_opensky_states.sql`

#### Columns in MVP table

| Column | Type (SQL) | Source | Description |
|--------|------------|--------|-------------|
| `snapshot_time_utc` | TIMESTAMP | root.`time` | API snapshot time (Unix → timestamp). |
| `snapshot_time_unix` | BIGINT | root.`time` | Raw Unix `time`. |
| `icao24` | STRING | `icao24` | ICAO 24-bit address. |
| `callsign` | STRING | `callsign` | Trimmed; blank → null. |
| `origin_country` | STRING | `origin_country` | Country from ICAO address. |
| `time_position_utc` | TIMESTAMP | `time_position` | Last position update. |
| `last_contact_utc` | TIMESTAMP | `last_contact` | Last contact. |
| `longitude` | DOUBLE | `longitude` | WGS-84 longitude. |
| `latitude` | DOUBLE | `latitude` | WGS-84 latitude. |
| `baro_altitude_m` | DOUBLE | `baro_altitude` | Barometric altitude (m). |
| `on_ground` | BOOLEAN | `on_ground` | On-ground flag. |
| `velocity_ms` | DOUBLE | `velocity` | Ground speed (m/s). |
| `true_track_deg` | DOUBLE | `true_track` | Track from north (degrees). |
| `geo_altitude_m` | DOUBLE | `geo_altitude` | Geometric altitude (m). |
| `squawk` | STRING | `squawk` | Squawk code. |
| `spi` | BOOLEAN | `spi` | Special Purpose Indicator. |
| `position_source` | INT | `position_source` | 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM. |
| `category` | INT | `category` | Aircraft category 0–20. |
| `transformation_run_id` | STRING | job | UUID of the Silver run. |
| `source_file_path` | STRING | `_metadata.file_path` | Full URI of the bronze file. |
| `ingestion_run_id` | STRING | metadata `run_id` | Ingestion run that wrote the bronze file. |
| `bronze_load_datetime_utc` | TIMESTAMP | metadata | When ingestion started. |
| `silver_load_datetime_utc` | TIMESTAMP | job | When this Silver run wrote the row. |

`vertical_rate` stays in bronze only; it is not in this Silver table yet.

#### Planned

| Column | Notes |
|--------|--------|
| `sensors` | Usually null for our pulls |
| `snapshot_date` | Generated from `snapshot_time_utc` if the Delta table is partitioned later |

### Silver cleaning rules (MVP)

- Drop rows with null `icao24`
- `TRIM` callsign; blank → null
- Cast numerics to DOUBLE / INT
- Keep API nulls for optional fields (no invented values)
- Auto Loader + `multiLine` JSON; skip already-seen files via checkpoint
- Insert-only `MERGE` on `(snapshot_time_unix, icao24)`

### How to refresh OpenSky silver

1. Configure `BRONZE_BASE_PATH`, `METADATA_BASE_PATH`, `SILVER_OPENSKY_TABLE`, `SILVER_OPENSKY_CHECKPOINT_PATH`.
2. Run `src/transformation/transform_opensky_states.py` on a Databricks cluster.
3. Run validation queries in `sql/silver/validate_opensky_states.sql`.

### Silver: `silver.dim_country` (implemented)

Country dimension from REST Countries bronze. SCD Type 2: close current row when attributes change (`row_hash`), then insert a new current version.

Script: `src/transformation/transform_dim_country.py`  
Validation: `sql/silver/validate_dim_country.sql`  
Older SQL version kept for reference: `sql/silver/create_dim_country.sql`

Flow:

1. Batch-read bronze under `source=restcountries/entity=countries/` (`api_version = v5`).
2. Explode `countries`, clean codes/names, pick primary capital (else first).
3. Keep latest snapshot per `country_cca2` (`ROW_NUMBER` by `fetched_at_utc`).
4. Compute `row_hash` (md5 of business attributes).
5. MERGE: if current hash differs → set `is_current = false`, `valid_to = load_date`.
6. INSERT rows that have no current version (new country or new version after close).

#### Columns

| Column | Type (SQL) | Description |
|--------|------------|-------------|
| `country_cca2` | STRING | ISO alpha-2 code (natural key). |
| `country_cca3` | STRING | ISO alpha-3 code. |
| `name_common` | STRING | Common name. |
| `name_official` | STRING | Official name. |
| `region` | STRING | Region. |
| `subregion` | STRING | Subregion. |
| `capital` | STRING | Primary capital name (else first). |
| `capital_latitude` | DOUBLE | Capital latitude. |
| `capital_longitude` | DOUBLE | Capital longitude. |
| `country_latitude` | DOUBLE | Country centroid latitude. |
| `country_longitude` | DOUBLE | Country centroid longitude. |
| `population` | BIGINT | Population. |
| `area_km2` | DOUBLE | Area in km². |
| `row_hash` | STRING | MD5 of attributes (change detection). |
| `valid_from` | DATE | Start of this version. |
| `valid_to` | DATE | End of version; `9999-12-31` while current. |
| `is_current` | BOOLEAN | True for the open version. |
| `source_fetched_at_utc` | TIMESTAMP | `fetched_at_utc` from bronze. |

#### How to refresh dim_country

1. Configure `BRONZE_BASE_PATH`, `SILVER_DIM_COUNTRY_TABLE`.
2. Run `src/transformation/transform_dim_country.py` on a Databricks cluster (after REST Countries ingest).
3. Run `sql/silver/validate_dim_country.sql`.

### Still open

- Delta partitioning on Silver (`snapshot_date`)
- Auto Loader file notifications (Event Grid) on ADLS
- Proper Gold model (current activity table is just a test / connectivity check)
- Power BI dashboard (connection works; no report yet)
- Weather Silver from Open-Meteo
- OpenSky `origin_country` → `country_cca2` mapping

## Example (bronze → silver row)

Bronze state:

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

Silver row (conceptually):

| icao24 | callsign | origin_country | latitude | longitude | baro_altitude_m | velocity_ms | on_ground |
|--------|----------|----------------|----------|-----------|-----------------|-------------|-----------|
| 39de4e | TVF85JA | France | 45.1353 | 7.9727 | 11582.4 | 220.01 | false |
