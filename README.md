# Air Traffic Data Platform

Cloud-native data engineering platform for collecting, processing, and analyzing European air traffic data.  
The platform combines aircraft state data with country reference data and weather forecasts, and processes them through a Bronze / Silver / Gold architecture using Azure Data Lake Storage Gen2 and Azure Databricks.

## Problem Statement

Air traffic data is distributed across multiple APIs and has different structures, update frequencies, and levels of granularity.

This project builds an end-to-end data platform that:
- ingests aircraft state snapshots from OpenSky,
- ingests country reference data and weather forecasts from additional sources,
- stores immutable raw API responses in a data lake,
- transforms raw data into curated analytical tables,
- preserves historical changes in reference data,
- and prepares curated datasets for downstream analytics in Power BI.

## Architecture (MVP)

```
OpenSky API ──────────┐
REST Countries API ───┼──→ Python ingestion
Open-Meteo API ───────┘          │
                                 ↓
              Azure Functions (timer)  and/or  Databricks Jobs
                                 │
                                 ↓
            Bronze + Metadata  (UC Volume now; ADLS Gen2 target)
                                 │
                                 ↓
                         Databricks
    Silver: OpenSky (Auto Loader + MERGE) + dim_country (SCD2)
    Gold:   opensky_activity_by_country_day (SQL: staging → assert → promote)
                                 │
                                 ↓
                    Power BI via SQL Warehouse (report in progress)
```

## Technology Stack

- **Python** — ingestion scripts
- **OpenSky Network API** — aircraft state vectors
- **REST Countries API v5** — country dimension (SCD Type 2)
- **Open-Meteo API** — daily forecast for selected European capitals
- **Unity Catalog Volumes / ADLS Gen2** — bronze + metadata
- **Azure Functions** — scheduled ingestion on Azure (Flex Consumption); Databricks Jobs for bronze→silver→gold on Databricks
- **Azure Databricks** — OpenSky Silver (Auto Loader + MERGE), dim_country (SCD2), Gold SQL mart
- **Delta Lake** — incremental tables and SCD Type 2 history
- **Unity Catalog** — Silver and Gold tables
- **Power BI** — connects to published gold via SQL Warehouse *(dashboard in progress)*

## Project Structure

```
src/
  ingestion/
    ingest_opensky_states.py
    ingest_restcountries_countries.py
    ingest_openmeteo_forecast.py
  transformation/
    transform_opensky_states.py
    transform_dim_country.py
    country_cca2_mapping.py
    backfill_opensky_country_cca2.py
  validation/
    validate_last_run.py
  utils/
    storage.py

sql/
  silver/
    create_opensky_states.sql
    validate_opensky_states.sql
    create_dim_country.sql
    validate_dim_country.sql
  gold/
    create_opensky_activity_by_country_day.sql
    validate_opensky_activity_by_country_day.sql

config/
  sources.yml
  origin_country_overrides.json

docs/
  data_model.md
  metadata_schema.md
  databricks_jobs.md

function_app.py
```

## Data Layout

**ADLS `bronze`** — raw JSON:

```text
source=opensky/entity=states/year=YYYY/month=MM/day=DD/opensky_states_….json
source=restcountries/entity=countries/year=YYYY/month=MM/day=DD/restcountries_countries_….json
source=openmeteo/entity=forecast/year=YYYY/month=MM/day=DD/openmeteo_forecast_….json
```

**ADLS `metadata`** — structured ingestion run metadata:

```text
pipeline_runs/source=opensky/entity=states/year=YYYY/month=MM/day=DD/…_metadata.json
pipeline_runs/source=restcountries/entity=countries/…
pipeline_runs/source=openmeteo/entity=forecast/…
```

**Databricks / Unity Catalog** — cleaned / mart tables:

```text
databricks_aircraft.silver.opensky_states
databricks_aircraft.silver.dim_country
databricks_aircraft.gold.opensky_activity_by_country_day
```

OpenSky silver grain: one row = one aircraft (`icao24`) in one API snapshot.  
`dim_country` natural key: `country_cca2` with SCD2 history (`valid_from` / `valid_to` / `is_current`).  
Gold grain: one row per `(activity_date, country_cca2)` (missing cca2 → `Unknown`).  
See `docs/data_model.md` and `docs/databricks_jobs.md`.

## Getting Started

### 1. Local environment

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
cp .env.example .env
```

Required secrets in `.env` (see `.env.example`): OpenSky OAuth, Azure Storage, `REST_COUNTRIES_API_KEY`.

### 2. Run ingestion (manual)

```bash
python src/ingestion/ingest_opensky_states.py
python src/ingestion/ingest_restcountries_countries.py
python src/ingestion/ingest_openmeteo_forecast.py
python src/validation/validate_last_run.py
```

Scheduled ingestion is configured in `function_app.py` using Azure Functions Timer Triggers:

| Source | Schedule |
|--------|----------|
| OpenSky | daily 10:00 UTC |
| Open-Meteo | daily 10:15 UTC |
| REST Countries | Mondays 10:30 UTC |

After deploy, set the same secrets in Function App settings (including `REST_COUNTRIES_API_KEY`).

### 3. Refresh OpenSky silver (Databricks PySpark)

Use `src/transformation/transform_opensky_states.py`.

Set these on the cluster or notebook (Databricks does not load `.env`):

- `BRONZE_BASE_PATH`
- `METADATA_BASE_PATH`
- `SILVER_OPENSKY_TABLE` (`catalog.schema.table`)
- `SILVER_DIM_COUNTRY_TABLE` (`catalog.schema.table`) — used to map `origin_country` → `country_cca2`
- `SILVER_OPENSKY_CHECKPOINT_PATH` — separate folder per pipeline, not under bronze  
  Example: `/Volumes/workspace/default/air_traffic_data/checkpoints/opensky_states`
- `ORIGIN_COUNTRY_OVERRIDES_PATH` — path to `config/origin_country_overrides.json`
- `TRANSFORMATION_CODE_PATH` — folder containing `country_cca2_mapping.py` (e.g. `/Volumes/.../src` or `src/transformation`)

The job uses Auto Loader (`availableNow`) for new bronze JSON files, then insert-only Delta `MERGE`.  
Maps `origin_country` → `country_cca2` (logical FK to `dim_country`).  
One-off backfill for existing silver rows: `src/transformation/backfill_opensky_country_cca2.py`.  
`sql/silver/create_opensky_states.sql` is the older SQL version; leave it as reference and do not run it on the PySpark table.

### 4. Refresh dim_country (Databricks PySpark, SCD2)

Use `src/transformation/transform_dim_country.py` after a successful REST Countries bronze load.

Set on the cluster or notebook:

- `BRONZE_BASE_PATH` (same root as OpenSky)
- `SILVER_DIM_COUNTRY_TABLE` (`catalog.schema.table`)

Batch read of bronze snapshots, latest row per `country_cca2`, then SCD2 close + insert.  
`sql/silver/create_dim_country.sql` is the older SQL version; leave it as reference.  
Validate with `sql/silver/validate_dim_country.sql`.

Scheduled Bronze → Silver → Gold runs are documented in [`docs/databricks_jobs.md`](docs/databricks_jobs.md) (OpenSky daily including gold; REST Countries weekly for dim only).

### 5. Refresh gold activity mart (Databricks SQL)

Run `sql/gold/create_opensky_activity_by_country_day.sql` (Job task after OpenSky silver, or manually on a SQL Warehouse).

The script rebuilds `gold.opensky_activity_by_country_day_staging`, hard-fails on grain + silver reconcile (`assert_true`), then promotes to `gold.opensky_activity_by_country_day`.  
Optional diagnostics on the published table: `sql/gold/validate_opensky_activity_by_country_day.sql`.

## Data Coverage

Continental Europe bbox (OpenSky):

```text
(35.0, 72.0, -12.0, 42.0)  # min_lat, max_lat, min_lon, max_lon
```

Open-Meteo MVP locations: selected European capitals listed in `config/sources.yml`.

## Current Project Status

### Implemented

- [x] Multi-source REST API ingestion (OpenSky, REST Countries, Open-Meteo)
- [x] ADLS Gen2 Bronze storage
- [x] Date-partitioned Bronze layout
- [x] Structured ingestion run metadata
- [x] Scheduled ingestion with Azure Functions
- [x] Silver OpenSky transformation
- [x] Incremental/idempotent Silver `MERGE`
- [x] Country dimension with SCD Type 2 history
- [x] PySpark dim_country (SCD2 close + insert)
- [x] Silver validation queries
- [x] Gold mart — daily activity by `country_cca2` (staging → assert → promote)
- [x] Unity Catalog structure for Silver and Gold
- [x] Power BI connectivity (SQL Warehouse → published gold)
- [x] PySpark OpenSky Silver (Auto Loader + insert-only MERGE)
- [x] File-level incremental processing (Auto Loader checkpoint)
- [x] Silver hard-fail DQ (OpenSky staging; dim_country staging + post-SCD2)
- [x] Gold hard-fail DQ before promote (grain + silver reconcile)
- [x] Databricks Jobs orchestration (see `docs/databricks_jobs.md`)
- [x] OpenSky `origin_country` → ISO `country_cca2` mapping

### Planned

- [ ] Silver weather transformation
- [ ] Define weather-to-air-traffic mapping strategy
- [ ] Integrate weather data into Gold analytics
- [ ] Extended Gold analytical model
- [ ] Production-quality Power BI dashboard
- [ ] Aircraft reference data enrichment
- [ ] Unity Catalog access-control examples

> Incremental processing: OpenSky Silver uses Auto Loader plus insert-only MERGE on `(snapshot_time_unix, icao24)`. dim_country uses batch bronze read and SCD2. OpenSky Job also rebuilds gold (staging → assert → promote). See `docs/databricks_jobs.md`.

## License

Personal educational project.
Third-party datasets and APIs remain subject to their respective licenses and terms of use.
