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
                         Azure Functions
                         Timer Triggers
                                 │
                                 ↓
                            ADLS Gen2
                       Bronze + Metadata
                                 │
                                 ↓
                        Databricks SQL
                         Silver + Gold
                                 │
                                 ↓
                         Power BI (in progress)
```

## Technology Stack

- **Python** — ingestion scripts
- **OpenSky Network API** — aircraft state vectors
- **REST Countries API v5** — country dimension (SCD Type 2)
- **Open-Meteo API** — daily forecast for selected European capitals
- **Azure Data Lake Storage Gen2** — bronze + metadata containers
- **Azure Functions** — scheduled ingestion (Flex Consumption)
- **Azure Databricks / Databricks SQL** — Silver and Gold transformations
- **Delta Lake** — incremental tables and SCD Type 2 history
- **Unity Catalog** — organization of Silver and Gold data assets
- **Power BI** — analytical reporting *(in progress)*

## Project Structure

```
src/
  ingestion/
    ingest_opensky_states.py
    ingest_restcountries_countries.py
    ingest_openmeteo_forecast.py
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

docs/
  data_model.md
  metadata_schema.md

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

OpenSky grain: one row = one aircraft (`icao24`) in one API snapshot.  
`dim_country` natural key: `country_cca2` with SCD2 history (`valid_from` / `valid_to` / `is_current`).  
See `docs/data_model.md`.

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
| OpenSky | daily **10:00 UTC** |
| Open-Meteo | daily **10:15 UTC** |
| REST Countries | Mondays **10:30 UTC** |

After deploy, set the same secrets in Function App settings (including `REST_COUNTRIES_API_KEY`).

### 3. Refresh silver / gold (Databricks SQL)

1. Start SQL Warehouse (e.g. `DWH_aircraft`).
2. Run `sql/silver/create_opensky_states.sql` (incremental `MERGE`).
3. Run `sql/silver/create_dim_country.sql` (SCD2; typically weekly after REST Countries ingest).
4. Run `sql/gold/create_opensky_activity_by_country_day.sql`.
5. Optionally run matching `validate_*.sql` scripts.

`dim_country` requires at least one successful `ingest_restcountries_countries.py` run first.

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
- [x] Silver validation queries
- [x] Initial Gold mart — aircraft activity by country/day
- [x] Unity Catalog structure for Silver and Gold
- [x] Power BI connectivity

### Planned

- [ ] OpenSky `origin_country` → ISO `country_cca2` mapping
- [ ] Silver weather transformation
- [ ] Define weather-to-air-traffic mapping strategy
- [ ] Integrate weather data into Gold analytics
- [ ] Extended Gold analytical model
- [ ] Production-quality Power BI dashboard
- [ ] PySpark transformations
- [ ] Databricks Jobs orchestration
- [ ] File-level incremental processing / watermarking
- [ ] Automated data quality checks
- [ ] Aircraft reference data enrichment
- [ ] Unity Catalog access-control examples

> **Incremental processing:** Silver currently uses an idempotent `MERGE`, while Bronze files are still scanned recursively. File-level watermarking is planned to process only newly ingested Bronze files.

## License

Personal educational project.
Third-party datasets and APIs remain subject to their respective licenses and terms of use.
