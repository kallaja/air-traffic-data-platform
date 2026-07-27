# Air Traffic Data Platform

Cloud-native data platform for collecting and analyzing European air traffic data.  
Personal project demonstrating batch ingestion, Azure Data Lake Storage Gen2, Databricks SQL, and a Bronze / Silver / Gold architecture.

## Problem Statement

Aviation data is spread across multiple APIs and formats. This platform collects raw aircraft state snapshots, stores them in a scalable data lake, and prepares them for downstream analytics and reporting.

## Architecture (MVP)

```
OpenSky API
    → Python ingestion (+ Azure Functions timer)
    → ADLS Gen2 (bronze + metadata)
    → Databricks SQL (silver)
    → Gold / Power BI (planned)
```

## Technology Stack

- **Python** — ingestion scripts
- **OpenSky Network API** — aircraft state vectors
- **Azure Data Lake Storage Gen2** — bronze + metadata containers
- **Azure Functions** — scheduled daily ingestion (Flex Consumption)
- **Azure Databricks SQL** — bronze → silver transforms
- **Azure Identity / Access Connector** — secure ADLS access from Databricks

## Project Structure

```
src/
  ingestion/
    ingest_opensky_states.py   # OpenSky states → ADLS bronze
  validation/
    validate_last_run.py       # check latest metadata run
  utils/
    storage.py                 # ADLS helpers
sql/
  silver/
    create_opensky_states.sql  # Databricks SQL: flatten bronze → silver
    validate_opensky_states.sql
function_app.py                # timer trigger (daily 10:00 UTC)
config/
  sources.yml
docs/
  data_model.md
  metadata_schema.md
```

## Data Layout

**ADLS `bronze`** — raw JSON:

```text
source=opensky/entity=states/year=YYYY/month=MM/day=DD/opensky_states_….json
```

**ADLS `metadata`** — pipeline run logs:

```text
pipeline_runs/source=opensky/entity=states/year=YYYY/month=MM/day=DD/…_metadata.json
```

**Databricks** — cleaned table:

```text
silver.opensky_states
```

Grain: one row = one aircraft (`icao24`) in one API snapshot. See `docs/data_model.md`.

## Getting Started

### 1. Local environment

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
cp .env.example .env
```

### 2. Run ingestion (manual)

```bash
python src/ingestion/ingest_opensky_states.py
python src/validation/validate_last_run.py
```

Scheduled runs: Azure Function timer (`function_app.py`) at **10:00 UTC**.

### 3. Refresh silver (Databricks SQL)

1. Start SQL Warehouse (e.g. `DWH_aircraft`, 2X-Small, auto-stop on).
2. Run `sql/silver/create_opensky_states.sql`.
3. Optionally run `sql/silver/validate_opensky_states.sql`.
4. **Stop** the SQL Warehouse when finished (billing is for running time).

## Data Coverage

Continental Europe bbox:

```text
(35.0, 72.0, -12.0, 42.0)  # min_lat, max_lat, min_lon, max_lon
```

## Roadmap

- [x] Bronze ingestion to ADLS
- [x] Metadata + last-run validation
- [x] Scheduled ingestion (Azure Functions)
- [x] Silver layer (Databricks SQL)
- [ ] Gold layer aggregations
- [ ] Power BI dashboards
- [ ] Additional sources (weather, airport metadata)

## License

Personal project — not for commercial use of third-party API data without checking respective terms.
