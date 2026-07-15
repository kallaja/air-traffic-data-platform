# Air Traffic Data Platform

Cloud-native data platform for collecting and analyzing European air traffic data.  
Personal project demonstrating batch ingestion, Azure Data Lake Storage Gen2, and a Bronze / Silver / Gold architecture.

## Problem Statement

Aviation data is spread across multiple APIs and formats. This platform collects raw aircraft state snapshots, stores them in a scalable data lake, and prepares them for downstream analytics and reporting.

## Architecture (MVP)

```
OpenSky API  →  Python ingestion  →  ADLS Gen2 (bronze)  →  Databricks (silver/gold)  →  Power BI
```

## Technology Stack

- **Python** — ingestion scripts
- **OpenSky Network API** — aircraft state vectors
- **Azure Data Lake Storage Gen2** — bronze + metadata containers
- **Azure Identity** — authentication

## Project Structure

```
src/
  ingestion/
    ingest_opensky_states.py   # daily OpenSky states → bronze
  utils/
    storage.py                 # ADLS upload helpers
config/
  sources.yml                  # source config (bbox, schedule)
docs/                          # architecture, ADRs
```

## Data Lake Layout

**Container `bronze`** — raw data:

```
source=opensky/entity=states/year=YYYY/month=MM/day=DD/opensky_states_YYYYMMDDTHHMMSSZ.json
```

**Container `metadata`** — pipeline run logs:

```
pipeline_runs/source=opensky/entity=states/year=YYYY/month=MM/day=DD/opensky_states_YYYYMMDDTHHMMSSZ_metadata.json
```

## Getting Started

### 1. Clone and set up environment

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy the template and fill in your values:

```bash
cp .env.example .env
```

Required variables:

- `OPEN_SKY_CLIENT_ID`, `OPEN_SKY_CLIENT_SECRET`
- `AZURE_STORAGE_CONNECTION_STRING` (local dev) **or** `AZURE_STORAGE_ACCOUNT_NAME` (Azure Identity)
- `AZURE_CONTAINER_NAME=bronze`
- `AZURE_METADATA_CONTAINER_NAME=metadata`

### 3. Create ADLS containers

In Azure Portal → Storage Account → Containers, create:

- `bronze`
- `metadata`

### 4. Run ingestion

```bash
python src/ingestion/ingest_opensky_states.py
```

Run once per day. Each execution creates a timestamped file in bronze and a matching metadata record.

## Data Coverage

Current MVP ingests aircraft states for **continental Europe**:

```
bbox: (35.0, 72.0, -12.0, 42.0)  # min_lat, max_lat, min_lon, max_lon
```

## Roadmap

- [ ] Silver layer transforms (Databricks)
- [ ] Gold layer aggregations
- [ ] Power BI dashboards
- [ ] Scheduled daily runs (Azure Functions / ADF)
- [ ] Additional sources (weather, airport metadata)

## License

Personal project — not for commercial use of third-party API data without checking respective terms.