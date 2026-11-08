# Databricks Jobs

Orchestration for Bronze → Silver on Databricks.  
Developed and scheduled on **Databricks Free Edition** (Unity Catalog Volumes). The same jobs are designed to run against **ADLS Gen2** by changing path env vars (`BRONZE_BASE_PATH`, etc.) — see `.env.example`.

Hard-fail **data quality** lives inside the transformation scripts (`raise ValueError`), not as separate Job tasks. A failed DQ check fails the task; dependent downstream tasks are **skipped**.

## Job overview

| Job | Cadence | Tasks (order) | Purpose |
|-----|---------|---------------|---------|
| OpenSky daily | Daily (cron; e.g. aligned with 10:00 UTC ingest) | 1) ingest OpenSky → 2) transform silver `opensky_states` | Fresh aircraft snapshots into Delta silver |
| REST Countries weekly | Weekly (cron; e.g. Monday) | 1) ingest REST Countries → 2) transform silver `dim_country` (SCD2) | Refresh country dimension with history |

Two jobs (not one) because the sources have different refresh frequencies.

Exact clock times are set in the Job **Schedule** (Quartz/cron), not only as a vague “daily / weekly” label — e.g. `0 0 10 * * ?` (daily 10:00) or `0 0 10 ? * MON` (Monday 10:00). Replace with the cron shown in your Job UI if different.

## Job: OpenSky daily

```text
[Task 1: ingest_opensky]
        │
        ▼  (runs only if Task 1 succeeds)
[Task 2: transform_opensky_states]
```

| | |
|--|--|
| **Task 1** | Notebook / script that runs OpenSky ingest into bronze (+ metadata) on the Volume (or ADLS in Azure). |
| **Task 2** | `src/transformation/transform_opensky_states.py` — Auto Loader + insert-only MERGE. |
| **DQ** | Inside Task 2: ingestion metadata, bronze↔metadata coverage, silver staging (required fields, grain `(snapshot_time_unix, icao24)`, snapshot vs `api_time`). |
| **Env / Job params** | `BRONZE_BASE_PATH`, `METADATA_BASE_PATH`, `SILVER_OPENSKY_TABLE`, `SILVER_OPENSKY_CHECKPOINT_PATH` (+ OpenSky API secrets for ingest). |

Optional manual sanity after a run: `sql/silver/validate_opensky_states.sql`.

## Job: REST Countries weekly

```text
[Task 1: ingest_restcountries]
        │
        ▼
[Task 2: transform_dim_country]
```

| | |
|--|--|
| **Task 1** | Notebook / script that runs REST Countries ingest into bronze (+ metadata). |
| **Task 2** | `src/transformation/transform_dim_country.py` — batch bronze → staging → SCD2 close + insert. |
| **DQ** | Staging checks before write; post-SCD2 hard-fail on the silver table (one current row per `country_cca2`, required fields, `valid_from` / `valid_to` / `is_current`). |
| **Env / Job params** | `BRONZE_BASE_PATH`, `SILVER_DIM_COUNTRY_TABLE` (+ `REST_COUNTRIES_API_KEY` for ingest). |

Optional manual sanity: `sql/silver/validate_dim_country.sql`.

## Failure behaviour

| Concept | Meaning in this project |
|---------|-------------------------|
| **Status** | Job / task Succeeded or Failed (Workflows → Job → Run). |
| **Logs** | Open the failed **task** → Output / Logs (traceback from DQ `raise` or ingest errors). |
| **Retry** | Ingest: 1 retry after 2 minutes (transient API issues). Silver: no retries — DQ / transform failures are not retried. |
| **Downstream skip** | If ingest fails, silver does not run. |

## Storage paths (Free vs Azure)

Same code; different prefixes:

| | Free Edition (current) | Azure + ADLS Gen2 (target smoke) |
|--|------------------------|----------------------------------|
| Bronze | `/Volumes/.../bronze/` | `abfss://bronze@....dfs.core.windows.net/` |
| Metadata | `/Volumes/.../metadata/` | `abfss://metadata@....dfs.core.windows.net/` |
| Checkpoint (OpenSky) | `/Volumes/.../checkpoints/opensky_states` | durable path outside bronze (e.g. on metadata container) |

Databricks does **not** load `.env` automatically — set the same variables on the Job or cluster.

## Out of scope (for now)

- Gold refresh as a Job task (add after the gold mart is finalized; typically after OpenSky silver, not necessarily after every countries run).
- Separate Job tasks only for DQ (DQ is embedded in transforms).
- Open-Meteo silver / job (ingest exists; silver later).
