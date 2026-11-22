# Databricks Jobs

Orchestration for Bronze → Silver → Gold on Databricks.  
Developed and scheduled on Databricks with Unity Catalog Volumes. The same jobs are designed to run against **ADLS Gen2** by changing path env vars (`BRONZE_BASE_PATH`, etc.) — see `.env.example`.

Hard-fail **data quality**:
- **Silver:** checks on an in-memory / staging DataFrame (`raise ValueError`) **before** MERGE / SCD2 write — bad batch does not land in the published silver table.
- **Gold:** one SQL script builds staging → `assert_true` → promote. If an assert fails, later statements (including promote) do not run; the previous published gold stays unchanged.

A failed DQ check fails the task; dependent downstream tasks are **skipped**.

## Job overview

| Job | Cadence | Tasks (order) | Purpose |
|-----|---------|---------------|---------|
| OpenSky daily | Daily (cron; e.g. aligned with 10:00 UTC ingest) | 1) ingest → 2) silver → 3) gold (staging + assert + promote) | Fresh aircraft snapshots into Delta silver + daily country activity gold |
| REST Countries weekly | Weekly (cron; e.g. Monday) | 1) ingest REST Countries → 2) transform silver `dim_country` (SCD2) | Refresh country dimension with history |

Two jobs (not one) because the sources have different refresh frequencies.

Exact clock times are set in the Job **Schedule** (Quartz/cron), not only as a vague “daily / weekly” label — e.g. `0 0 10 * * ?` (daily 10:00) or `0 0 10 ? * MON` (Monday 10:00). Replace with the cron shown in your Job UI if different.

## Job: OpenSky daily

```text
[Task 1: ingest_opensky]
        │
        ▼  (runs only if Task 1 succeeds)
[Task 2: transform_opensky_states]
        │
        ▼  (runs only if Task 2 succeeds)
[Task 3: create_opensky_activity_by_country_day]
        (staging → assert → promote in one SQL file)
```

| | |
|--|--|
| **Task 1** | Notebook / script that runs OpenSky ingest into bronze (+ metadata) on the Volume (or ADLS in Azure). |
| **Task 2** | `src/transformation/transform_opensky_states.py` — Auto Loader + insert-only MERGE (DQ before write). |
| **Task 3** | `sql/gold/create_opensky_activity_by_country_day.sql` — rebuild staging, hard-fail asserts (grain + silver reconcile), then promote to `gold.opensky_activity_by_country_day`. |
| **DQ** | **Task 2:** ingestion metadata, bronze↔metadata coverage, silver staging (required fields, grain `(snapshot_time_unix, icao24)`, snapshot vs `api_time`). Unmapped `origin_country` is WARNING only. **Task 3:** gold staging grain; `observation_count` / `distinct_aircraft` reconcile with silver (full outer). Optional diagnostics: `sql/silver/validate_opensky_states.sql`, `sql/gold/validate_opensky_activity_by_country_day.sql` (published table). |
| **Env / Job params** | `BRONZE_BASE_PATH`, `METADATA_BASE_PATH`, `SILVER_OPENSKY_TABLE`, `SILVER_DIM_COUNTRY_TABLE`, `SILVER_OPENSKY_CHECKPOINT_PATH`, `ORIGIN_COUNTRY_OVERRIDES_PATH`, `TRANSFORMATION_CODE_PATH` (+ OpenSky API secrets for ingest). |

Optional manual sanity after a run: silver / gold `validate_*.sql` (Unknown share, metric sanity, samples).

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
| **Logs** | Open the failed **task** → Output / Logs (traceback from DQ `raise` / `assert_true` or ingest errors). |
| **Retry** | Ingest: 1 retry after 2 minutes (transient API issues). Silver / gold: no retries — DQ / transform failures are not retried. |
| **Downstream skip** | If ingest fails → no silver/gold. If silver fails → no gold. |
| **Published safety** | Silver: DQ before MERGE (insert-only). Gold: promote only after asserts in the same script; assert failure skips promote. Ingest: append-only bronze + metadata status — no published mart to roll back. |

## Storage paths (Volume vs ADLS)

Same code; different prefixes:

| | Databricks Volume (example) | Azure + ADLS Gen2 (target smoke) |
|--|-----------------------------|-------------------------------|
| Bronze | `/Volumes/.../bronze/` | `abfss://bronze@....dfs.core.windows.net/` |
| Metadata | `/Volumes/.../metadata/` | `abfss://metadata@....dfs.core.windows.net/` |
| Checkpoint (OpenSky) | `/Volumes/.../checkpoints/opensky_states` | durable path outside bronze (e.g. on metadata container) |

Databricks does **not** load `.env` automatically — set the same variables on the Job or cluster.

## Out of scope (for now)

- Dropping gold staging after successful promote (kept for debug; next create replaces it).
- Open-Meteo silver / job (ingest exists; silver later).
- Gold does not re-run on the REST Countries weekly job — dim attributes appear on the next OpenSky daily gold refresh via current `dim_country`.
