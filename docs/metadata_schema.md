# Metadata Schema — Pipeline Runs

Metadata files are stored in the `metadata` container under:

```
pipeline_runs/source=opensky/entity=states/year=YYYY/month=MM/day=DD/
  opensky_states_YYYYMMDDTHHMMSSZ_metadata.json
```

Each ingestion run produces one metadata file. Bronze data is stored separately in the `bronze` container when the run loads at least one record.

## Fields


| Field               | Type              | Required | Description                                                                               |
| ------------------- | ----------------- | -------- | ----------------------------------------------------------------------------------------- |
| `run_id`            | string (UUID)     | yes      | Unique identifier for the pipeline run                                                    |
| `source`            | string            | yes      | Data source name. Current value: `opensky`                                                |
| `entity`            | string            | yes      | Dataset within the source. Current value: `states`                                        |
| `load_datetime_utc` | string (ISO 8601) | yes      | UTC timestamp when ingestion started                                                      |
| `records_loaded`    | integer           | yes      | Number of aircraft state vectors returned by OpenSky                                      |
| `destination_path`  | string | null     | yes      | Path to the bronze file in the `bronze` container. `null` when no bronze file was written |
| `bbox`              | array[4 float]    | yes      | Geographic bounding box used for the API request: `[min_lat, max_lat, min_lon, max_lon]`  |
| `api_time_unix`     | integer           | yes      | Unix timestamp returned by OpenSky in the API response                                    |
| `status`            | string            | yes      | Run outcome. Allowed values: `success`, `warning`, `failed`                               |


## Status Values


| Status    | Meaning                                                                                 |
| --------- | --------------------------------------------------------------------------------------- |
| `success` | Data fetched successfully and `records_loaded > 0`. Bronze file uploaded                |
| `warning` | API call completed but `records_loaded = 0`. Bronze file was not uploaded           |
| `failed`  | Ingestion failed before a successful metadata write, or an unrecoverable error occurred |


## Example — successful run

```json
{
  "run_id": "ea477b23-05eb-4858-94c9-c80935c58ad9",
  "source": "opensky",
  "entity": "states",
  "load_datetime_utc": "2026-07-12T22:21:08.409000+00:00",
  "records_loaded": 1605,
  "destination_path": "source=opensky/entity=states/year=2026/month=07/day=12/opensky_states_20260712T222108Z.json",
  "bbox": [35.0, 72.0, -12.0, 42.0],
  "api_time_unix": 1783894832,
  "status": "success"
}
```

## Validation Rules

The script `src/validation/validate_last_run.py` checks the latest metadata file and fails when:

- `status` is `failed`
- `status` is `warning`
- `records_loaded` is `0`
- `destination_path` is missing for a successful run

## Notes

- `bbox` describes one rectangular region requested from OpenSky, not individual countries
- `destination_path` is relative to the `bronze` container
- Metadata is always written, even when bronze upload is skipped

