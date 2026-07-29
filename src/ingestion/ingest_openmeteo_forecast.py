"""Ingest Open-Meteo daily forecast for configured European capitals into ADLS bronze."""

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.storage import upload_json_to_container

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logger = logging.getLogger(__name__)

OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_DAILY_VARS = (
    "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
)


def load_locations() -> list[dict]:
    config_path = PROJECT_ROOT / "config" / "sources.yml"
    with config_path.open(encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    locations = (config.get("openmeteo") or {}).get("locations") or []
    if not locations:
        raise ValueError("No openmeteo.locations found in config/sources.yml")
    return locations


def fetch_forecasts(locations: list[dict], forecast_days: int = 7) -> dict:
    latitudes = ",".join(str(loc["latitude"]) for loc in locations)
    longitudes = ",".join(str(loc["longitude"]) for loc in locations)

    response = requests.get(
        OPENMETEO_URL,
        params={
            "latitude": latitudes,
            "longitude": longitudes,
            "daily": DEFAULT_DAILY_VARS,
            "timezone": "UTC",
            "forecast_days": forecast_days,
        },
        timeout=60,
    )
    response.raise_for_status()
    body = response.json()

    # Multi-location calls return a list; single location returns one object.
    forecasts = body if isinstance(body, list) else [body]
    if len(forecasts) != len(locations):
        raise ValueError(
            f"Open-Meteo returned {len(forecasts)} forecasts for {len(locations)} locations"
        )

    return {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "api_url": OPENMETEO_URL,
        "daily_variables": DEFAULT_DAILY_VARS,
        "forecast_days": forecast_days,
        "locations": [
            {
                "location_id": loc["id"],
                "name": loc["name"],
                "country_cca2": loc["country_cca2"],
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
                "forecast": forecast,
            }
            for loc, forecast in zip(locations, forecasts)
        ],
    }


def build_blob_paths(load_datetime: datetime, timestamp_label: str) -> tuple[str, str]:
    date_path = load_datetime.strftime("year=%Y/month=%m/day=%d")

    data_path = (
        f"source=openmeteo/entity=forecast/{date_path}/"
        f"openmeteo_forecast_{timestamp_label}.json"
    )
    metadata_path = (
        f"pipeline_runs/source=openmeteo/entity=forecast/{date_path}/"
        f"openmeteo_forecast_{timestamp_label}_metadata.json"
    )
    return data_path, metadata_path


def main() -> None:
    run_id = str(uuid.uuid4())
    load_datetime = datetime.now(timezone.utc)
    timestamp_label = load_datetime.strftime("%Y%m%dT%H%M%SZ")

    bronze_container = os.getenv("AZURE_CONTAINER_NAME", "bronze")
    metadata_container = os.getenv("AZURE_METADATA_CONTAINER_NAME", "metadata")

    locations = load_locations()
    logger.info(
        "Starting Open-Meteo forecast ingestion. run_id=%s locations=%s",
        run_id,
        len(locations),
    )

    payload = fetch_forecasts(locations)
    records_loaded = len(payload["locations"])

    data_path, metadata_path = build_blob_paths(load_datetime, timestamp_label)

    if records_loaded == 0:
        status = "warning"
        destination_path = None
        logger.warning("No forecasts loaded. Skipping bronze upload. run_id=%s", run_id)
    else:
        status = "success"
        destination_path = data_path
        upload_json_to_container(
            container_name=bronze_container,
            blob_path=data_path,
            data=payload,
        )
        logger.info("Uploaded raw data to %s/%s", bronze_container, data_path)

    metadata = {
        "run_id": run_id,
        "source": "openmeteo",
        "entity": "forecast",
        "load_datetime_utc": load_datetime.isoformat(),
        "records_loaded": records_loaded,
        "destination_path": destination_path,
        "location_ids": [loc["id"] for loc in locations],
        "status": status,
    }

    upload_json_to_container(
        container_name=metadata_container,
        blob_path=metadata_path,
        data=metadata,
    )
    logger.info(
        "Uploaded metadata to %s/%s. records_loaded=%s",
        metadata_container,
        metadata_path,
        records_loaded,
    )


if __name__ == "__main__":
    main()
