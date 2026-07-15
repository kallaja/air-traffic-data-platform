import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from opensky_api import OpenSkyApi

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.storage import upload_json_to_container


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Continental Europe (min_lat, max_lat, min_lon, max_lon)
EUROPE_BBOX = (35.0, 72.0, -12.0, 42.0)


def state_vector_to_dict(state) -> dict:
    return {key: getattr(state, key) for key in state.keys}


def fetch_opensky_states(bbox: tuple[float, float, float, float]) -> dict:
    client_id = os.getenv("OPEN_SKY_CLIENT_ID")
    client_secret = os.getenv("OPEN_SKY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("Missing OPEN_SKY_CLIENT_ID or OPEN_SKY_CLIENT_SECRET in .env")

    api = OpenSkyApi(client_id=client_id, client_secret=client_secret)
    response = api.get_states(bbox=bbox)

    states = response.states or []

    return {
        "time": response.time,
        "states": [state_vector_to_dict(state) for state in states],
    }


def build_blob_paths(load_datetime: datetime, timestamp_label: str) -> tuple[str, str]:
    date_path = load_datetime.strftime("year=%Y/month=%m/day=%d")

    data_path = (
        f"source=opensky/entity=states/{date_path}/"
        f"opensky_states_{timestamp_label}.json"
    )
    metadata_path = (
        f"pipeline_runs/source=opensky/entity=states/{date_path}/"
        f"opensky_states_{timestamp_label}_metadata.json"
    )

    return data_path, metadata_path


def main() -> None:
    run_id = str(uuid.uuid4())
    load_datetime = datetime.now(timezone.utc)
    timestamp_label = load_datetime.strftime("%Y%m%dT%H%M%SZ")

    bronze_container = os.getenv("AZURE_CONTAINER_NAME", "bronze")
    metadata_container = os.getenv("AZURE_METADATA_CONTAINER_NAME", "metadata")

    logger.info("Starting OpenSky states ingestion. run_id=%s bbox=%s", run_id, EUROPE_BBOX)

    payload = fetch_opensky_states(bbox=EUROPE_BBOX)
    records_loaded = len(payload["states"])

    data_path, metadata_path = build_blob_paths(load_datetime, timestamp_label)

    if records_loaded == 0:
        status = "warning"
        destination_path = None
        logger.warning(
            "No records loaded. Skipping bronze upload. run_id=%s",
            run_id,
        )
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
        "source": "opensky",
        "entity": "states",
        "load_datetime_utc": load_datetime.isoformat(),
        "records_loaded": records_loaded,
        "destination_path": destination_path,
        "bbox": list(EUROPE_BBOX),
        "api_time_unix": payload["time"],
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
