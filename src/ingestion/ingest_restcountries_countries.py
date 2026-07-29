"""Ingest REST Countries v5 reference data into ADLS bronze + metadata.
Requires REST_COUNTRIES_API_KEY in .env
"""

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
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

DEFAULT_RESTCOUNTRIES_URL = "https://api.restcountries.com/countries/v5"

RESTCOUNTRIES_FIELDS = (
    "names.common,names.official,"
    "codes.alpha_2,codes.alpha_3,"
    "capitals,region,subregion,coordinates,population,area"
)
PAGE_LIMIT = 100  # free-plan max


def fetch_countries(api_key: str, api_url: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {api_key}"}
    countries: list[dict] = []
    offset = 0

    while True:
        response = requests.get(
            api_url,
            headers=headers,
            params={
                "limit": PAGE_LIMIT,
                "offset": offset,
                "response_fields": RESTCOUNTRIES_FIELDS,
            },
            timeout=60,
        )
        response.raise_for_status()
        body = response.json()

        if not isinstance(body, dict) or "data" not in body:
            raise ValueError(
                f"Unexpected REST Countries v5 response shape: {str(body)[:300]}"
            )

        data = body["data"] or {}
        if data.get("_demo"):
            raise ValueError(
                "REST Countries returned the demo payload. "
                "Set a real REST_COUNTRIES_API_KEY from https://restcountries.com/sign-up "
                "(demo key does not return the full country list)."
            )

        page = data.get("objects") or []
        if not isinstance(page, list):
            raise ValueError("Unexpected REST Countries v5 response: data.objects missing")

        countries.extend(page)
        meta = data.get("meta") or {}
        total = meta.get("total")
        more = meta.get("more")

        logger.info(
            "Fetched page offset=%s count=%s total_so_far=%s",
            offset,
            len(page),
            len(countries),
        )

        if not page:
            break
        if more is False:
            break
        if total is not None and len(countries) >= int(total):
            break

        offset += PAGE_LIMIT

    return countries


def build_blob_paths(load_datetime: datetime, timestamp_label: str) -> tuple[str, str]:
    date_path = load_datetime.strftime("year=%Y/month=%m/day=%d")

    data_path = (
        f"source=restcountries/entity=countries/{date_path}/"
        f"restcountries_countries_{timestamp_label}.json"
    )
    metadata_path = (
        f"pipeline_runs/source=restcountries/entity=countries/{date_path}/"
        f"restcountries_countries_{timestamp_label}_metadata.json"
    )
    return data_path, metadata_path


def main() -> None:
    run_id = str(uuid.uuid4())
    load_datetime = datetime.now(timezone.utc)
    timestamp_label = load_datetime.strftime("%Y%m%dT%H%M%SZ")

    bronze_container = os.getenv("AZURE_CONTAINER_NAME", "bronze")
    metadata_container = os.getenv("AZURE_METADATA_CONTAINER_NAME", "metadata")
    api_key = os.getenv("REST_COUNTRIES_API_KEY") or os.getenv("RESTCOUNTRIES_API_KEY")
    api_url = os.getenv("REST_COUNTRIES_URL", DEFAULT_RESTCOUNTRIES_URL)

    if not api_key:
        raise ValueError(
            "Missing REST_COUNTRIES_API_KEY in .env. "
            "Create a free key at https://restcountries.com/sign-up"
        )

    logger.info("Starting REST Countries v5 ingestion. run_id=%s", run_id)

    countries = fetch_countries(api_key, api_url)
    records_loaded = len(countries)

    payload = {
        "fetched_at_utc": load_datetime.isoformat(),
        "api_url": api_url,
        "api_version": "v5",
        "response_fields": RESTCOUNTRIES_FIELDS,
        "countries": countries,
    }

    data_path, metadata_path = build_blob_paths(load_datetime, timestamp_label)

    if records_loaded == 0:
        status = "warning"
        destination_path = None
        logger.warning("No countries returned. Skipping bronze upload. run_id=%s", run_id)
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
        "source": "restcountries",
        "entity": "countries",
        "load_datetime_utc": load_datetime.isoformat(),
        "records_loaded": records_loaded,
        "destination_path": destination_path,
        "api_version": "v5",
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
