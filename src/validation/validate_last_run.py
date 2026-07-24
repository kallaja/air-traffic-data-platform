import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.storage import download_json_from_container, find_latest_blob_path


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
# Quiet Azure SDK HTTP noise (Request URL / headers / response dump).
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logger = logging.getLogger(__name__)

METADATA_PREFIX = "pipeline_runs/source=opensky/entity=states/"


def validate_metadata(metadata: dict) -> None:
    run_id = metadata.get("run_id", "unknown")
    status = metadata.get("status")
    records_loaded = metadata.get("records_loaded")

    logger.info(
        "Validating run_id=%s status=%s records_loaded=%s",
        run_id,
        status,
        records_loaded,
    )

    if status == "failed":
        raise SystemExit(f"Validation failed: run {run_id} has status=failed")

    if status == "warning" or records_loaded == 0:
        raise SystemExit(
            f"Validation failed: run {run_id} loaded zero records (status={status})"
        )

    if status != "success":
        raise SystemExit(f"Validation failed: unexpected status={status}")

    if not metadata.get("destination_path"):
        raise SystemExit(
            f"Validation failed: run {run_id} has no destination_path in bronze"
        )

    logger.info("Validation passed for run_id=%s", run_id)


def main() -> None:
    metadata_container = os.getenv("AZURE_METADATA_CONTAINER_NAME", "metadata")

    latest_metadata_path = find_latest_blob_path(metadata_container, METADATA_PREFIX)
    if not latest_metadata_path:
        raise SystemExit("Validation failed: no metadata files found")

    logger.info("Checking latest metadata file: %s", latest_metadata_path)

    metadata = download_json_from_container(metadata_container, latest_metadata_path)
    if not isinstance(metadata, dict):
        raise SystemExit("Validation failed: metadata file is not a JSON object")

    validate_metadata(metadata)


if __name__ == "__main__":
    main()
