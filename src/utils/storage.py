import json
import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def get_blob_service_client() -> BlobServiceClient:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    if not account_name:
        raise ValueError(
            "Missing Azure credentials. Set AZURE_STORAGE_CONNECTION_STRING "
            "or AZURE_STORAGE_ACCOUNT_NAME for DefaultAzureCredential."
        )

    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url, credential=DefaultAzureCredential())


def upload_json_to_container(
    container_name: str,
    blob_path: str,
    data: dict | list,
) -> None:
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_path,
    )

    payload = json.dumps(data, indent=2, ensure_ascii=False)
    blob_client.upload_blob(payload, overwrite=True)


def upload_json_to_container(
    container_name: str,
    blob_path: str,
    data: dict | list,
) -> None:
    storage_backend = os.getenv("STORAGE_BACKEND", "azure")

    payload = json.dumps(data, indent=2, ensure_ascii=False)

    if storage_backend == "local":
        local_root = Path(
            os.getenv("LOCAL_STORAGE_ROOT", "data")
        )

        file_path = local_root / container_name / blob_path
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_path.write_text(
            payload,
            encoding="utf-8",
        )
        return

    if storage_backend == "azure":
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_path,
        )

        blob_client.upload_blob(payload, overwrite=True)
        return

    raise ValueError(
        f"Unsupported STORAGE_BACKEND: {storage_backend}"
    )


def download_json_from_container(container_name: str, blob_path: str) -> dict | list:
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_path,
    )

    payload = blob_client.download_blob().readall()
    return json.loads(payload)


def find_latest_blob_path(container_name: str, prefix: str) -> str | None:
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)

    latest_blob_name = None
    latest_modified = None

    for blob in container_client.list_blobs(name_starts_with=prefix):
        # ADLS Gen2 lists empty directory markers (size=0) alongside real files.
        # Only treat actual JSON blobs as candidates.
        if not blob.name.endswith(".json") or (blob.size or 0) == 0:
            continue

        if latest_modified is None or blob.last_modified > latest_modified:
            latest_modified = blob.last_modified
            latest_blob_name = blob.name

    return latest_blob_name
