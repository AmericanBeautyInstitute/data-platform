"""Loader tasks."""

import pyarrow as pa
from prefect import get_run_logger, task

from helpers.authenticator import google_cloud_storage_authenticator
from load.google_cloud_storage_loader import GoogleCloudStorageLoader


@task
def load_to_google_cloud_storage(
    data: pa.Table,
    bucket_name: str,
    blob_name: str,
    credentials_file_path: str,
) -> None:
    """Prefect task to load extracted data into Google Cloud Storage."""
    logger = get_run_logger()

    gcs_client = google_cloud_storage_authenticator(credentials_file_path)
    loader = GoogleCloudStorageLoader(gcs_client)

    logger.info(f"Uploading table to gs://{bucket_name}/{blob_name}")
    loader.load(data=data, bucket_name=bucket_name, blob_name=blob_name)
    logger.info("Upload complete.")
