"""The Google Cloud Storage data Loader."""

from pathlib import Path

import pyarrow as pa
from google.cloud import storage

from helpers.google_cloud_platform import authenticate_google_cloud_storage
from load.loader import Loader
from utils.parquet import table_to_parquet_buffer


class GoogleCloudStorageLoader(Loader):
    """Loads data into a Google Cloud Storage bucket."""

    def load(
        self,
        data: pa.Table | Path | str,
        bucket_name: str,
        blob_name: str,
    ) -> None:
        """Loads data into the specified destination."""
        client = self.client
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        parquet_buffer = table_to_parquet_buffer(data)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        print(f"Uploaded Parquet file to gs://{bucket_name}/{blob_name}")

    def _authenticate(self) -> storage.Client:
        """Authenticates and returns either the client or service object."""
        google_cloud_storage_client = authenticate_google_cloud_storage(
            self.credentials_file_path
        )
        return google_cloud_storage_client
