"""The Google Cloud Storage data Loader."""

from pathlib import Path

import pyarrow as pa

from load.loader import Loader
from utils.parquet import table_to_parquet_buffer
from utils.timestamp import generate_utc_timestamp


class GoogleCloudStorageLoader(Loader):
    """Loads data into a Google Cloud Storage bucket."""

    def load(
        self,
        data: pa.Table | Path | str,
        bucket_name: str,
        blob_name: str,
        timestamp: bool = True,
    ) -> None:
        """Loads data into the specified destination."""
        if timestamp:
            blob_name = f"{blob_name}_{generate_utc_timestamp()}.parquet"
        else:
            blob_name = f"{blob_name}.parquet"

        client = self.client
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        parquet_buffer = table_to_parquet_buffer(data)
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        print(f"Uploaded Parquet file to gs://{bucket_name}/{blob_name}")
