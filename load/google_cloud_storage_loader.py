"""The Google Cloud Storage data Loader."""

import io

from load.loader import Loader


class GoogleCloudStorageLoader(Loader):
    """Loads data into a Google Cloud Storage bucket."""

    def load(
        self,
        data: io.BytesIO,
        bucket_name: str,
        blob_name: str,
    ) -> None:
        """Loads data into the specified destination."""
        client = self.client
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_file(data, content_type="application/octet-stream")

        print(f"Uploaded Parquet file to gs://{bucket_name}/{blob_name}")
