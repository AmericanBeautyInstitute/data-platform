"""GCS parquet loader."""

import io

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from load.config import GCSConfig
from load.gcs.partition import build_gcs_blob_path


def load(
    table: pa.Table,
    config: GCSConfig,
    client: storage.Client,
) -> str:
    """Loads a PyArrow table to GCS as a parquet file.

    Returns the full GCS URI of the uploaded blob.
    """
    blob_path = build_gcs_blob_path(
        config.source,
        config.partition_date,
        config.run_id,
    )
    data = _serialize(table)
    gcs_uri = _upload(client, config.bucket, blob_path, data)
    return gcs_uri


def _serialize(table: pa.Table) -> bytes:
    """Serializes a PyArrow table to parquet bytes."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    parquet_bytes = buffer.getvalue()
    return parquet_bytes


def _upload(
    client: storage.Client,
    bucket_name: str,
    blob_path: str,
    data: bytes,
) -> str:
    """Uploads bytes to GCS and returns the GCS URI."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(data, content_type="application/octet-stream")
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    return gcs_uri
