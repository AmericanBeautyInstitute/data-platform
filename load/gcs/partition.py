"""Data partitioner for Google Cloud Storage."""

from datetime import date


def build_gcs_blob_path(source: str, partition_date: date, run_id: str) -> str:
    """Builds a deterministic GCS blob path for a partitioned parquet file."""
    date = partition_date.strftime("%Y-%m-%d")
    filename = f"{source}-{run_id}.parquet"
    blob_path = f"{source}/date={date}/{filename}"
    return blob_path
