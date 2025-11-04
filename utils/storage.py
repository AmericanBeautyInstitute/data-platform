"""Cloud object storage utilities."""

from datetime import datetime


def build_object_path(source: str, partition_key: str, date: datetime) -> str:
    """Builds a canonical object path for cloud storage.

    Format: {source}/{partition_key}/{source}-{date}.parquet
    Example: students/date=2024-11-03/students-2024-11-03.parquet
    """
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{source}-{date_str}.parquet"
    object_path = f"{source}/{partition_key}/{filename}"
    return object_path
