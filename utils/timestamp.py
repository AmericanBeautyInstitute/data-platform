"""Utilities for handling timestamps."""

from datetime import UTC, datetime


def generate_file_timestamp() -> str:
    """Generates a UTC timestamp in ISO-like format for file names."""
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
    return timestamp


def generate_utc_timestamp() -> datetime:
    """Generates a pure UTC timestamp for "created_at" column."""
    timestamp = datetime.now(UTC)
    return timestamp
