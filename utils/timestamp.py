"""Utilities for handling timestamps."""

from datetime import UTC, datetime


def utc_timestamp() -> str:
    """Returns the current UTC timestamp in ISO-like format."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
