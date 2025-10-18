"""Utilities for handling timestamps."""

from datetime import UTC, datetime


def generate_timestamp() -> str:
    """Returns the current UTC timestamp in ISO-like format."""
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
    return timestamp
