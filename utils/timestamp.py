"""Utilities for handling timestamps."""

from datetime import UTC, datetime


def generate_timestamp() -> datetime:
    """Generates a pure UTC timestamp for "created_at" column."""
    timestamp = datetime.now(UTC)
    return timestamp
