"""Domain transformations."""

from datetime import UTC, datetime

import pyarrow as pa


def add_created_at_column(table: pa.Table, column_name: str) -> pa.Table:
    """Adds a "created_at" column to an Arrow table."""
    timestamp_array = pa.repeat(datetime.now(UTC), len(table))
    table_with_timestamp = table.append_column(column_name, timestamp_array)
    return table_with_timestamp
