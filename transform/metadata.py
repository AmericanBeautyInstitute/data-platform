"""Metadata utilities for SQLMesh incremental models."""

from datetime import UTC, datetime

import pyarrow as pa


def add_loaded_at(table: pa.Table) -> pa.Table:
    """Appends a loaded_at timestamp column to a PyArrow table.

    Used by SQLMesh incremental models to track when a partition
    was last written, enabling reliable incremental processing.
    """
    loaded_at = datetime.now(tz=UTC)
    loaded_at_array = pa.array(
        [loaded_at] * len(table),
        type=pa.timestamp("us", tz="UTC"),
    )
    return table.append_column("loaded_at", loaded_at_array)
