"""Utilities for working with Parquet in-memory."""

import io

import pyarrow as pa
import pyarrow.parquet as pq


def table_to_parquet_buffer(table: pa.Table) -> io.BytesIO:
    """Converts a PyArrow table to an in-memory Parquet buffer."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer
