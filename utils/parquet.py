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


def write_parquet(table: pa.Table) -> io.BytesIO:
    """Writes a single PyArrow table to Parquet buffer."""
    buffer = table_to_parquet_buffer(table)
    return buffer


def write_partitioned_parquet(
    partitioned_data: dict[str, pa.Table],
) -> dict[str, io.BytesIO]:
    """Writes partitioned tables to separate Parquet buffers."""
    buffers = {
        partition_key: table_to_parquet_buffer(table)
        for partition_key, table in partitioned_data.items()
    }
    return buffers
