"""Tests for Parquet writer utilities."""

import io

import pyarrow as pa
import pyarrow.parquet as pq

from utils.parquet import write_parquet, write_partitioned_parquet


def test_write_parquet_single_table():
    """Test writing a single PyArrow table to Parquet buffer."""
    table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
    buffer = write_parquet(table)

    assert isinstance(buffer, io.BytesIO)
    buffer.seek(0)
    read_table = pq.read_table(buffer)
    assert read_table.equals(table)


def test_write_partitioned_parquet_multiple_tables():
    """Test writing partitioned tables to separate Parquet buffers."""
    tables = {
        "part1": pa.table({"id": [1, 2], "value": [10, 20]}),
        "part2": pa.table({"id": [3, 4], "value": [30, 40]}),
    }

    buffers = write_partitioned_parquet(tables)

    assert isinstance(buffers, dict)
    assert set(buffers.keys()) == {"part1", "part2"}

    for key, buf in buffers.items():
        assert isinstance(buf, io.BytesIO)
        buf.seek(0)
        read_table = pq.read_table(buf)
        assert read_table.equals(tables[key])


def test_write_partitioned_parquet_empty_dict():
    """Test partitioned parquet with empty input."""
    buffers = write_partitioned_parquet({})
    assert buffers == {}
