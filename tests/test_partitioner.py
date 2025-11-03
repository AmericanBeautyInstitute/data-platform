"""Tests for the Partitioner class."""

from datetime import datetime

import pyarrow as pa
import pytest

from transform.partitioner import Partitioner


def test_partition_by_date():
    """Test basic partitioning by date column."""
    table = pa.table(
        {
            "id": [1, 2, 3, 4],
            "amount": [100, 200, 150, 300],
            "date": [
                datetime(2024, 11, 1),
                datetime(2024, 11, 1),
                datetime(2024, 11, 2),
                datetime(2024, 11, 2),
            ],
        }
    )

    partitioner = Partitioner(partition_column="date")
    result = partitioner.partition(table)

    expected_partition_count = 2
    expected_partition_1 = "date=2024-11-01"
    expected_partition_2 = "date=2024-11-02"
    expected_rows_per_partition = 2

    assert len(result) == expected_partition_count
    assert expected_partition_1 in result
    assert expected_partition_2 in result
    assert len(result[expected_partition_1]) == expected_rows_per_partition
    assert len(result[expected_partition_2]) == expected_rows_per_partition


def test_partition_by_timestamp():
    """Test partitioning by timestamp column (should convert to date)."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "created_at": pa.array(
                [
                    datetime(2024, 11, 1, 10, 30),
                    datetime(2024, 11, 1, 14, 15),
                    datetime(2024, 11, 2, 9, 0),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )

    partitioner = Partitioner(partition_column="created_at")
    result = partitioner.partition(table)

    expected_partition_count = 2
    expected_partition_1 = "date=2024-11-01"
    expected_partition_2 = "date=2024-11-02"
    expected_rows_partition_1 = 2
    expected_rows_partition_2 = 1

    assert len(result) == expected_partition_count
    assert expected_partition_1 in result
    assert expected_partition_2 in result
    assert len(result[expected_partition_1]) == expected_rows_partition_1
    assert len(result[expected_partition_2]) == expected_rows_partition_2


def test_partition_with_custom_format():
    """Test custom partition format."""
    table = pa.table(
        {"id": [1, 2], "date": [datetime(2024, 11, 1), datetime(2024, 12, 1)]}
    )

    partitioner = Partitioner(partition_column="date", partition_format="%Y-%m-%d")
    result = partitioner.partition(table)

    expected_partition_1 = "2024-11-01"
    expected_partition_2 = "2024-12-01"
    expected_partition_count = 2

    assert len(result) == expected_partition_count
    assert expected_partition_1 in result
    assert expected_partition_2 in result


def test_partition_single_date():
    """Test table with only one date value."""
    table = pa.table({"id": [1, 2, 3], "date": [datetime(2024, 11, 1)] * 3})

    partitioner = Partitioner(partition_column="date")
    result = partitioner.partition(table)

    expected_partition_count = 1
    expected_partition = "date=2024-11-01"
    expected_rows = 3

    assert len(result) == expected_partition_count
    assert expected_partition in result
    assert len(result[expected_partition]) == expected_rows


def test_partition_skips_nulls():
    """Test that null dates are skipped."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "date": pa.array([datetime(2024, 11, 1), None, datetime(2024, 11, 2)]),
        }
    )

    partitioner = Partitioner(partition_column="date")
    result = partitioner.partition(table)

    expected_partition_count = 2
    expected_partition_1 = "date=2024-11-01"
    expected_partition_2 = "date=2024-11-02"
    expected_rows_partition_1 = 1
    expected_rows_partition_2 = 1

    assert len(result) == expected_partition_count
    assert expected_partition_1 in result
    assert expected_partition_2 in result
    assert len(result[expected_partition_1]) == expected_rows_partition_1
    assert len(result[expected_partition_2]) == expected_rows_partition_2


def test_partition_missing_column_raises_error():
    """Test error when partition column doesn't exist."""
    table = pa.table({"id": [1, 2, 3]})
    partitioner = Partitioner(partition_column="missing_column")

    expected_error_message = "not found in table"

    with pytest.raises(ValueError, match=expected_error_message):
        partitioner.partition(table)


def test_partition_non_temporal_column_raises_error():
    """Test error when partition column is not date/timestamp."""
    table = pa.table({"id": [1, 2, 3], "category": ["A", "B", "C"]})
    partitioner = Partitioner(partition_column="category")

    expected_error_message = "must be date or timestamp"

    with pytest.raises(ValueError, match=expected_error_message):
        partitioner.partition(table)


def test_partition_empty_table():
    """Test partitioning an empty table."""
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "date": pa.array([], type=pa.date32()),
        }
    )

    partitioner = Partitioner(partition_column="date")
    result = partitioner.partition(table)

    expected_partition_count = 0
    assert len(result) == expected_partition_count
