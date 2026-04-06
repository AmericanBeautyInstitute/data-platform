"""Tests for transform metadata utilities."""

from datetime import UTC, datetime

import pyarrow as pa
import pytest

from transform.metadata import add_loaded_at

EXPECTED_COLUMN_COUNT_AFTER = 3
EXPECTED_ORIGINAL_COLUMN_COUNT = 2


@pytest.fixture
def sample_table():
    """Sample PyArrow table without loaded_at column."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        }
    )


def test_add_loaded_at_returns_pyarrow_table(sample_table):
    """Returns a pa.Table instance."""
    result = add_loaded_at(sample_table)
    assert isinstance(result, pa.Table)


def test_add_loaded_at_preserves_row_count(sample_table):
    """Row count is unchanged after adding loaded_at."""
    result = add_loaded_at(sample_table)
    assert result.num_rows == sample_table.num_rows


def test_add_loaded_at_appends_column(sample_table):
    """loaded_at column is added to the table."""
    result = add_loaded_at(sample_table)
    assert result.num_columns == EXPECTED_COLUMN_COUNT_AFTER
    assert "loaded_at" in result.column_names


def test_add_loaded_at_preserves_original_columns(sample_table):
    """Original columns are preserved unchanged."""
    result = add_loaded_at(sample_table)
    assert "id" in result.column_names
    assert "name" in result.column_names
    assert result.column("id").to_pylist() == [1, 2, 3]


def test_add_loaded_at_column_is_timestamp(sample_table):
    """loaded_at column has UTC timestamp type."""
    result = add_loaded_at(sample_table)
    loaded_at_type = result.schema.field("loaded_at").type
    assert pa.types.is_timestamp(loaded_at_type)
    assert loaded_at_type.tz == "UTC"


def test_add_loaded_at_values_are_recent(sample_table):
    """loaded_at values are close to current UTC time."""
    before = datetime.now(tz=UTC)
    result = add_loaded_at(sample_table)
    after = datetime.now(tz=UTC)

    loaded_at_values = result.column("loaded_at").to_pylist()
    for val in loaded_at_values:
        assert before <= val <= after


def test_add_loaded_at_all_values_equal(sample_table):
    """All rows share the same loaded_at timestamp."""
    result = add_loaded_at(sample_table)
    loaded_at_values = result.column("loaded_at").to_pylist()
    assert len(set(loaded_at_values)) == 1


def test_add_loaded_at_empty_table():
    """Empty table returns empty table with loaded_at column."""
    empty = pa.table({"id": pa.array([], type=pa.int64())})
    result = add_loaded_at(empty)
    assert result.num_rows == 0
    assert "loaded_at" in result.column_names
