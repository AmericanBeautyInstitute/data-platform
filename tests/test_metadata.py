"""Tests for domain transformations."""

from datetime import datetime

import pyarrow as pa

from transform.metadata import add_created_at_column


def test_add_created_at_column():
    """Test adding a 'created_at' column with UTC timestamps."""
    column_name = "created_at"
    table = pa.table({"id": [1, 2, 3]})
    result = add_created_at_column(table, column_name)

    assert column_name in result.column_names

    assert result.num_rows == len(table)

    created_at_column = result[column_name].to_pylist()
    for ts in created_at_column:
        assert isinstance(ts, datetime)
        assert ts.utcoffset().total_seconds() == 0
