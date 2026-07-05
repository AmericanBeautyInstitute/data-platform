"""Shared conversion from Pydantic records to PyArrow tables."""

from collections.abc import Sequence

import pyarrow as pa
from pydantic import BaseModel

# TODO: Relies on PyArrow schema inference, which does not scale
# robustly across many APIs and daily partitions:
#   1. Empty records yield a schemaless table (zero columns, not columns with
#      zero rows), so no-data partitions can break downstream loads.
#   2. An all-null column in a batch infers `null` type, mismatching the target.
#   3. Inferred types can drift between partitions (int64 one day, double the
#      next) for the same table.
#
# Fix: derive an explicit pa.Schema from the Record model (types are already
# declared there) and pass it to from_pylist(..., schema=schema).


def to_table(records: Sequence[BaseModel]) -> pa.Table:
    """Converts a list of Pydantic records into a PyArrow table."""
    if not records:
        return pa.Table.from_pylist([])
    rows = [r.model_dump(mode="json") for r in records]
    table = pa.Table.from_pylist(rows)
    return table
