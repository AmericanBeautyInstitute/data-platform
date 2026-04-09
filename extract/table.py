"""Shared conversion from Pydantic records to PyArrow tables."""

from collections.abc import Sequence

import pyarrow as pa
from pydantic import BaseModel


def to_table(records: Sequence[BaseModel]) -> pa.Table:
    """Converts a list of Pydantic records into a PyArrow table."""
    if not records:
        return pa.Table.from_pylist([])
    rows = [r.model_dump(mode="json") for r in records]
    table = pa.Table.from_pylist(rows)
    return table
