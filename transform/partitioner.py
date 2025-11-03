"""Table partition transformations."""

from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc


class Partitioner:
    """Partitions PyArrow tables by date column."""

    def __init__(self, partition_column: str, partition_format: str = "date=%Y-%m-%d"):
        """Initializes the Partitioner."""
        self.partition_column = partition_column
        self.partition_format = partition_format

    def partition(self, table: pa.Table) -> dict[str, pa.Table]:
        """Partitions a PyArrow table by date."""
        partition_col = self._get_validated_partition_column(table)
        dates = self._normalize_to_dates(partition_col)
        unique_dates = self._get_unique_dates(dates)

        partitioned_table = self._build_partitions(table, dates, unique_dates)
        return partitioned_table

    def _get_validated_partition_column(self, table: pa.Table) -> pa.ChunkedArray:
        """Gets and validates the partition column exists and is temporal."""
        if self.partition_column not in table.column_names:
            raise ValueError(
                f"Partition column '{self.partition_column}' not found in table. "
                f"Available columns: {table.column_names}"
            )

        partition_col = table.column(self.partition_column)

        if not pa.types.is_temporal(partition_col.type):
            raise ValueError(
                f"'{self.partition_column}' must be date or timestamp, "
                f"got {partition_col.type}"
            )

        return partition_col

    def _normalize_to_dates(self, partition_col: pa.ChunkedArray) -> pa.ChunkedArray:
        """Normalizes timestamp columns to date32 for grouping."""
        if pa.types.is_timestamp(partition_col.type):
            return pc.cast(partition_col, pa.date32())
        return partition_col

    def _get_unique_dates(self, dates: pa.ChunkedArray) -> list:
        """Extracts unique date values, excluding nulls."""
        unique_dates = pc.unique(dates).to_pylist()
        return [d for d in unique_dates if d is not None]

    def _build_partitions(
        self, table: pa.Table, dates: pa.ChunkedArray, unique_dates: list
    ) -> dict[str, pa.Table]:
        """Builds dictionary of partition key to filtered table."""
        partitions = {}
        for date_value in unique_dates:
            partition_key = self._format_partition_key(date_value)
            partition_table = self._filter_table_by_date(table, dates, date_value)
            partitions[partition_key] = partition_table

        return partitions

    def _filter_table_by_date(
        self, table: pa.Table, dates: pa.ChunkedArray, date_value
    ) -> pa.Table:
        """Filters table to rows matching the given date."""
        mask = pc.equal(dates, date_value)
        return table.filter(mask)

    def _format_partition_key(self, date_value) -> str:
        """Formats a date value into a partition key string."""
        if hasattr(date_value, "as_py"):
            date_value = date_value.as_py()

        return datetime.strftime(date_value, self.partition_format)
