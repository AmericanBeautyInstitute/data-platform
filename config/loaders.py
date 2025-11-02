"""Pydantic base models for loader configurations."""

from typing import Any, Literal

from pydantic import BaseModel


class BigQueryLoadConfig(BaseModel):
    """Configuration for BigQuery load operations."""

    dataset_id: str
    table_id: str
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"] = (
        "WRITE_TRUNCATE"
    )
    source_format: Literal["PARQUET", "CSV", "JSON", "AVRO"] = "PARQUET"
    create_table_if_needed: bool = True
    partition_field: str | None = None
    cluster_fields: list[str] | None = None


class BigQueryTableConfig(BaseModel):
    """Configuration for BigQuery table creation."""

    dataset_id: str
    table_id: str
    schema: Any
    partition_field: str | None = None
    cluster_fields: list[str] | None = None
    description: str | None = None

    model_config = {
        "arbitrary_types_allowed": True,
    }

    @property
    def table_reference(self) -> str:
        """Returns dataset.table reference."""
        return f"{self.dataset_id}.{self.table_id}"
