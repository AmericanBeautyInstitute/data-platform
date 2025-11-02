"""Pydantic base models for loader configurations."""

from pydantic import BaseModel


class BigQueryLoadConfig(BaseModel):
    """Configuration for the BigQuery loader."""

    dataset_id: str
    table_id: str
    write_disposition: str = "WRITE_TRUNCATE"
    source_format: str = "PARQUET"
    create_table_if_needed: bool = True
    partition_field: str | None = None
    cluster_fields: list[str] | None = None

    class Config:
        """Configuration settings."""

        arbitrary_types_allowed = True


class BigQueryTableConfig(BaseModel):
    """Configuration for BigQuery table creation."""

    dataset_id: str
    table_id: str
    schema: list | None = None
    partition_field: str | None = None
    cluster_fields: list[str] | None = None
    description: str | None = None

    class Config:
        """Configuration settings."""

        # Allow BigQuery/Arrow types
        arbitrary_types_allowed = True

    @property
    def table_reference(self) -> str:
        """Returns dataset.table reference."""
        return f"{self.dataset_id}.{self.table_id}"
