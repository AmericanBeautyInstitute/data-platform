"""PyDantic models for configurations."""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class GCSConfig(BaseModel):
    """Immutable configuration for a GCS upload."""

    model_config = ConfigDict(frozen=True)

    bucket: str
    source: str
    partition_date: date
    run_id: str


class BigQueryConfig(BaseModel):
    """Immutable configuration for a BigQuery partition load."""

    model_config = ConfigDict(frozen=True)

    project: str
    dataset: str
    table: str
    partition_date: date
    partition_field: str = "date"
    cluster_fields: list[str] = Field(default_factory=list)
