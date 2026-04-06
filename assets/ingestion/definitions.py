"""Ingestion layer Dagster definitions."""

from dagster import Definitions

from assets.ingestion.jobs import ingestion_job
from assets.ingestion.resources import bigquery_resource, gcs_resource
from assets.ingestion.schedules import daily_schedule

ingestion_defs = Definitions(
    assets=[],
    jobs=[ingestion_job],
    schedules=[daily_schedule],
    resources={
        "bigquery": bigquery_resource,
        "gcs": gcs_resource,
    },
)
