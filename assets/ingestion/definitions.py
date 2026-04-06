"""Ingestion layer Dagster definitions."""

from dagster import Definitions

from assets.ingestion.google_ads import google_ads_raw
from assets.ingestion.google_analytics import google_analytics_raw
from assets.ingestion.google_sheets import google_sheets_assets
from assets.ingestion.jobs import ingestion_job
from assets.ingestion.resources import (
    bigquery_resource,
    gcs_resource,
    google_ads_resource,
    google_analytics_resource,
    google_sheets_resource,
)
from assets.ingestion.schedules import daily_schedule

ingestion_defs = Definitions(
    assets=[google_ads_raw, google_analytics_raw, *google_sheets_assets],
    jobs=[ingestion_job],
    schedules=[daily_schedule],
    resources={
        "bigquery": bigquery_resource,
        "gcs": gcs_resource,
        "google_sheets": google_sheets_resource,
        "google_analytics": google_analytics_resource,
        "google_ads": google_ads_resource,
    },
)
