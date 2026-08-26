"""Ingestion layer Dagster definitions."""

from dagster import Definitions
from dagster_dlt import DagsterDltResource

from orchestration.defs.ingestion.assets import INGESTION_ASSETS
from orchestration.defs.ingestion.jobs import ingestion_job
from orchestration.defs.ingestion.resources import (
    bigquery_resource,
    facebook_ads_resource,
    gcs_resource,
    google_ads_resource,
    google_analytics_resource,
    google_sheets_resource,
    ingestion_env,
    paypal_resource,
    stripe_resource,
)
from orchestration.defs.ingestion.schedules import daily_schedule

ingestion_defs = Definitions(
    assets=INGESTION_ASSETS,
    jobs=[ingestion_job],
    schedules=[daily_schedule],
    resources={
        "bigquery": bigquery_resource,
        "dlt": DagsterDltResource(),
        "ingestion_env": ingestion_env,
        "facebook_ads": facebook_ads_resource,
        "gcs": gcs_resource,
        "google_ads": google_ads_resource,
        "google_analytics": google_analytics_resource,
        "google_sheets": google_sheets_resource,
        "paypal": paypal_resource,
        "stripe": stripe_resource,
    },
)
