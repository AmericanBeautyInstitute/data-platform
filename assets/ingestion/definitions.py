"""Ingestion layer Dagster definitions."""

from dagster import Definitions

from assets.ingestion.facebook_ads import facebook_ads_raw
from assets.ingestion.google_ads import google_ads_raw
from assets.ingestion.google_analytics import google_analytics_raw
from assets.ingestion.google_sheets import google_sheets_assets
from assets.ingestion.jobs import ingestion_job
from assets.ingestion.paypal import paypal_transactions_raw
from assets.ingestion.resources import (
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
from assets.ingestion.schedules import daily_schedule
from assets.ingestion.stripe import stripe_charges_raw

ingestion_defs = Definitions(
    assets=[
        facebook_ads_raw,
        google_ads_raw,
        google_analytics_raw,
        paypal_transactions_raw,
        stripe_charges_raw,
        *google_sheets_assets,
    ],
    jobs=[ingestion_job],
    schedules=[daily_schedule],
    resources={
        "bigquery": bigquery_resource,
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
