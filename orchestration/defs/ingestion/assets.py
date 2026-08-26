"""Partitioned Dagster assets that load API data with dlt."""

from collections.abc import Iterator
from datetime import date, timedelta
from typing import Any, cast

import dlt as dlthub
from dagster import AssetExecutionContext, AssetKey, AssetSpec
from dagster._core.definitions.partition import TimeWindow
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from orchestration.defs.ingestion.resources import (
    FacebookAdsResource,
    GoogleAdsResource,
    GoogleAnalyticsResource,
    GoogleSheetsResource,
    PayPalResource,
    StripeResource,
)
from orchestration.defs.ingestion.schedules import daily_partitions
from sources.facebook_ads import facebook_ads_source
from sources.google_ads import google_ads_source
from sources.google_analytics import google_analytics_source
from sources.google_sheets import google_sheets_source
from sources.paypal import paypal_source
from sources.stripe import stripe_source

_DATASET_NAME = "raw"
_GROUP_NAME = "ingestion"
_SCHEMA_DATE = date(2000, 1, 1)
# dlt inspects source schemas during Dagster registration without iterating clients.
_SCHEMA_CLIENT = cast(Any, None)


class RawDagsterDltTranslator(DagsterDltTranslator):
    """Maps dlt resources to the asset keys used by dbt raw sources."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Returns an asset spec keyed as raw/<dlt resource name>."""
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            key=AssetKey([_DATASET_NAME, data.resource.name])
        )


def _pipeline(source_name: str) -> dlthub.Pipeline:
    """Returns a stable BigQuery pipeline for one source system."""
    return dlthub.pipeline(
        pipeline_name=f"{source_name}_ingestion",
        destination="bigquery",
        dataset_name=_DATASET_NAME,
        progress="log",
    )


# Dagster requires pipelines and sources when decorators register asset definitions.
_FACEBOOK_ADS_PIPELINE = _pipeline("facebook_ads")
_GOOGLE_ADS_PIPELINE = _pipeline("google_ads")
_GOOGLE_ANALYTICS_PIPELINE = _pipeline("google_analytics")
_GOOGLE_SHEETS_PIPELINE = _pipeline("google_sheets")
_PAYPAL_PIPELINE = _pipeline("paypal")
_STRIPE_PIPELINE = _pipeline("stripe")
_TRANSLATOR = RawDagsterDltTranslator()


@dlt_assets(
    dlt_source=facebook_ads_source(
        _SCHEMA_CLIENT,
        _SCHEMA_DATE,
        _SCHEMA_DATE,
    ),
    dlt_pipeline=_FACEBOOK_ADS_PIPELINE,
    name="facebook_ads_assets",
    group_name=_GROUP_NAME,
    partitions_def=daily_partitions,
    pool="facebook_ads",
    dagster_dlt_translator=_TRANSLATOR,
)
def facebook_ads_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    facebook_ads: FacebookAdsResource,
) -> Iterator[object]:
    """Loads one partition window of Facebook Ads campaign data."""
    start_date, end_date = _partition_dates(context)
    source = facebook_ads_source(
        facebook_ads.get_client(),
        start_date,
        end_date,
    )
    yield from dlt.run(context=context, dlt_source=source)


@dlt_assets(
    dlt_source=google_ads_source(
        _SCHEMA_CLIENT,
        "schema-customer",
        _SCHEMA_DATE,
        _SCHEMA_DATE,
    ),
    dlt_pipeline=_GOOGLE_ADS_PIPELINE,
    name="google_ads_assets",
    group_name=_GROUP_NAME,
    partitions_def=daily_partitions,
    pool="google_ads",
    dagster_dlt_translator=_TRANSLATOR,
)
def google_ads_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    google_ads: GoogleAdsResource,
) -> Iterator[object]:
    """Loads one partition window of Google Ads campaign data."""
    start_date, end_date = _partition_dates(context)
    source = google_ads_source(
        google_ads.get_client(),
        google_ads.customer_id,
        start_date,
        end_date,
    )
    yield from dlt.run(context=context, dlt_source=source)


@dlt_assets(
    dlt_source=google_analytics_source(
        _SCHEMA_CLIENT,
        "schema-property",
        _SCHEMA_DATE,
        _SCHEMA_DATE,
    ),
    dlt_pipeline=_GOOGLE_ANALYTICS_PIPELINE,
    name="google_analytics_assets",
    group_name=_GROUP_NAME,
    partitions_def=daily_partitions,
    pool="google_analytics",
    dagster_dlt_translator=_TRANSLATOR,
)
def google_analytics_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    google_analytics: GoogleAnalyticsResource,
) -> Iterator[object]:
    """Loads one partition window of Google Analytics session data."""
    start_date, end_date = _partition_dates(context)
    source = google_analytics_source(
        google_analytics.get_client(),
        google_analytics.property_id,
        start_date,
        end_date,
    )
    yield from dlt.run(context=context, dlt_source=source)


@dlt_assets(
    dlt_source=google_sheets_source(
        _SCHEMA_CLIENT,
        "schema-spreadsheet",
        _SCHEMA_DATE,
    ),
    dlt_pipeline=_GOOGLE_SHEETS_PIPELINE,
    name="google_sheets_assets",
    group_name=_GROUP_NAME,
    partitions_def=daily_partitions,
    pool="google_sheets",
    dagster_dlt_translator=_TRANSLATOR,
)
def google_sheets_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    google_sheets: GoogleSheetsResource,
) -> Iterator[object]:
    """Loads one dated snapshot of the configured Google spreadsheet."""
    snapshot_date, _ = _partition_dates(context)
    source = google_sheets_source(
        google_sheets.get_client(),
        google_sheets.spreadsheet_id,
        snapshot_date,
    )
    yield from dlt.run(context=context, dlt_source=source)


@dlt_assets(
    dlt_source=paypal_source(
        _SCHEMA_CLIENT,
        _SCHEMA_DATE,
        _SCHEMA_DATE,
    ),
    dlt_pipeline=_PAYPAL_PIPELINE,
    name="paypal_assets",
    group_name=_GROUP_NAME,
    partitions_def=daily_partitions,
    pool="paypal",
    dagster_dlt_translator=_TRANSLATOR,
)
def paypal_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    paypal: PayPalResource,
) -> Iterator[object]:
    """Loads one partition window of PayPal transactions."""
    start_date, end_date = _partition_dates(context)
    source = paypal_source(
        paypal.get_client(),
        start_date,
        end_date,
    )
    yield from dlt.run(context=context, dlt_source=source)


@dlt_assets(
    dlt_source=stripe_source(
        _SCHEMA_CLIENT,
        _SCHEMA_DATE,
        _SCHEMA_DATE,
    ),
    dlt_pipeline=_STRIPE_PIPELINE,
    name="stripe_assets",
    group_name=_GROUP_NAME,
    partitions_def=daily_partitions,
    pool="stripe",
    dagster_dlt_translator=_TRANSLATOR,
)
def stripe_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    stripe: StripeResource,
) -> Iterator[object]:
    """Loads one partition window of Stripe charges."""
    start_date, end_date = _partition_dates(context)
    source = stripe_source(
        stripe.get_client(),
        start_date,
        end_date,
    )
    yield from dlt.run(context=context, dlt_source=source)


INGESTION_ASSETS = [
    facebook_ads_assets,
    google_ads_assets,
    google_analytics_assets,
    google_sheets_assets,
    paypal_assets,
    stripe_assets,
]


def _partition_dates(context: AssetExecutionContext) -> tuple[date, date]:
    """Returns the inclusive source date range for the current partition."""
    return _partition_date_range(context.partition_time_window)


def _partition_date_range(window: TimeWindow) -> tuple[date, date]:
    """Returns the inclusive source date range for a Dagster time window."""
    start_date = window.start.date()
    end_date = window.end.date() - timedelta(days=1)
    return start_date, end_date
