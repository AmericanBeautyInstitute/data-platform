"""Tests for partitioned dlt ingestion assets."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from dagster import AssetKey
from dagster._core.definitions.partition import TimeWindow

from orchestration.defs.ingestion.assets import (
    INGESTION_ASSETS,
    _partition_date_range,
)
from orchestration.defs.ingestion.schedules import daily_partitions

EXPECTED_ASSET_KEYS = {
    AssetKey(["raw", "facebook_ads"]),
    AssetKey(["raw", "google_ads"]),
    AssetKey(["raw", "google_analytics"]),
    AssetKey(["raw", "google_sheets_inventory"]),
    AssetKey(["raw", "google_sheets_programs"]),
    AssetKey(["raw", "google_sheets_students"]),
    AssetKey(["raw", "paypal_transactions"]),
    AssetKey(["raw", "stripe_charges"]),
}


def test_ingestion_assets_match_dbt_raw_sources():
    """Ingestion assets expose every dbt raw source key."""
    asset_keys = {
        asset_key
        for assets_definition in INGESTION_ASSETS
        for asset_key in assets_definition.keys
    }
    assert asset_keys == EXPECTED_ASSET_KEYS


def test_ingestion_assets_use_daily_partitions():
    """Every ingestion asset uses the shared daily partitions definition."""
    assert all(
        assets_definition.partitions_def == daily_partitions
        for assets_definition in INGESTION_ASSETS
    )


def test_ingestion_assets_belong_to_ingestion_group():
    """Every raw asset belongs to the ingestion group."""
    group_names = {
        group_name
        for assets_definition in INGESTION_ASSETS
        for group_name in assets_definition.group_names_by_key.values()
    }
    assert group_names == {"ingestion"}


def test_partition_date_range_is_inclusive():
    """A Dagster time window becomes an inclusive source API date range."""
    timezone = ZoneInfo("America/New_York")
    window = TimeWindow(
        start=datetime(2024, 3, 9, tzinfo=timezone),
        end=datetime(2024, 3, 12, tzinfo=timezone),
    )
    assert _partition_date_range(window) == (
        date(2024, 3, 9),
        date(2024, 3, 11),
    )
