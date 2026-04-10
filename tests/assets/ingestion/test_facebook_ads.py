"""Tests for Facebook Ads ingestion asset."""

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from dagster import DailyPartitionsDefinition, materialize

from assets.ingestion.facebook_ads import TABLE, facebook_ads_raw
from assets.ingestion.resources import (
    FacebookAdsResource,
    IngestionConfig,
    bigquery_resource,
    gcs_resource,
)

ingestion_config = IngestionConfig(project="fake-project", bucket="my-bucket")

PARTITION_KEY = "2024-01-15"
FAKE_GCS_URI = "gs://my-bucket/facebook_ads/date=2024-01-15/facebook_ads-run-id.parquet"
FAKE_ROWS_LOADED = 25
FAKE_ACCESS_TOKEN = "fake-access-token"
FAKE_AD_ACCOUNT_ID = "act_123456789"
FAKE_PROJECT = "fake-project"
FAKE_BUCKET = "my-bucket"
EXPECTED_METADATA_KEYS = {"rows_loaded", "gcs_uri", "partition_date"}

SAMPLE_TABLE = pa.table(
    {
        "date": ["2024-01-15"],
        "campaign_id": ["123456789"],
        "campaign_name": ["ABI Spring Enrollment"],
        "impressions": [1000],
        "clicks": [50],
        "spend_usd": [25.50],
        "reach": [900],
        "frequency": [1.11],
        "link_clicks": [45],
        "leads": [3],
        "conversions": [1],
    }
)


@pytest.fixture
def env_vars(monkeypatch):
    """Sets required environment variables for asset execution."""
    monkeypatch.setenv("GCP_PROJECT_ID", FAKE_PROJECT)
    monkeypatch.setenv("GCS_BUCKET", FAKE_BUCKET)


@pytest.fixture
def facebook_ads_resource():
    """FacebookAdsResource with fake credentials."""
    return FacebookAdsResource(
        access_token=FAKE_ACCESS_TOKEN,
        ad_account_id=FAKE_AD_ACCOUNT_ID,
    )


def test_asset_name():
    """Asset name is facebook_ads_raw."""
    assert facebook_ads_raw.key.path[-1] == "facebook_ads_raw"


def test_asset_has_ingestion_group():
    """Asset belongs to the ingestion group."""
    assert facebook_ads_raw.group_names_by_key[facebook_ads_raw.key] == "ingestion"


def test_asset_has_daily_partitions():
    """Asset uses DailyPartitionsDefinition."""
    assert isinstance(facebook_ads_raw.partitions_def, DailyPartitionsDefinition)


def test_materialize_succeeds(env_vars, facebook_ads_resource):
    """Asset materializes without error using mocked dependencies."""
    with (
        patch(
            "assets.ingestion.facebook_ads.fb_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch("assets.ingestion.facebook_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.facebook_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.FacebookAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [facebook_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "facebook_ads": facebook_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    assert result.success


def test_materialize_passes_date_range_to_extract(env_vars, facebook_ads_resource):
    """extract() is called with start and end date equal to partition date."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.facebook_ads.fb_extract.extract", mock_extract),
        patch("assets.ingestion.facebook_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.facebook_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.FacebookAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [facebook_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "facebook_ads": facebook_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    args = mock_extract.call_args[0]
    assert args[1] == date.fromisoformat(PARTITION_KEY)
    assert args[2] == date.fromisoformat(PARTITION_KEY)


def test_materialize_gcs_source_is_table_name(env_vars, facebook_ads_resource):
    """GCS load is called with source equal to TABLE constant."""
    mock_gcs_load = MagicMock(return_value=FAKE_GCS_URI)

    with (
        patch(
            "assets.ingestion.facebook_ads.fb_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch("assets.ingestion.facebook_ads.gcs_load.load", mock_gcs_load),
        patch(
            "assets.ingestion.facebook_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.FacebookAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [facebook_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "facebook_ads": facebook_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    gcs_config_arg = mock_gcs_load.call_args[0][1]
    assert gcs_config_arg.source == TABLE


def test_materialize_output_metadata_keys(env_vars, facebook_ads_resource):
    """Output metadata contains all expected keys."""
    with (
        patch(
            "assets.ingestion.facebook_ads.fb_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch("assets.ingestion.facebook_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.facebook_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.FacebookAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [facebook_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "facebook_ads": facebook_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    mat_event = result.get_asset_materialization_events()[0]
    metadata_keys = set(mat_event.materialization.metadata.keys())
    assert EXPECTED_METADATA_KEYS.issubset(metadata_keys)
