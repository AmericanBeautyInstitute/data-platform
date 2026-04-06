"""Tests for Google Ads ingestion asset."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from dagster import DailyPartitionsDefinition, materialize

from assets.ingestion.google_ads import QUERY, TABLE, google_ads_raw
from assets.ingestion.resources import (
    GoogleAdsResource,
    IngestionConfig,
    bigquery_resource,
    gcs_resource,
)

ingestion_config = IngestionConfig(project="fake-project", bucket="my-bucket")

PARTITION_KEY = "2024-01-15"
FAKE_GCS_URI = "gs://my-bucket/google_ads/date=2024-01-15/google_ads-run-id.parquet"
FAKE_ROWS_LOADED = 50
FAKE_CREDENTIALS_PATH = "/tmp/ads-creds.json"
FAKE_CUSTOMER_ID = "1234567890"
FAKE_PROJECT = "fake-project"
FAKE_BUCKET = "my-bucket"
EXPECTED_METADATA_KEYS = {"rows_loaded", "gcs_uri", "partition_date"}

SAMPLE_TABLE = pa.table(
    {
        "date": ["2024-01-15"],
        "clicks": [10],
        "impressions": [100],
        "cost_micros": [1500000],
        "conversions": [2.0],
        "customer_id": [FAKE_CUSTOMER_ID],
    }
)


@pytest.fixture
def env_vars(monkeypatch):
    """Sets required environment variables for asset execution."""
    monkeypatch.setenv("GCP_PROJECT_ID", FAKE_PROJECT)
    monkeypatch.setenv("GCS_BUCKET", FAKE_BUCKET)


@pytest.fixture
def google_ads_resource():
    """GoogleAdsResource with fake credentials."""
    return GoogleAdsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        customer_id=FAKE_CUSTOMER_ID,
    )


def test_asset_name():
    """Asset name is google_ads_raw."""
    assert google_ads_raw.key.path[-1] == "google_ads_raw"


def test_asset_has_ingestion_group():
    """Asset belongs to the ingestion group."""
    assert google_ads_raw.group_names_by_key[google_ads_raw.key] == "ingestion"


def test_asset_has_daily_partitions():
    """Asset uses DailyPartitionsDefinition."""
    assert isinstance(google_ads_raw.partitions_def, DailyPartitionsDefinition)


def test_query_contains_date_placeholder():
    """QUERY contains a {date} format placeholder."""
    assert "{date}" in QUERY


def test_materialize_succeeds(env_vars, google_ads_resource):
    """Asset materializes without error using mocked dependencies."""
    with (
        patch(
            "assets.ingestion.google_ads.ads_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.google_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.google_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [google_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_ads": google_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    assert result.success


def test_materialize_passes_formatted_query_to_extract(env_vars, google_ads_resource):
    """extract() is called with the partition date substituted into the query."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.google_ads.ads_extract.extract", mock_extract),
        patch("assets.ingestion.google_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.google_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [google_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_ads": google_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    query_arg = mock_extract.call_args[0][2]
    assert PARTITION_KEY in query_arg
    assert "{date}" not in query_arg


def test_materialize_passes_customer_id_to_extract(env_vars, google_ads_resource):
    """extract() is called with customer_id from the resource."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.google_ads.ads_extract.extract", mock_extract),
        patch("assets.ingestion.google_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.google_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [google_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_ads": google_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    assert mock_extract.call_args[0][1] == FAKE_CUSTOMER_ID


def test_materialize_gcs_source_is_table_name(env_vars, google_ads_resource):
    """GCS load is called with source equal to TABLE constant."""
    mock_gcs_load = MagicMock(return_value=FAKE_GCS_URI)

    with (
        patch(
            "assets.ingestion.google_ads.ads_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.google_ads.gcs_load.load", mock_gcs_load),
        patch(
            "assets.ingestion.google_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [google_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_ads": google_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    gcs_config_arg = mock_gcs_load.call_args[0][1]
    assert gcs_config_arg.source == TABLE


def test_materialize_output_metadata_keys(env_vars, google_ads_resource):
    """Output metadata contains all expected keys."""
    with (
        patch(
            "assets.ingestion.google_ads.ads_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.google_ads.gcs_load.load", return_value=FAKE_GCS_URI),
        patch(
            "assets.ingestion.google_ads.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAdsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [google_ads_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_ads": google_ads_resource,
                "ingestion_env": ingestion_config,
            },
        )

    mat_event = result.get_asset_materialization_events()[0]
    metadata_keys = set(mat_event.materialization.metadata.keys())
    assert EXPECTED_METADATA_KEYS.issubset(metadata_keys)
