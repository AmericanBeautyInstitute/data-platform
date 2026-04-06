"""Tests for Google Analytics ingestion asset."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from dagster import DailyPartitionsDefinition, materialize

from assets.ingestion.google_analytics import REPORT_CONFIG, TABLE, google_analytics_raw
from assets.ingestion.resources import (
    GoogleAnalyticsResource,
    IngestionConfig,
    bigquery_resource,
    gcs_resource,
)

ingestion_config = IngestionConfig(project="fake-project", bucket="my-bucket")

PARTITION_KEY = "2024-01-15"
FAKE_GCS_URI = (
    "gs://my-bucket/google_analytics/date=2024-01-15/google_analytics-run-id.parquet"
)
FAKE_ROWS_LOADED = 99
FAKE_CREDENTIALS_PATH = "/tmp/ga-creds.json"
FAKE_PROPERTY_ID = "123456"
FAKE_PROJECT = "fake-project"
FAKE_BUCKET = "my-bucket"
EXPECTED_METADATA_KEYS = {"rows_loaded", "gcs_uri", "partition_date"}

SAMPLE_TABLE = pa.table(
    {
        "date": ["2024-01-15"],
        "sessions": ["100"],
        "screenPageViews": ["500"],
    }
)


@pytest.fixture
def env_vars(monkeypatch):
    """Sets required environment variables for asset execution."""
    monkeypatch.setenv("GCP_PROJECT_ID", FAKE_PROJECT)
    monkeypatch.setenv("GCS_BUCKET", FAKE_BUCKET)


@pytest.fixture
def google_analytics_resource():
    """GoogleAnalyticsResource with fake credentials."""
    return GoogleAnalyticsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        property_id=FAKE_PROPERTY_ID,
    )


def test_asset_name():
    """Asset name is google_analytics_raw."""
    assert google_analytics_raw.key.path[-1] == "google_analytics_raw"


def test_asset_has_ingestion_group():
    """Asset belongs to the ingestion group."""
    assert (
        google_analytics_raw.group_names_by_key[google_analytics_raw.key] == "ingestion"
    )


def test_asset_has_daily_partitions():
    """Asset uses DailyPartitionsDefinition."""
    assert isinstance(google_analytics_raw.partitions_def, DailyPartitionsDefinition)


def test_report_config_has_date_dimension():
    """REPORT_CONFIG includes date as a dimension."""
    assert "date" in REPORT_CONFIG.dimension_names


def test_report_config_has_sessions_metric():
    """REPORT_CONFIG includes sessions as a metric."""
    assert "sessions" in REPORT_CONFIG.metric_names


def test_materialize_succeeds(env_vars, google_analytics_resource):
    """Asset materializes without error using mocked dependencies."""
    with (
        patch(
            "assets.ingestion.google_analytics.ga_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch(
            "assets.ingestion.google_analytics.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_analytics.bq_load.load",
            return_value=FAKE_ROWS_LOADED,
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAnalyticsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [google_analytics_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_analytics": google_analytics_resource,
                "ingestion_env": ingestion_config,
            },
        )

    assert result.success


def test_materialize_passes_date_range_to_extract(env_vars, google_analytics_resource):
    """extract() is called with start_date and end_date equal to partition date."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.google_analytics.ga_extract.extract", mock_extract),
        patch(
            "assets.ingestion.google_analytics.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_analytics.bq_load.load",
            return_value=FAKE_ROWS_LOADED,
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAnalyticsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [google_analytics_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_analytics": google_analytics_resource,
                "ingestion_env": ingestion_config,
            },
        )

    args = mock_extract.call_args[0]
    assert args[2] == PARTITION_KEY
    assert args[3] == PARTITION_KEY


def test_materialize_passes_property_id_to_extract(env_vars, google_analytics_resource):
    """extract() is called with the correct property_id from the resource."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.google_analytics.ga_extract.extract", mock_extract),
        patch(
            "assets.ingestion.google_analytics.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_analytics.bq_load.load",
            return_value=FAKE_ROWS_LOADED,
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAnalyticsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [google_analytics_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_analytics": google_analytics_resource,
                "ingestion_env": ingestion_config,
            },
        )

    assert mock_extract.call_args[0][1] == FAKE_PROPERTY_ID


def test_materialize_gcs_source_is_table_name(env_vars, google_analytics_resource):
    """GCS load is called with source equal to TABLE constant."""
    mock_gcs_load = MagicMock(return_value=FAKE_GCS_URI)

    with (
        patch(
            "assets.ingestion.google_analytics.ga_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch("assets.ingestion.google_analytics.gcs_load.load", mock_gcs_load),
        patch(
            "assets.ingestion.google_analytics.bq_load.load",
            return_value=FAKE_ROWS_LOADED,
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAnalyticsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [google_analytics_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_analytics": google_analytics_resource,
                "ingestion_env": ingestion_config,
            },
        )

    gcs_config_arg = mock_gcs_load.call_args[0][1]
    assert gcs_config_arg.source == TABLE


def test_materialize_output_metadata_keys(env_vars, google_analytics_resource):
    """Output metadata contains all expected keys."""
    with (
        patch(
            "assets.ingestion.google_analytics.ga_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch(
            "assets.ingestion.google_analytics.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_analytics.bq_load.load",
            return_value=FAKE_ROWS_LOADED,
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleAnalyticsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [google_analytics_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_analytics": google_analytics_resource,
                "ingestion_env": ingestion_config,
            },
        )

    mat_event = result.get_asset_materialization_events()[0]
    metadata_keys = set(mat_event.materialization.metadata.keys())
    assert EXPECTED_METADATA_KEYS.issubset(metadata_keys)
