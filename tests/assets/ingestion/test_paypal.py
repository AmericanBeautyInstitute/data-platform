"""Tests for PayPal ingestion asset."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from dagster import DailyPartitionsDefinition, materialize

from assets.ingestion.paypal import TABLE, paypal_transactions_raw
from assets.ingestion.resources import (
    PayPalResource,
    bigquery_resource,
    gcs_resource,
)

PARTITION_KEY = "2024-01-15"
FAKE_GCS_URI = "gs://my-bucket/paypal_transactions/date=2024-01-15/paypal_transactions-run-id.parquet"
FAKE_ROWS_LOADED = 10
FAKE_CLIENT_ID = "fake-client-id"
FAKE_CLIENT_SECRET = "fake-client-secret"
FAKE_PROJECT = "fake-project"
FAKE_BUCKET = "my-bucket"
EXPECTED_METADATA_KEYS = {"rows_loaded", "gcs_uri", "partition_date"}

SAMPLE_TABLE = pa.table(
    {
        "transaction_id": ["TXN123456"],
        "transaction_date": ["2024-01-15"],
        "gross_amount_usd": [150.00],
        "currency_code": ["USD"],
        "transaction_status": ["S"],
        "transaction_subject": ["Enrollment"],
        "payer_email": ["student@example.com"],
        "payer_name": ["Alice Smith"],
        "fee_amount_usd": [-4.65],
        "net_amount_usd": [145.35],
    }
)


@pytest.fixture
def env_vars(monkeypatch):
    """Sets required environment variables for asset execution."""
    monkeypatch.setenv("GCP_PROJECT_ID", FAKE_PROJECT)
    monkeypatch.setenv("GCS_BUCKET", FAKE_BUCKET)


@pytest.fixture
def paypal_resource():
    """PayPalResource with fake credentials."""
    return PayPalResource(
        client_id=FAKE_CLIENT_ID,
        client_secret=FAKE_CLIENT_SECRET,
    )


def test_asset_name():
    """Asset name is paypal_transactions_raw."""
    assert paypal_transactions_raw.key.path[-1] == "paypal_transactions_raw"


def test_asset_has_ingestion_group():
    """Asset belongs to the ingestion group."""
    assert (
        paypal_transactions_raw.group_names_by_key[paypal_transactions_raw.key]
        == "ingestion"
    )


def test_asset_has_daily_partitions():
    """Asset uses DailyPartitionsDefinition."""
    assert isinstance(paypal_transactions_raw.partitions_def, DailyPartitionsDefinition)


def test_materialize_succeeds(env_vars, paypal_resource):
    """Asset materializes without error using mocked dependencies."""
    with (
        patch(
            "assets.ingestion.paypal.paypal_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.paypal.gcs_load.load", return_value=FAKE_GCS_URI),
        patch("assets.ingestion.paypal.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.PayPalResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [paypal_transactions_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "paypal": paypal_resource,
            },
        )

    assert result.success


def test_materialize_passes_date_range_to_extract(env_vars, paypal_resource):
    """extract() is called with start and end date equal to partition date."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.paypal.paypal_extract.extract", mock_extract),
        patch("assets.ingestion.paypal.gcs_load.load", return_value=FAKE_GCS_URI),
        patch("assets.ingestion.paypal.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.PayPalResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [paypal_transactions_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "paypal": paypal_resource,
            },
        )

    args = mock_extract.call_args[0]
    assert args[1] == PARTITION_KEY
    assert args[2] == PARTITION_KEY


def test_materialize_gcs_source_is_table_name(env_vars, paypal_resource):
    """GCS load is called with source equal to TABLE constant."""
    mock_gcs_load = MagicMock(return_value=FAKE_GCS_URI)

    with (
        patch(
            "assets.ingestion.paypal.paypal_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.paypal.gcs_load.load", mock_gcs_load),
        patch("assets.ingestion.paypal.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.PayPalResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [paypal_transactions_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "paypal": paypal_resource,
            },
        )

    gcs_config_arg = mock_gcs_load.call_args[0][1]
    assert gcs_config_arg.source == TABLE


def test_materialize_output_metadata_keys(env_vars, paypal_resource):
    """Output metadata contains all expected keys."""
    with (
        patch(
            "assets.ingestion.paypal.paypal_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.paypal.gcs_load.load", return_value=FAKE_GCS_URI),
        patch("assets.ingestion.paypal.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.PayPalResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [paypal_transactions_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "paypal": paypal_resource,
            },
        )

    mat_event = result.get_asset_materialization_events()[0]
    metadata_keys = set(mat_event.materialization.metadata.keys())
    assert EXPECTED_METADATA_KEYS.issubset(metadata_keys)
