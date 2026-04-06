"""Tests for Stripe ingestion asset."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from dagster import DailyPartitionsDefinition, materialize

from assets.ingestion.resources import (
    StripeResource,
    bigquery_resource,
    gcs_resource,
)
from assets.ingestion.stripe import TABLE, stripe_charges_raw

PARTITION_KEY = "2024-01-15"
FAKE_GCS_URI = (
    "gs://my-bucket/stripe_charges/date=2024-01-15/stripe_charges-run-id.parquet"
)
FAKE_ROWS_LOADED = 8
FAKE_SECRET_KEY = "sk_test_fake"
FAKE_PROJECT = "fake-project"
FAKE_BUCKET = "my-bucket"
EXPECTED_METADATA_KEYS = {"rows_loaded", "gcs_uri", "partition_date"}

SAMPLE_TABLE = pa.table(
    {
        "charge_id": ["ch_abc123"],
        "charge_date": ["2024-01-15"],
        "gross_amount_usd": [150.00],
        "amount_captured_usd": [150.00],
        "fee_usd": [4.65],
        "net_usd": [145.35],
        "currency": ["usd"],
        "status": ["succeeded"],
        "description": ["Enrollment"],
        "customer_email": ["student@example.com"],
        "customer_name": ["Alice Smith"],
        "payment_intent_id": ["pi_abc123"],
    }
)


@pytest.fixture
def env_vars(monkeypatch):
    """Sets required environment variables for asset execution."""
    monkeypatch.setenv("GCP_PROJECT_ID", FAKE_PROJECT)
    monkeypatch.setenv("GCS_BUCKET", FAKE_BUCKET)


@pytest.fixture
def stripe_resource():
    """StripeResource with fake credentials."""
    return StripeResource(secret_key=FAKE_SECRET_KEY)


def test_asset_name():
    """Asset name is stripe_charges_raw."""
    assert stripe_charges_raw.key.path[-1] == "stripe_charges_raw"


def test_asset_has_ingestion_group():
    """Asset belongs to the ingestion group."""
    assert stripe_charges_raw.group_names_by_key[stripe_charges_raw.key] == "ingestion"


def test_asset_has_daily_partitions():
    """Asset uses DailyPartitionsDefinition."""
    assert isinstance(stripe_charges_raw.partitions_def, DailyPartitionsDefinition)


def test_materialize_succeeds(env_vars, stripe_resource):
    """Asset materializes without error using mocked dependencies."""
    with (
        patch(
            "assets.ingestion.stripe.stripe_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.stripe.gcs_load.load", return_value=FAKE_GCS_URI),
        patch("assets.ingestion.stripe.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.StripeResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [stripe_charges_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "stripe": stripe_resource,
            },
        )

    assert result.success


def test_materialize_passes_date_range_to_extract(env_vars, stripe_resource):
    """extract() is called with start and end date equal to partition date."""
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.stripe.stripe_extract.extract", mock_extract),
        patch("assets.ingestion.stripe.gcs_load.load", return_value=FAKE_GCS_URI),
        patch("assets.ingestion.stripe.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.StripeResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [stripe_charges_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "stripe": stripe_resource,
            },
        )

    args = mock_extract.call_args[0]
    assert args[1] == PARTITION_KEY
    assert args[2] == PARTITION_KEY


def test_materialize_gcs_source_is_table_name(env_vars, stripe_resource):
    """GCS load is called with source equal to TABLE constant."""
    mock_gcs_load = MagicMock(return_value=FAKE_GCS_URI)

    with (
        patch(
            "assets.ingestion.stripe.stripe_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.stripe.gcs_load.load", mock_gcs_load),
        patch("assets.ingestion.stripe.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.StripeResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [stripe_charges_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "stripe": stripe_resource,
            },
        )

    gcs_config_arg = mock_gcs_load.call_args[0][1]
    assert gcs_config_arg.source == TABLE


def test_materialize_output_metadata_keys(env_vars, stripe_resource):
    """Output metadata contains all expected keys."""
    with (
        patch(
            "assets.ingestion.stripe.stripe_extract.extract", return_value=SAMPLE_TABLE
        ),
        patch("assets.ingestion.stripe.gcs_load.load", return_value=FAKE_GCS_URI),
        patch("assets.ingestion.stripe.bq_load.load", return_value=FAKE_ROWS_LOADED),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.StripeResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [stripe_charges_raw],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "stripe": stripe_resource,
            },
        )

    mat_event = result.get_asset_materialization_events()[0]
    metadata_keys = set(mat_event.materialization.metadata.keys())
    assert EXPECTED_METADATA_KEYS.issubset(metadata_keys)
