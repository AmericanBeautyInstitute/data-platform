"""Tests for Google Sheets ingestion assets."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from dagster import DailyPartitionsDefinition, materialize

from assets.ingestion.google_sheets import (
    SHEET_NAMES,
    build_google_sheets_asset,
    google_sheets_assets,
)
from assets.ingestion.resources import (
    GoogleSheetsResource,
    bigquery_resource,
    gcs_resource,
)

EXPECTED_ASSET_COUNT = 3
EXPECTED_METADATA_KEYS = {"rows_loaded", "gcs_uri", "partition_date", "sheet_name"}
PARTITION_KEY = "2024-01-15"
FAKE_GCS_URI = "gs://my-bucket/google_sheets_students/date=2024-01-15/google_sheets_students-run-id.parquet"
FAKE_ROWS_LOADED = 42
FAKE_CREDENTIALS_PATH = "/tmp/creds.json"
FAKE_SPREADSHEET_ID = "fake-spreadsheet-id"
FAKE_PROJECT = "fake-project"
FAKE_BUCKET = "my-bucket"

SAMPLE_TABLE = pa.table(
    {
        "name": ["Alice", "Bob"],
        "age": ["25", "30"],
    }
)


@pytest.fixture
def env_vars(monkeypatch):
    """Sets required environment variables for asset execution."""
    monkeypatch.setenv("GCP_PROJECT_ID", FAKE_PROJECT)
    monkeypatch.setenv("GCS_BUCKET", FAKE_BUCKET)


@pytest.fixture
def google_sheets_resource():
    """GoogleSheetsResource with fake credentials."""
    return GoogleSheetsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        spreadsheet_id=FAKE_SPREADSHEET_ID,
    )


def test_google_sheets_assets_count():
    """One asset is built per sheet name."""
    assert len(google_sheets_assets) == EXPECTED_ASSET_COUNT


def test_google_sheets_assets_names():
    """Asset names follow the google_sheets_{sheet_name}_raw pattern."""
    names = {a.key.path[-1] for a in google_sheets_assets}
    expected = {f"google_sheets_{n}_raw" for n in SHEET_NAMES}
    assert names == expected


def test_all_assets_have_ingestion_group():
    """All assets belong to the ingestion group."""
    for a in google_sheets_assets:
        assert a.group_names_by_key[a.key] == "ingestion"


def test_all_assets_have_daily_partitions():
    """All assets use DailyPartitionsDefinition."""
    for a in google_sheets_assets:
        assert isinstance(a.partitions_def, DailyPartitionsDefinition)


def test_build_google_sheets_asset_returns_asset():
    """Factory returns a valid Dagster asset."""
    asset_def = build_google_sheets_asset("students")
    assert asset_def.key.path[-1] == "google_sheets_students_raw"


def test_materialize_students_asset(env_vars, google_sheets_resource):
    """Students asset materializes without error using mocked dependencies."""
    asset_def = build_google_sheets_asset("students")

    with (
        patch(
            "assets.ingestion.google_sheets.sheets_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch(
            "assets.ingestion.google_sheets.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_sheets.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleSheetsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [asset_def],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_sheets": google_sheets_resource,
            },
        )

    assert result.success


def test_materialize_passes_sheet_name_to_extract(env_vars, google_sheets_resource):
    """extract() is called with the correct sheet_name."""
    asset_def = build_google_sheets_asset("programs")
    mock_extract = MagicMock(return_value=SAMPLE_TABLE)

    with (
        patch("assets.ingestion.google_sheets.sheets_extract.extract", mock_extract),
        patch(
            "assets.ingestion.google_sheets.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_sheets.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleSheetsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [asset_def],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_sheets": google_sheets_resource,
            },
        )

    assert mock_extract.call_args[0][2] == "programs"


def test_materialize_gcs_source_includes_sheet_name(env_vars, google_sheets_resource):
    """GCS load is called with source name containing the sheet name."""
    asset_def = build_google_sheets_asset("inventory")
    mock_gcs_load = MagicMock(return_value=FAKE_GCS_URI)

    with (
        patch(
            "assets.ingestion.google_sheets.sheets_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch("assets.ingestion.google_sheets.gcs_load.load", mock_gcs_load),
        patch(
            "assets.ingestion.google_sheets.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleSheetsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        materialize(
            [asset_def],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_sheets": google_sheets_resource,
            },
        )

    gcs_config_arg = mock_gcs_load.call_args[0][1]
    assert "inventory" in gcs_config_arg.source


def test_materialize_output_metadata_keys(env_vars, google_sheets_resource):
    """Output metadata contains all expected keys."""
    asset_def = build_google_sheets_asset("students")

    with (
        patch(
            "assets.ingestion.google_sheets.sheets_extract.extract",
            return_value=SAMPLE_TABLE,
        ),
        patch(
            "assets.ingestion.google_sheets.gcs_load.load", return_value=FAKE_GCS_URI
        ),
        patch(
            "assets.ingestion.google_sheets.bq_load.load", return_value=FAKE_ROWS_LOADED
        ),
        patch("dagster_gcp.GCSResource.get_client", return_value=MagicMock()),
        patch("dagster_gcp.BigQueryResource.get_client", return_value=MagicMock()),
        patch(
            "assets.ingestion.resources.GoogleSheetsResource.get_client",
            return_value=MagicMock(),
        ),
    ):
        result = materialize(
            [asset_def],
            partition_key=PARTITION_KEY,
            resources={
                "gcs": gcs_resource,
                "bigquery": bigquery_resource,
                "google_sheets": google_sheets_resource,
            },
        )

    mat_event = result.get_asset_materialization_events()[0]
    metadata_keys = set(mat_event.materialization.metadata.keys())
    assert EXPECTED_METADATA_KEYS.issubset(metadata_keys)
