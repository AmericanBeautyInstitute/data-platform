"""Tests for BigQuery partition loader."""

from datetime import date
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from load.bigquery.load import _build_job_config, _build_partition_ref, load
from load.config import BigQueryConfig

PROJECT = "my-project"
DATASET = "raw"
TABLE = "google_ads"

PARTITION_DATE = date(2024, 1, 15)

GCS_URI = "gs://my-bucket/google_ads/date=2024-01-15/google_ads-abc-123.parquet"

EXPECTED_IDEMPOTENT_CALL_COUNT = 2
EXPECTED_PARTITION_REF = "my-project.raw.google_ads$20240115"
EXPECTED_ROWS_LOADED = 100


@pytest.fixture
def config():
    """Sample BigQueryConfig."""
    return BigQueryConfig(
        project=PROJECT,
        dataset=DATASET,
        table=TABLE,
        partition_date=PARTITION_DATE,
    )


@pytest.fixture
def config_with_clustering():
    """BigQueryConfig with cluster fields."""
    return BigQueryConfig(
        project=PROJECT,
        dataset=DATASET,
        table=TABLE,
        partition_date=PARTITION_DATE,
        cluster_fields=["customer_id", "campaign_id"],
    )


@pytest.fixture
def mock_client():
    """Mocked BigQuery client."""
    client = MagicMock(spec=bigquery.Client)
    client.load_table_from_uri.return_value.output_rows = EXPECTED_ROWS_LOADED
    return client


def test_build_partition_ref_correct_format(config):
    """Partition ref uses project.dataset.table$YYYYMMDD format."""
    result = _build_partition_ref(config)

    assert result == EXPECTED_PARTITION_REF


def test_build_partition_ref_date_format(config):
    """Date in partition ref uses YYYYMMDD format without separators."""
    result = _build_partition_ref(config)

    assert "$20240115" in result


def test_build_partition_ref_contains_full_table_path(config):
    """Partition ref contains full project.dataset.table path."""
    result = _build_partition_ref(config)

    assert f"{PROJECT}.{DATASET}.{TABLE}" in result


def test_build_job_config_returns_load_job_config(config):
    """Returns a BigQuery LoadJobConfig instance."""
    result = _build_job_config(config)

    assert isinstance(result, bigquery.LoadJobConfig)


def test_build_job_config_source_format_is_parquet(config):
    """Source format is set to PARQUET."""
    result = _build_job_config(config)

    assert result.source_format == bigquery.SourceFormat.PARQUET


def test_build_job_config_write_disposition_is_truncate(config):
    """Write disposition is WRITE_TRUNCATE."""
    result = _build_job_config(config)

    assert result.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE


def test_build_job_config_autodetect_is_false(config):
    """Autodetect is disabled."""
    result = _build_job_config(config)

    assert result.autodetect is False


def test_build_job_config_time_partitioning_field(config):
    """Time partitioning uses configured partition field."""
    result = _build_job_config(config)

    assert result.time_partitioning.field == config.partition_field


def test_build_job_config_no_clustering_when_empty(config):
    """Clustering fields is None when config has empty list."""
    result = _build_job_config(config)

    assert result.clustering_fields is None


def test_build_job_config_clustering_fields_set(config_with_clustering):
    """Clustering fields are set when provided in config."""
    result = _build_job_config(config_with_clustering)

    assert result.clustering_fields == ["customer_id", "campaign_id"]


def test_load_returns_row_count(mock_client, config):
    """Returns number of rows loaded."""
    result = load(GCS_URI, config, mock_client)

    assert result == EXPECTED_ROWS_LOADED


def test_load_calls_load_table_from_uri(mock_client, config):
    """load_table_from_uri called with correct GCS URI and partition ref."""
    load(GCS_URI, config, mock_client)

    mock_client.load_table_from_uri.assert_called_once_with(
        GCS_URI,
        EXPECTED_PARTITION_REF,
        job_config=mock_client.load_table_from_uri.call_args.kwargs["job_config"],
    )


def test_load_calls_job_result(mock_client, config):
    """job.result() is called to block until load completes."""
    load(GCS_URI, config, mock_client)

    mock_client.load_table_from_uri.return_value.result.assert_called_once()


def test_load_uses_correct_partition_ref(mock_client, config):
    """Load targets the correct partition decorator."""
    load(GCS_URI, config, mock_client)

    call_args = mock_client.load_table_from_uri.call_args
    assert call_args.args[1] == EXPECTED_PARTITION_REF


def test_load_idempotent_on_repeated_calls(mock_client, config):
    """Repeated loads with same config call load_table_from_uri each time."""
    load(GCS_URI, config, mock_client)
    load(GCS_URI, config, mock_client)

    assert mock_client.load_table_from_uri.call_count == EXPECTED_IDEMPOTENT_CALL_COUNT
