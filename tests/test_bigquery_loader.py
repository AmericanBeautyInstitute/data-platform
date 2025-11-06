"""Tests for BigQueryLoader."""

from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pyarrow as pa
import pytest
from google.cloud.exceptions import NotFound

from config.loaders import BigQueryLoadConfig
from load.bigquery_loader import BigQueryLoader


@pytest.fixture
def mock_bigquery_client() -> Mock:
    """Creates a mocked BigQuery client."""
    mock_client = Mock()
    mock_client.project = "test-project"

    mock_client.get_table.side_effect = NotFound("Table not found")
    mock_client.create_table.side_effect = lambda table: table

    mock_client.load_table_from_file.side_effect = (
        lambda file, table_ref, job_config: Mock(output_rows=3, result=lambda: None)
    )
    mock_client.load_table_from_uri.side_effect = (
        lambda uri, table_ref, job_config: Mock(output_rows=100, result=lambda: None)
    )

    return mock_client


@pytest.fixture
def sample_table() -> pa.Table:
    """Creates a sample PyArrow Table."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    }
    return pa.table(data)


@pytest.fixture
def basic_load_config() -> BigQueryLoadConfig:
    """Creates a basic load configuration."""
    return BigQueryLoadConfig(
        dataset_id="staging",
        table_id="stg_students",
    )


def test_load_from_arrow_table(
    mock_bigquery_client: Mock,
    sample_table: pa.Table,
    basic_load_config: BigQueryLoadConfig,
) -> None:
    """Tests loading from PyArrow table."""
    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, basic_load_config)

    assert mock_bigquery_client.get_table.called
    assert mock_bigquery_client.create_table.called

    assert mock_bigquery_client.load_table_from_file.called


def test_load_from_gcs_uri(
    mock_bigquery_client: Mock, basic_load_config: BigQueryLoadConfig
) -> None:
    """Tests loading from GCS URI."""
    gcs_uri = "gs://my-bucket/data/students/*.parquet"
    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(gcs_uri, basic_load_config)

    mock_bigquery_client.load_table_from_uri.assert_called_once()
    call_args = mock_bigquery_client.load_table_from_uri.call_args
    assert call_args[0][0] == gcs_uri
    assert "test-project.staging.stg_students" in call_args[0][1]


def test_load_from_file_path(
    mock_bigquery_client: Mock, basic_load_config: BigQueryLoadConfig, tmp_path: Path
) -> None:
    """Tests loading from local file path."""
    test_file = tmp_path / "test.parquet"
    test_file.write_bytes(b"fake parquet data")

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(test_file, basic_load_config)

    mock_bigquery_client.load_table_from_file.assert_called_once()


@pytest.mark.parametrize(
    "write_disposition,source_format",
    [
        ("WRITE_TRUNCATE", "PARQUET"),
        ("WRITE_APPEND", "PARQUET"),
        ("WRITE_TRUNCATE", "CSV"),
    ],
)
def test_load_config_variations(
    mock_bigquery_client: Mock,
    sample_table: pa.Table,
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
    source_format: Literal["PARQUET", "CSV", "JSON", "AVRO"],
) -> None:
    """Tests different load configuration options."""
    config = BigQueryLoadConfig(
        dataset_id="staging",
        table_id="stg_test",
        write_disposition=write_disposition,
        source_format=source_format,
    )

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, config)

    assert mock_bigquery_client.load_table_from_file.called


def test_create_table_if_needed_false(
    mock_bigquery_client: Mock, sample_table: pa.Table
) -> None:
    """Tests that table creation is skipped when create_table_if_needed=False."""
    config = BigQueryLoadConfig(
        dataset_id="staging",
        table_id="stg_students",
        create_table_if_needed=False,
    )

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, config)

    # Table creation should NOT be called
    assert not mock_bigquery_client.get_table.called
    assert not mock_bigquery_client.create_table.called


def test_load_invalid_data_type(
    mock_bigquery_client: Mock, basic_load_config: BigQueryLoadConfig
) -> None:
    """Tests that invalid data types raise TypeError."""
    loader = BigQueryLoader(client=mock_bigquery_client)

    with pytest.raises(TypeError, match="Unsupported data type"):
        loader.load(12345, basic_load_config)


def test_load_gcs_uri_not_starting_with_gs(
    mock_bigquery_client: Mock, basic_load_config: BigQueryLoadConfig, tmp_path: Path
) -> None:
    """Tests that strings not starting with gs:// are treated as file paths."""
    test_file = tmp_path / "data.parquet"
    test_file.write_bytes(b"fake data")

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(str(test_file), basic_load_config)

    mock_bigquery_client.load_table_from_file.assert_called_once()
    mock_bigquery_client.load_table_from_uri.assert_not_called()
