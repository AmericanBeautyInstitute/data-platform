"""Tests for BigQueryLoader."""

from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock, Mock

import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from config.loaders import BigQueryLoadConfig
from load.bigquery_loader import BigQueryLoader


def assert_table_created(
    mock_ensure_table_exists: MagicMock,
    expected_dataset: str,
    expected_table: str,
) -> None:
    """Asserts that ensure_table_exists was called with correct config."""
    mock_ensure_table_exists.assert_called_once()
    call_args = mock_ensure_table_exists.call_args
    config = call_args[0][1]  # Second argument is the config
    assert config.dataset_id == expected_dataset
    assert config.table_id == expected_table


def assert_load_job_executed(mock_load_job: MagicMock) -> None:
    """Asserts that the load job result was called."""
    mock_load_job.result.assert_called_once()


def assert_job_config_correct(
    mock_load_method: MagicMock,
    expected_write_disposition: str,
    expected_source_format: str,
) -> None:
    """Asserts that job config was built correctly."""
    call_args = mock_load_method.call_args
    job_config = call_args[0][2]  # Third argument is job_config
    assert job_config.write_disposition == expected_write_disposition
    assert job_config.source_format.name == expected_source_format


@pytest.fixture
def mock_bigquery_client() -> MagicMock:
    """Creates a mocked BigQuery client."""
    mock_client = MagicMock()
    mock_client.project = "test-project"
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
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    sample_table: pa.Table,
    basic_load_config: BigQueryLoadConfig,
) -> None:
    """Tests loading from PyArrow table."""
    mock_ensure_table = mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 3
    mock_bigquery_client.load_table_from_file.return_value = mock_load_job

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, basic_load_config)

    assert_table_created(mock_ensure_table, "staging", "stg_students")
    assert_load_job_executed(mock_load_job)
    mock_bigquery_client.load_table_from_file.assert_called_once()


def test_load_from_gcs_uri(
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    basic_load_config: BigQueryLoadConfig,
) -> None:
    """Tests loading from GCS URI."""
    mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 100
    mock_bigquery_client.load_table_from_uri.return_value = mock_load_job

    gcs_uri = "gs://my-bucket/data/students/*.parquet"
    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(gcs_uri, basic_load_config)

    assert_load_job_executed(mock_load_job)
    mock_bigquery_client.load_table_from_uri.assert_called_once()
    call_args = mock_bigquery_client.load_table_from_uri.call_args
    assert call_args[0][0] == gcs_uri
    assert "test-project.staging.stg_students" in call_args[0][1]


def test_load_from_file_path(
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    basic_load_config: BigQueryLoadConfig,
    tmp_path: Path,
) -> None:
    """Tests loading from local file path."""
    mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 50
    mock_bigquery_client.load_table_from_file.return_value = mock_load_job

    # Create a temporary file
    test_file = tmp_path / "test.parquet"
    test_file.write_bytes(b"fake parquet data")

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(test_file, basic_load_config)

    assert_load_job_executed(mock_load_job)
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
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    sample_table: pa.Table,
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
    source_format: Literal["PARQUET", "CSV", "JSON", "AVRO"],
) -> None:
    """Tests different load configuration options."""
    mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 3
    mock_bigquery_client.load_table_from_file.return_value = mock_load_job

    config = BigQueryLoadConfig(
        dataset_id="staging",
        table_id="stg_test",
        write_disposition=write_disposition,
        source_format=source_format,
    )

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, config)

    # Verify job config was built correctly
    call_args = mock_bigquery_client.load_table_from_file.call_args
    job_config = call_args[1]["job_config"]
    assert job_config.write_disposition == write_disposition


def test_create_table_if_needed_false(
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    sample_table: pa.Table,
) -> None:
    """Tests that table creation is skipped when create_table_if_needed=False."""
    mock_ensure_table = mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 3
    mock_bigquery_client.load_table_from_file.return_value = mock_load_job

    config = BigQueryLoadConfig(
        dataset_id="staging",
        table_id="stg_students",
        create_table_if_needed=False,  # Disable table creation
    )

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, config)

    mock_ensure_table.assert_not_called()
    assert_load_job_executed(mock_load_job)


def test_load_with_partitioning_and_clustering(
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    sample_table: pa.Table,
) -> None:
    """Tests loading with partition and cluster fields."""
    mock_ensure_table = mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 3
    mock_bigquery_client.load_table_from_file.return_value = mock_load_job

    config = BigQueryLoadConfig(
        dataset_id="staging",
        table_id="stg_students",
        partition_field="created_date",
        cluster_fields=["student_id", "school_id"],
    )

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(sample_table, config)

    # Verify table config passed to ensure_table_exists
    call_args = mock_ensure_table.call_args
    table_config = call_args[0][1]
    assert table_config.partition_field == "created_date"
    assert table_config.cluster_fields == ["student_id", "school_id"]


def test_load_invalid_data_type(
    mock_bigquery_client: MagicMock,
    basic_load_config: BigQueryLoadConfig,
) -> None:
    """Tests that invalid data types raise TypeError."""
    loader = BigQueryLoader(client=mock_bigquery_client)

    with pytest.raises(TypeError, match="Unsupported data type"):
        loader.load(12345, basic_load_config)  # Invalid: integer


def test_load_gcs_uri_not_starting_with_gs(
    mocker: MockerFixture,
    mock_bigquery_client: MagicMock,
    basic_load_config: BigQueryLoadConfig,
    tmp_path: Path,
) -> None:
    """Tests that strings not starting with gs:// are treated as file paths."""
    mocker.patch("load.bigquery_loader.ensure_table_exists")
    mock_load_job = Mock()
    mock_load_job.output_rows = 10
    mock_bigquery_client.load_table_from_file.return_value = mock_load_job

    # Create a temporary file with a string path
    test_file = tmp_path / "data.parquet"
    test_file.write_bytes(b"fake data")

    loader = BigQueryLoader(client=mock_bigquery_client)
    loader.load(str(test_file), basic_load_config)

    # Should call load_table_from_file, not load_table_from_uri
    mock_bigquery_client.load_table_from_file.assert_called_once()
    mock_bigquery_client.load_table_from_uri.assert_not_called()
