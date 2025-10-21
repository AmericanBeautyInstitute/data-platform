"""Tests for GoogleCloudStorageLoader."""

from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from load.google_cloud_storage_loader import GoogleCloudStorageLoader


def assert_bucket_called_correctly(
    mock_client: MagicMock, expected_bucket_name: str
) -> None:
    """Asserts that the GCS client bucket method was called with the expected name."""
    mock_client.bucket.assert_called_once_with(expected_bucket_name)


def assert_blob_called_correctly(
    mock_bucket: MagicMock, expected_blob_name: str
) -> None:
    """Asserts that the bucket.blob method was called with the expected blob name."""
    mock_bucket.blob.assert_called_once_with(expected_blob_name)


def assert_upload_called_correctly(mock_blob: MagicMock) -> None:
    """Asserts that upload_from_file was called with a file-like object."""
    args, kwargs = mock_blob.upload_from_file.call_args
    buffer_arg = args[0]
    assert hasattr(buffer_arg, "read"), "Expected a file-like object"
    assert kwargs["content_type"] == "application/octet-stream"


@pytest.fixture
def mock_credentials_file_path() -> Path:
    """Creates a temporary credentials file path."""
    tmp_dir = Path("scratch/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    credentials_file_path = tmp_dir / "mock_gcp_service_account_key.json"
    credentials_file_path.write_text('{"type": "service_account"}')
    return credentials_file_path


@pytest.fixture
def sample_table() -> pa.Table:
    """Creates a sample PyArrow Table."""
    data = {
        "name": ["Alice", "Bob", "Relena"],
        "age": ["25", "30", "35"],
        "city": ["Tokyo", "New York", "Paris"],
    }
    return pa.table(data)


@pytest.mark.parametrize(
    "timestamp,expected_blob_suffix",
    [
        (True, "2025-01-01T090000.parquet"),
        (False, ".parquet"),
    ],
)
def test_loader_upload_param(
    mocker: MockerFixture,
    mock_credentials_file_path: Path,
    sample_table: pa.Table,
    timestamp: bool,
    expected_blob_suffix: str,
) -> None:
    """Tests loader data file transfer with and without timestamps."""
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mocker.patch(
        "load.google_cloud_storage_loader.authenticate_google_cloud_storage",
        return_value=mock_client,
    )

    if timestamp:
        mocker.patch(
            "load.google_cloud_storage_loader.generate_timestamp",
            return_value="2025-01-01T090000",
        )

    loader = GoogleCloudStorageLoader(mock_credentials_file_path)
    loader.load(
        sample_table,
        bucket_name="mock-bucket",
        blob_name="students",
        timestamp=timestamp,
    )

    expected_blob_name = (
        f"students_{expected_blob_suffix}"
        if timestamp
        else f"students{expected_blob_suffix}"
    )

    assert_bucket_called_correctly(mock_client, "mock-bucket")
    assert_blob_called_correctly(mock_bucket, expected_blob_name)
    assert_upload_called_correctly(mock_blob)
