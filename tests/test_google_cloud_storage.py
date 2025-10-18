"""Tests for GoogleCloudStorageLoader."""

from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from load.google_cloud_storage_loader import GoogleCloudStorageLoader


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
    """Creates a sample PyArrow table."""
    data = {
        "name": ["Alice", "Bob", "Relena"],
        "age": ["25", "30", "35"],
        "city": ["Tokyo", "New York", "Paris"],
    }
    return pa.table(data)


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
    """Asserts that the uploaded file is a byte-like object."""
    args, kwargs = mock_blob.upload_from_file.call_args
    buffer_arg = args[0]
    assert hasattr(buffer_arg, "read"), "Expected a file-like object"
    assert kwargs["content_type"] == "application/octet-stream"


def test_loader_upload(
    mocker: MockerFixture,
    mock_credentials_file_path: Path,
    sample_table: pa.Table,
) -> None:
    """Tests that the loader attempts to upload a parquet file."""
    mock_blob = mocker.MagicMock()
    mock_bucket = mocker.MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = mocker.MagicMock()
    mock_client.bucket.return_value = mock_bucket

    target = "load.google_cloud_storage_loader.authenticate_google_cloud_storage"
    mocker.patch(target=target, return_value=mock_client)

    loader = GoogleCloudStorageLoader(mock_credentials_file_path)
    loader.load(
        data=sample_table,
        bucket_name="mock_bucket",
        blob_name="students.parquet",
    )

    assert_bucket_called_correctly(mock_client, "mock_bucket")
    assert_blob_called_correctly(mock_bucket, "students.parquet")
    assert_upload_called_correctly(mock_blob)
