"""Tests for GCS parquet loader."""

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.cloud import storage

from load.config import GCSConfig
from load.gcs.load import _serialize, _upload, load

BUCKET = "my-bucket"
SOURCE = "google_ads"
RUN_ID = "abc-123"
PARTITION_DATE = date(2024, 1, 15)
EXPECTED_BLOB_PATH = "google_ads/date=2024-01-15/google_ads-abc-123.parquet"
EXPECTED_GCS_URI = f"gs://{BUCKET}/{EXPECTED_BLOB_PATH}"


@pytest.fixture
def config():
    """Sample GCSConfig."""
    return GCSConfig(
        bucket=BUCKET,
        source=SOURCE,
        partition_date=PARTITION_DATE,
        run_id=RUN_ID,
    )


@pytest.fixture
def sample_table():
    """Sample PyArrow table."""
    return pa.table(
        {
            "date": ["2024-01-15"],
            "clicks": [10],
            "impressions": [100],
        }
    )


@pytest.fixture
def mock_client():
    """Mocked GCS client."""
    return MagicMock(spec=storage.Client)


def test_serialize_returns_bytes(sample_table):
    """Returns bytes."""
    result = _serialize(sample_table)

    assert isinstance(result, bytes)


def test_serialize_produces_valid_parquet(sample_table):
    """Serialized bytes can be read back as a valid parquet table."""
    result = _serialize(sample_table)

    buffer = io.BytesIO(result)
    recovered = pq.read_table(buffer)
    assert recovered.equals(sample_table)


def test_serialize_uses_snappy_compression(sample_table):
    """Serialized parquet uses snappy compression."""
    result = _serialize(sample_table)

    buffer = io.BytesIO(result)
    pf = pq.ParquetFile(buffer)
    compression = pf.metadata.row_group(0).column(0).compression

    assert compression == "SNAPPY"


def test_upload_returns_gcs_uri(mock_client):
    """Returns correct GCS URI."""
    result = _upload(mock_client, BUCKET, EXPECTED_BLOB_PATH, b"data")

    assert result == EXPECTED_GCS_URI


def test_upload_calls_bucket_with_correct_name(mock_client):
    """GCS client bucket called with correct bucket name."""
    _upload(mock_client, BUCKET, EXPECTED_BLOB_PATH, b"data")

    mock_client.bucket.assert_called_once_with(BUCKET)


def test_upload_calls_blob_with_correct_path(mock_client):
    """Bucket blob called with correct blob path."""
    _upload(mock_client, BUCKET, EXPECTED_BLOB_PATH, b"data")

    mock_client.bucket.return_value.blob.assert_called_once_with(EXPECTED_BLOB_PATH)


def test_upload_calls_upload_from_string_with_correct_content_type(mock_client):
    """upload_from_string called with correct content type."""
    _upload(mock_client, BUCKET, EXPECTED_BLOB_PATH, b"data")

    mock_client.bucket.return_value.blob.return_value.upload_from_string.assert_called_once_with(
        b"data",
        content_type="application/octet-stream",
    )


def test_load_returns_gcs_uri(mock_client, config, sample_table):
    """Returns correct GCS URI."""
    result = load(sample_table, config, mock_client)

    assert result == EXPECTED_GCS_URI


def test_load_calls_serialize_and_upload(mock_client, config, sample_table):
    """load() composes _serialize and _upload correctly."""
    with (
        patch("load.gcs.load._serialize") as mock_serialize,
        patch("load.gcs.load._upload") as mock_upload,
    ):
        mock_serialize.return_value = b"parquet_bytes"
        mock_upload.return_value = EXPECTED_GCS_URI

        result = load(sample_table, config, mock_client)

        mock_serialize.assert_called_once_with(sample_table)
        mock_upload.assert_called_once_with(
            mock_client,
            BUCKET,
            EXPECTED_BLOB_PATH,
            b"parquet_bytes",
        )
        assert result == EXPECTED_GCS_URI


def test_load_blob_path_uses_config_fields(mock_client, config, sample_table):
    """Blob path is built from config source, date, and run_id."""
    load(sample_table, config, mock_client)

    mock_client.bucket.return_value.blob.assert_called_once_with(EXPECTED_BLOB_PATH)


def test_load_empty_table(mock_client, config):
    """Empty table serializes and uploads without error."""
    empty_table = pa.table({"date": [], "clicks": []})

    result = load(empty_table, config, mock_client)

    assert result == EXPECTED_GCS_URI
