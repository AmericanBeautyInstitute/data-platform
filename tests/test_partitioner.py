"""Tests for GCS partition path builder."""

from datetime import date

from load.gcs.partition import build_gcs_blob_path

SOURCE = "google_ads"
RUN_ID = "abc-123"
PARTITION_DATE = date(2024, 1, 15)
EXPECTED_PATH = "google_ads/date=2024-01-15/google_ads-abc-123.parquet"


def test_build_gcs_blob_path_returns_string():
    """Returns a string."""
    result = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert isinstance(result, str)


def test_build_gcs_blob_path_correct_format():
    """Path matches expected Hive-style format."""
    result = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert result == EXPECTED_PATH


def test_build_gcs_blob_path_contains_source():
    """Path contains source name in both directory and filename."""
    result = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert result.startswith(SOURCE)
    assert f"{SOURCE}-{RUN_ID}.parquet" in result


def test_build_gcs_blob_path_contains_hive_date_prefix():
    """Path contains Hive-compatible date= prefix."""
    result = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert "date=2024-01-15" in result


def test_build_gcs_blob_path_contains_run_id():
    """Path contains run ID in filename."""
    result = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert RUN_ID in result


def test_build_gcs_blob_path_parquet_extension():
    """Path ends with .parquet extension."""
    result = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert result.endswith(".parquet")


def test_build_gcs_blob_path_different_sources_produce_different_paths():
    """Different sources produce distinct paths."""
    path_a = build_gcs_blob_path("google_ads", PARTITION_DATE, RUN_ID)
    path_b = build_gcs_blob_path("google_sheets", PARTITION_DATE, RUN_ID)

    assert path_a != path_b


def test_build_gcs_blob_path_different_dates_produce_different_paths():
    """Different partition dates produce distinct paths."""
    path_a = build_gcs_blob_path(SOURCE, date(2024, 1, 15), RUN_ID)
    path_b = build_gcs_blob_path(SOURCE, date(2024, 1, 16), RUN_ID)

    assert path_a != path_b


def test_build_gcs_blob_path_different_run_ids_produce_different_paths():
    """Different run IDs produce distinct paths."""
    path_a = build_gcs_blob_path(SOURCE, PARTITION_DATE, "run-1")
    path_b = build_gcs_blob_path(SOURCE, PARTITION_DATE, "run-2")

    assert path_a != path_b


def test_build_gcs_blob_path_is_deterministic():
    """Same inputs always produce the same path."""
    path_a = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)
    path_b = build_gcs_blob_path(SOURCE, PARTITION_DATE, RUN_ID)

    assert path_a == path_b
