"""Tests for Google Analytics extraction."""

from datetime import date
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pydantic import ValidationError

from extract.google_analytics.extract import (
    Raw,
    Record,
    ReportConfig,
    _build_request,
    _parse_response,
    extract,
    fetch,
    parse,
    to_table,
)

PROPERTY_ID = "123456"
START_DATE = "2024-01-01"
END_DATE = "2024-01-31"
EXPECTED_ROW_COUNT = 2
EXPECTED_COLUMN_COUNT = 4


@pytest.fixture
def config():
    """ReportConfig with sample dimensions and metrics."""
    return ReportConfig(
        dimension_names=["date", "country"],
        metric_names=["sessions", "pageviews"],
    )


@pytest.fixture
def mock_client():
    """Mocked GA4 API client."""
    client = MagicMock()

    mock_row_1 = MagicMock()
    mock_row_1.dimension_values = [
        MagicMock(value="20240101"),
        MagicMock(value="Japan"),
    ]
    mock_row_1.metric_values = [
        MagicMock(value="100"),
        MagicMock(value="500"),
    ]

    mock_row_2 = MagicMock()
    mock_row_2.dimension_values = [
        MagicMock(value="20240102"),
        MagicMock(value="USA"),
    ]
    mock_row_2.metric_values = [
        MagicMock(value="200"),
        MagicMock(value="700"),
    ]

    client.run_report.return_value.rows = [mock_row_1, mock_row_2]
    return client


@pytest.fixture
def raw_rows():
    """Sample Raw rows."""
    return [
        Raw(date="20240101", dimensions=["20240101", "Japan"], metrics=["100", "500"]),
        Raw(date="20240102", dimensions=["20240102", "USA"], metrics=["200", "700"]),
    ]


@pytest.fixture
def records(raw_rows, config):
    """Parsed Records from sample raw rows."""
    return [parse(r, config) for r in raw_rows]


def test_build_request_sets_property():
    """Property ID is prefixed with 'properties/'."""
    cfg = ReportConfig(
        dimension_names=["date", "country"],
        metric_names=["sessions", "pageviews"],
    )
    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, cfg)

    assert request.property == f"properties/{PROPERTY_ID}"


def test_build_request_sets_date_range():
    """Date range is set correctly."""
    cfg = ReportConfig(
        dimension_names=["date", "country"],
        metric_names=["sessions", "pageviews"],
    )
    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, cfg)

    assert request.date_ranges[0].start_date == START_DATE
    assert request.date_ranges[0].end_date == END_DATE


def test_build_request_sets_dimensions(config):
    """Dimension names are wrapped in Dimension objects."""
    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, config)

    assert [d.name for d in request.dimensions] == config.dimension_names


def test_build_request_sets_metrics(config):
    """Metric names are wrapped in Metric objects."""
    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, config)

    assert [m.name for m in request.metrics] == config.metric_names


def test_parse_response_returns_raw_list(mock_client, config):
    """Returns a list of Raw instances."""
    result = _parse_response(mock_client.run_report.return_value, config)

    assert all(isinstance(r, Raw) for r in result)


def test_parse_response_row_count(mock_client, config):
    """Returns one Raw per response row."""
    result = _parse_response(mock_client.run_report.return_value, config)

    assert len(result) == EXPECTED_ROW_COUNT


def test_parse_response_extracts_date(mock_client, config):
    """Date extracted correctly from dimension values."""
    result = _parse_response(mock_client.run_report.return_value, config)

    assert result[0].date == "20240101"
    assert result[1].date == "20240102"


def test_extract_returns_pyarrow_table(mock_client, config):
    """Returns a pa.Table instance."""
    result = extract(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    assert isinstance(result, pa.Table)


def test_extract_row_count(mock_client, config):
    """Table has correct number of rows."""
    result = extract(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    assert result.num_rows == EXPECTED_ROW_COUNT


def test_extract_column_names(mock_client, config):
    """Table contains expected column names."""
    result = extract(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    assert "date" in result.column_names
    assert "country" in result.column_names
    assert "sessions" in result.column_names
    assert "pageviews" in result.column_names


def test_extract_composes_fetch_parse_to_table(mock_client, config):
    """Result matches manually composing fetch, parse, and to_table."""
    raw_rows = fetch(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)
    parsed = [parse(r, config) for r in raw_rows]
    expected = to_table(parsed, config)

    result = extract(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    assert result.equals(expected)


def test_fetch_returns_list_of_raw(mock_client, config):
    """Returns a list of Raw instances."""
    result = fetch(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    assert all(isinstance(r, Raw) for r in result)


def test_fetch_calls_api(mock_client, config):
    """GA4 API run_report is called once."""
    fetch(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    mock_client.run_report.assert_called_once()


def test_fetch_row_count(mock_client, config):
    """Returns correct number of rows."""
    result = fetch(mock_client, PROPERTY_ID, START_DATE, END_DATE, config)

    assert len(result) == EXPECTED_ROW_COUNT


def test_parse_returns_record(raw_rows, config):
    """Returns a Record instance."""
    result = parse(raw_rows[0], config)

    assert isinstance(result, Record)


def test_parse_date_converted_to_date_type(raw_rows, config):
    """Date string YYYYMMDD is parsed to date object."""
    result = parse(raw_rows[0], config)

    assert result.date == date(2024, 1, 1)


def test_parse_dimensions_mapped_correctly(raw_rows, config):
    """Dimension values mapped to dimension names."""
    result = parse(raw_rows[0], config)

    assert result.dimensions == {"date": "20240101", "country": "Japan"}


def test_parse_metrics_mapped_correctly(raw_rows, config):
    """Metric values mapped to metric names."""
    result = parse(raw_rows[0], config)

    assert result.metrics == {"sessions": "100", "pageviews": "500"}


def test_parse_record_is_immutable(raw_rows, config):
    """Record instances cannot be mutated."""
    result = parse(raw_rows[0], config)

    with pytest.raises(ValidationError):
        result.date = date(2025, 1, 1)


def test_to_table_returns_pyarrow_table(records, config):
    """Returns a pa.Table instance."""
    result = to_table(records, config)

    assert isinstance(result, pa.Table)


def test_to_table_row_count(records, config):
    """Table has one row per record."""
    result = to_table(records, config)

    assert result.num_rows == EXPECTED_ROW_COUNT


def test_to_table_column_count(records, config):
    """Table has correct number of columns."""
    result = to_table(records, config)

    assert result.num_columns == EXPECTED_COLUMN_COUNT


def test_to_table_contains_date_column(records, config):
    """Table contains a date column."""
    result = to_table(records, config)

    assert "date" in result.column_names


def test_to_table_date_values_correct(records, config):
    """Date column contains correct ISO format values."""
    result = to_table(records, config)

    assert result.column("date").to_pylist() == ["2024-01-01", "2024-01-02"]


def test_to_table_metric_values_correct(records, config):
    """Metric columns contain correct values."""
    result = to_table(records, config)

    assert result.column("sessions").to_pylist() == ["100", "200"]
    assert result.column("pageviews").to_pylist() == ["500", "700"]


def test_to_table_empty_records_returns_empty_table(config):
    """Empty records list returns empty table."""
    result = to_table([], config)

    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_raw_is_immutable():
    """Raw instances cannot be mutated."""
    raw = Raw(date="20240101", dimensions=["20240101", "Japan"], metrics=["100", "500"])

    with pytest.raises(ValidationError):
        raw.date = "20240102"


def test_record_date_validator_accepts_date_object():
    """Date validator passes through an existing date object unchanged."""
    existing_date = date(2024, 1, 1)
    record = Record(
        date=existing_date,
        dimensions={"date": "20240101", "country": "Japan"},
        metrics={"sessions": "100", "pageviews": "500"},
    )

    assert record.date == existing_date


def test_record_date_validator_parses_yyyymmdd_string():
    """Date validator correctly parses YYYYMMDD string."""
    record = Record(
        date="20240115",  # ty: ignore[invalid-argument-type]
        dimensions={"date": "20240115", "country": "Japan"},
        metrics={"sessions": "100", "pageviews": "500"},
    )

    assert record.date == date(2024, 1, 15)
