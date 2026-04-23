"""Tests for Google Sheets extraction."""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pydantic import ValidationError

from extract.google_sheets.extract import Raw, Record, extract, fetch, parse


def to_table(records: list[Record]) -> pa.Table:
    """Replicates the old to_table for test assertions."""
    return pa.Table.from_pylist([record.data for record in records])


SPREADSHEET_ID = "test-spreadsheet-id"
SHEET_NAME = "Sheet1"
HEADERS = ["name", "age", "city"]
ROWS = [
    ["Alice", "25", "Tokyo"],
    ["Bob", "30", "New York"],
    ["Relena", "35", "Paris"],
]
EXPECTED_ROW_COUNT = 3
EXPECTED_COLUMN_COUNT = 3


@pytest.fixture
def mock_client():
    """Mocked Google Sheets API client."""
    client = MagicMock()
    client.spreadsheets().values().get().execute.return_value = {
        "values": [HEADERS, *ROWS]
    }
    return client


@pytest.fixture
def raw():
    """A Raw instance with sample sheet data."""
    return Raw(headers=HEADERS, rows=ROWS)


@pytest.fixture
def records(raw):
    """Parsed Records from sample raw data."""
    return parse(raw)


def test_extract_column_names_correct(mock_client):
    """Table has correct column names."""
    result = extract(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert result.column_names == HEADERS


def test_extract_composes_fetch_parse_to_table(mock_client):
    """Result matches manually composing fetch, parse, and to_table."""
    raw = fetch(mock_client, SPREADSHEET_ID, SHEET_NAME)
    records = parse(raw)
    expected = to_table(records)

    mock_client.spreadsheets().values().get().execute.return_value = {
        "values": [HEADERS, *ROWS]
    }
    result = extract(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert result.equals(expected)


def test_extract_returns_pyarrow_table(mock_client):
    """Returns a pa.Table instance."""
    result = extract(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert isinstance(result, pa.Table)


def test_extract_row_count_correct(mock_client):
    """Table has correct number of rows."""
    result = extract(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert result.num_rows == EXPECTED_ROW_COUNT


def test_fetch_calls_api_with_correct_args(mock_client):
    """API called with correct spreadsheet ID and range."""
    fetch(mock_client, SPREADSHEET_ID, SHEET_NAME)

    mock_client.spreadsheets().values().get.assert_called_with(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME,
    )


def test_fetch_empty_sheet_raises(mock_client):
    """Empty sheet raises ValueError with spreadsheet and sheet context."""
    mock_client.spreadsheets().values().get().execute.return_value = {"values": []}

    with pytest.raises(ValueError, match=SHEET_NAME):
        fetch(mock_client, SPREADSHEET_ID, SHEET_NAME)


def test_fetch_headers_extracted_correctly(mock_client):
    """First row becomes headers."""
    result = fetch(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert result.headers == HEADERS


def test_fetch_returns_raw_instance(mock_client):
    """Returns a Raw instance."""
    result = fetch(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert isinstance(result, Raw)


def test_fetch_rows_exclude_header(mock_client):
    """Rows do not include the header row."""
    result = fetch(mock_client, SPREADSHEET_ID, SHEET_NAME)

    assert result.rows == ROWS


def test_parse_empty_rows_returns_empty_list():
    """Empty rows produces empty list."""
    raw = Raw(headers=HEADERS, rows=[])

    result = parse(raw)

    assert result == []


def test_parse_record_count_matches_rows(raw):
    """One Record produced per row."""
    result = parse(raw)

    assert len(result) == EXPECTED_ROW_COUNT


def test_parse_record_data_maps_headers_to_values(raw):
    """Record data correctly maps headers to row values."""
    result = parse(raw)

    assert result[0].data == {"name": "Alice", "age": "25", "city": "Tokyo"}
    assert result[1].data == {"name": "Bob", "age": "30", "city": "New York"}
    assert result[2].data == {"name": "Relena", "age": "35", "city": "Paris"}


def test_parse_records_are_immutable(raw):
    """Record instances cannot be mutated."""
    result = parse(raw)

    with pytest.raises(ValidationError):
        result[0].data = {}


def test_parse_returns_list_of_records(raw):
    """Returns a list of Record instances."""
    result = parse(raw)

    assert all(isinstance(r, Record) for r in result)


def test_to_table_column_count_matches_headers(records):
    """Table has one column per header."""
    result = to_table(records)

    assert result.num_columns == EXPECTED_COLUMN_COUNT


def test_to_table_column_names_match_headers(records):
    """Column names match sheet headers."""
    result = to_table(records)

    assert result.column_names == HEADERS


def test_to_table_column_values_correct(records):
    """Column values match original sheet data."""
    result = to_table(records)

    assert result.column("name").to_pylist() == ["Alice", "Bob", "Relena"]
    assert result.column("age").to_pylist() == ["25", "30", "35"]
    assert result.column("city").to_pylist() == ["Tokyo", "New York", "Paris"]


def test_to_table_empty_records_returns_empty_table():
    """Empty records list returns empty table."""
    result = to_table([])

    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_to_table_returns_pyarrow_table(records):
    """Returns a pa.Table instance."""
    result = to_table(records)

    assert isinstance(result, pa.Table)


def test_to_table_row_count_matches_records(records):
    """Table has one row per record."""
    result = to_table(records)

    assert result.num_rows == EXPECTED_ROW_COUNT
