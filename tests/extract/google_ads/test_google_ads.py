"""Tests for Google Ads extraction."""

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pydantic import ValidationError

from extract.google_ads.extract import (
    Raw,
    Record,
    _flatten_row,
    _to_raw,
    extract,
    fetch,
    parse,
)
from extract.table import to_table

CUSTOMER_ID = "1234567890"
QUERY = "SELECT segments.date, metrics.clicks FROM customer"
EXPECTED_ROW_COUNT = 2
EXPECTED_COLUMN_COUNT = 6
EXPECTED_CLICKS = 10
EXPECTED_IMPRESSIONS = 100
EXPECTED_COST_MICROS = 1500000
EXPECTED_CONVERSIONS = 2.0

RAW_ROW_1 = Raw(
    date="2024-01-15",
    clicks="10",
    impressions="100",
    cost_micros="1500000",
    conversions="2.0",
    customer_id=CUSTOMER_ID,
)
RAW_ROW_2 = Raw(
    date="2024-01-16",
    clicks="20",
    impressions="200",
    cost_micros="3000000",
    conversions="4.0",
    customer_id=CUSTOMER_ID,
)

FLAT_DICT_1 = {
    "segments.date": "2024-01-15",
    "metrics.clicks": "10",
    "metrics.impressions": "100",
    "metrics.costMicros": "1500000",
    "metrics.conversions": "2.0",
}
NESTED_DICT_1 = {
    "segments": {"date": "2024-01-15"},
    "metrics": {
        "clicks": "10",
        "impressions": "100",
        "costMicros": "1500000",
        "conversions": "2.0",
    },
}


@pytest.fixture
def raw_rows():
    """Sample Raw rows."""
    return [RAW_ROW_1, RAW_ROW_2]


@pytest.fixture
def records(raw_rows):
    """Parsed Records from sample raw rows."""
    return [parse(r) for r in raw_rows]


@pytest.fixture
def mock_client():
    """Mocked Google Ads API client."""
    client = MagicMock()
    mock_row_1 = MagicMock()
    mock_row_2 = MagicMock()
    client.get_service.return_value.search.return_value = [mock_row_1, mock_row_2]
    return client


def test_flatten_row_flattens_nested_dict():
    """Nested dict keys become dot-notation keys."""
    result = _flatten_row(NESTED_DICT_1)

    assert result == FLAT_DICT_1


def test_flatten_row_preserves_top_level_keys():
    """Top-level non-dict values are preserved as-is."""
    input_dict = {"customer_id": "123", "segments": {"date": "2024-01-15"}}
    result = _flatten_row(input_dict)

    assert result["customer_id"] == "123"
    assert result["segments.date"] == "2024-01-15"


def test_flatten_row_empty_dict():
    """Empty dict returns empty dict."""
    result = _flatten_row({})

    assert result == {}


def test_flatten_row_raises_on_nested_non_scalar():
    """Nested dict value raises ValueError."""
    nested = {"metrics": {"nested_field": {"inner": "value"}}}

    with pytest.raises(ValueError, match="Expected scalar value"):
        _flatten_row(nested)


def test_to_raw_returns_raw_instance():
    """Returns a Raw instance."""
    result = _to_raw(FLAT_DICT_1, CUSTOMER_ID)

    assert isinstance(result, Raw)


def test_to_raw_maps_fields_correctly():
    """All fields mapped correctly from flattened dict."""
    result = _to_raw(FLAT_DICT_1, CUSTOMER_ID)

    assert result.date == "2024-01-15"
    assert result.clicks == "10"
    assert result.impressions == "100"
    assert result.cost_micros == "1500000"
    assert result.conversions == "2.0"
    assert result.customer_id == CUSTOMER_ID


def test_to_raw_missing_key_raises():
    """Missing expected key raises KeyError."""
    incomplete_dict = {"segments.date": "2024-01-15"}

    with pytest.raises(KeyError):
        _to_raw(incomplete_dict, CUSTOMER_ID)


def test_extract_returns_pyarrow_table(mock_client):
    """Returns a pa.Table instance."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = extract(mock_client, CUSTOMER_ID, QUERY)

    assert isinstance(result, pa.Table)


def test_extract_row_count(mock_client):
    """Table has correct number of rows."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = extract(mock_client, CUSTOMER_ID, QUERY)

    assert result.num_rows == EXPECTED_ROW_COUNT


def test_extract_column_count(mock_client):
    """Table has correct number of columns."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = extract(mock_client, CUSTOMER_ID, QUERY)

    assert result.num_columns == EXPECTED_COLUMN_COUNT


def test_extract_column_names(mock_client):
    """Table contains expected column names."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = extract(mock_client, CUSTOMER_ID, QUERY)

    assert set(result.column_names) == {
        "date",
        "clicks",
        "impressions",
        "cost_micros",
        "conversions",
        "customer_id",
    }


def test_extract_composes_fetch_parse_to_table(mock_client):
    """Result matches manually composing fetch, parse, and to_table."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        raw_rows = fetch(mock_client, CUSTOMER_ID, QUERY)
        parsed = [parse(r) for r in raw_rows]
        expected = to_table(parsed)

    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = extract(mock_client, CUSTOMER_ID, QUERY)

    assert result.equals(expected)


def test_fetch_returns_list_of_raw(mock_client):
    """Returns a list of Raw instances."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = fetch(mock_client, CUSTOMER_ID, QUERY)

    assert all(isinstance(r, Raw) for r in result)


def test_fetch_calls_google_ads_service(mock_client):
    """GoogleAdsService search is called with correct args."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        fetch(mock_client, CUSTOMER_ID, QUERY)

    mock_client.get_service.assert_called_once_with("GoogleAdsService")
    mock_client.get_service.return_value.search.assert_called_once_with(
        customer_id=CUSTOMER_ID,
        query=QUERY,
    )


def test_fetch_row_count(mock_client):
    """Returns correct number of rows."""
    with patch(
        "extract.google_ads.extract.MessageToDict",
        return_value=NESTED_DICT_1,
    ):
        result = fetch(mock_client, CUSTOMER_ID, QUERY)

    assert len(result) == EXPECTED_ROW_COUNT


def test_parse_returns_record(raw_rows):
    """Returns a Record instance."""
    result = parse(raw_rows[0])

    assert isinstance(result, Record)


def test_parse_date_converted_to_date_type(raw_rows):
    """Date string is parsed to date object."""
    result = parse(raw_rows[0])

    assert result.date == date(2024, 1, 15)


def test_parse_clicks_converted_to_int(raw_rows):
    """Clicks string is parsed to int."""
    result = parse(raw_rows[0])

    assert result.clicks == EXPECTED_CLICKS
    assert isinstance(result.clicks, int)


def test_parse_impressions_converted_to_int(raw_rows):
    """Impressions string is parsed to int."""
    result = parse(raw_rows[0])

    assert result.impressions == EXPECTED_IMPRESSIONS
    assert isinstance(result.impressions, int)


def test_parse_cost_micros_converted_to_int(raw_rows):
    """Cost micros string is parsed to int."""
    result = parse(raw_rows[0])

    assert result.cost_micros == EXPECTED_COST_MICROS
    assert isinstance(result.cost_micros, int)


def test_parse_conversions_converted_to_float(raw_rows):
    """Conversions string is parsed to float."""
    result = parse(raw_rows[0])

    assert result.conversions == EXPECTED_CONVERSIONS
    assert isinstance(result.conversions, float)


def test_parse_customer_id_preserved(raw_rows):
    """Customer ID is preserved as string."""
    result = parse(raw_rows[0])

    assert result.customer_id == CUSTOMER_ID


def test_parse_record_is_immutable(raw_rows):
    """Record instances cannot be mutated."""
    result = parse(raw_rows[0])

    with pytest.raises(ValidationError):
        result.clicks = 999


def test_to_table_returns_pyarrow_table(records):
    """Returns a pa.Table instance."""
    result = to_table(records)

    assert isinstance(result, pa.Table)


def test_to_table_row_count(records):
    """Table has one row per record."""
    result = to_table(records)

    assert result.num_rows == EXPECTED_ROW_COUNT


def test_to_table_column_count(records):
    """Table has correct number of columns."""
    result = to_table(records)

    assert result.num_columns == EXPECTED_COLUMN_COUNT


def test_to_table_date_values_correct(records):
    """Date column contains correct ISO format values."""
    result = to_table(records)

    assert result.column("date").to_pylist() == ["2024-01-15", "2024-01-16"]


def test_to_table_clicks_values_correct(records):
    """Clicks column contains correct integer values."""
    result = to_table(records)

    assert result.column("clicks").to_pylist() == [EXPECTED_CLICKS, 20]


def test_to_table_cost_micros_values_correct(records):
    """Cost micros column contains correct integer values."""
    result = to_table(records)

    assert result.column("cost_micros").to_pylist() == [EXPECTED_COST_MICROS, 3000000]


def test_to_table_empty_records_returns_empty_table():
    """Empty records list returns empty table."""
    result = to_table([])

    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_raw_is_immutable():
    """Raw instances cannot be mutated."""
    with pytest.raises(ValidationError):
        RAW_ROW_1.date = "2024-01-16"


def test_record_date_validator_accepts_date_object():
    """Date validator passes through an existing date object unchanged."""
    existing_date = date(2024, 1, 15)
    record = Record(
        date=existing_date,
        clicks=10,
        impressions=100,
        cost_micros=1500000,
        conversions=2.0,
        customer_id=CUSTOMER_ID,
    )

    assert record.date == existing_date


def test_record_invalid_date_raises():
    """Invalid date string raises ValidationError."""
    with pytest.raises(ValidationError):
        Record(
            date="not-a-date",  # ty: ignore[invalid-argument-type]
            clicks=10,
            impressions=100,
            cost_micros=1500000,
            conversions=2.0,
            customer_id=CUSTOMER_ID,
        )


def test_record_invalid_clicks_raises():
    """Non-integer clicks raises ValidationError."""
    with pytest.raises(ValidationError):
        Record(
            date="2024-01-15",  # ty: ignore[invalid-argument-type]
            clicks="not-an-int",  # ty: ignore[invalid-argument-type]
            impressions=100,
            cost_micros=1500000,
            conversions=2.0,
            customer_id=CUSTOMER_ID,
        )
