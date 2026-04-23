"""Tests for PayPal transaction extraction."""

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pydantic import ValidationError

from extract.paypal.extract import (
    Raw,
    Record,
    _parse_transaction,
    extract,
    fetch,
    parse,
)
from extract.table import to_table

START_DATE = date(2024, 1, 15)
END_DATE = date(2024, 1, 15)
START_DATE_STR = "2024-01-15"
TRANSACTION_ID = "TXN123456"
EXPECTED_ROW_COUNT = 2
EXPECTED_COLUMN_COUNT = 10
EXPECTED_GROSS = 150.00
EXPECTED_FEE = -4.65
EXPECTED_NET = 145.35
EXPECTED_STATUS = "S"
EXPECTED_SUBJECT = "Cosmetology Program Enrollment"
EXPECTED_EMAIL = "student@example.com"
EXPECTED_NAME = "Alice Smith"

API_TRANSACTION_1 = {
    "transaction_info": {
        "transaction_id": TRANSACTION_ID,
        "paypal_reference_id": TRANSACTION_ID,
        "transaction_initiation_date": "2024-01-15T10:30:00+0000",
        "transaction_amount": {"value": "150.00", "currency_code": "USD"},
        "transaction_status": EXPECTED_STATUS,
        "transaction_subject": EXPECTED_SUBJECT,
        "fee_amount": {"value": "-4.65", "currency_code": "USD"},
        "transaction_net_amount": {"value": "145.35", "currency_code": "USD"},
    },
    "payer_info": {
        "email_address": EXPECTED_EMAIL,
        "payer_name": {"given_name": "Alice", "surname": "Smith"},
    },
}

API_TRANSACTION_2 = {
    "transaction_info": {
        "transaction_id": "TXN789012",
        "paypal_reference_id": "TXN789012",
        "transaction_initiation_date": "2024-01-15T14:00:00+0000",
        "transaction_amount": {"value": "75.00", "currency_code": "USD"},
        "transaction_status": "S",
        "transaction_subject": "Inventory Purchase",
        "fee_amount": {"value": "-2.48", "currency_code": "USD"},
        "transaction_net_amount": {"value": "72.52", "currency_code": "USD"},
    },
    "payer_info": {
        "email_address": "vendor@example.com",
        "payer_name": {"given_name": "Bob", "surname": "Jones"},
    },
}

RAW_ROW_1 = Raw(
    transaction_id=TRANSACTION_ID,
    transaction_date="2024-01-15T10:30:00+0000",
    transaction_amount="150.00",
    currency_code="USD",
    transaction_status=EXPECTED_STATUS,
    transaction_subject=EXPECTED_SUBJECT,
    payer_email=EXPECTED_EMAIL,
    payer_name=EXPECTED_NAME,
    fee_amount="-4.65",
    net_amount="145.35",
)

RAW_ROW_2 = Raw(
    transaction_id="TXN789012",
    transaction_date="2024-01-15T14:00:00+0000",
    transaction_amount="75.00",
    currency_code="USD",
    transaction_status="S",
    transaction_subject="Inventory Purchase",
    payer_email="vendor@example.com",
    payer_name="Bob Jones",
    fee_amount="-2.48",
    net_amount="72.52",
)


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
    """Mocked PayPal API client returning single page of transactions."""
    client = MagicMock()
    client.get.return_value = {
        "transaction_details": [API_TRANSACTION_1, API_TRANSACTION_2],
        "total_pages": 1,
    }
    return client


def test_extract_composes_fetch_parse_to_table(raw_rows):
    """Result matches manually composing fetch, parse, and to_table."""
    parsed = [parse(r) for r in raw_rows]
    expected = to_table(parsed)

    with patch("extract.paypal.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)

    assert result.equals(expected)


def test_extract_returns_pyarrow_table(raw_rows):
    """Returns a pa.Table instance."""
    with patch("extract.paypal.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)
    assert isinstance(result, pa.Table)


def test_extract_row_count(raw_rows):
    """Table has correct number of rows."""
    with patch("extract.paypal.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)
    assert result.num_rows == EXPECTED_ROW_COUNT


def test_fetch_calls_api_with_correct_date_range(mock_client):
    """API called with correct start and end date params."""
    fetch(mock_client, START_DATE, END_DATE)
    call_params = mock_client.get.call_args[1]["params"]
    assert START_DATE_STR in call_params["start_date"]
    assert START_DATE_STR in call_params["end_date"]


def test_fetch_empty_response_returns_empty_list():
    """Empty transaction_details returns empty list."""
    client = MagicMock()
    client.get.return_value = {"transaction_details": [], "total_pages": 1}
    result = fetch(client, START_DATE, END_DATE)
    assert result == []


def test_fetch_paginates_multiple_pages():
    """Fetches all pages when total_pages > 1."""
    client = MagicMock()
    client.get.side_effect = [
        {
            "transaction_details": [API_TRANSACTION_1],
            "total_pages": 2,
        },
        {
            "transaction_details": [API_TRANSACTION_2],
            "total_pages": 2,
        },
    ]
    result = fetch(client, START_DATE, END_DATE)
    assert len(result) == EXPECTED_ROW_COUNT
    assert client.get.call_count == EXPECTED_ROW_COUNT


def test_fetch_returns_list_of_raw(mock_client):
    """Returns a list of Raw instances."""
    result = fetch(mock_client, START_DATE, END_DATE)
    assert all(isinstance(r, Raw) for r in result)


def test_fetch_row_count(mock_client):
    """Returns correct number of rows for single page."""
    result = fetch(mock_client, START_DATE, END_DATE)
    assert len(result) == EXPECTED_ROW_COUNT


def test_parse_date_strips_time_component(raw_rows):
    """Date is parsed correctly from ISO 8601 datetime string."""
    result = parse(raw_rows[0])
    assert result.transaction_date == date(2024, 1, 15)


def test_parse_fee_amount_converted_to_float(raw_rows):
    """Fee amount string is parsed to float."""
    result = parse(raw_rows[0])
    assert result.fee_amount_usd == EXPECTED_FEE
    assert isinstance(result.fee_amount_usd, float)


def test_parse_gross_amount_converted_to_float(raw_rows):
    """Gross amount string is parsed to float."""
    result = parse(raw_rows[0])
    assert result.gross_amount_usd == EXPECTED_GROSS
    assert isinstance(result.gross_amount_usd, float)


def test_parse_net_amount_converted_to_float(raw_rows):
    """Net amount string is parsed to float."""
    result = parse(raw_rows[0])
    assert result.net_amount_usd == EXPECTED_NET
    assert isinstance(result.net_amount_usd, float)


def test_parse_record_is_immutable(raw_rows):
    """Record instances cannot be mutated."""
    result = parse(raw_rows[0])
    with pytest.raises(ValidationError):
        result.transaction_id = "NEW_ID"


def test_parse_returns_record(raw_rows):
    """Returns a Record instance."""
    result = parse(raw_rows[0])
    assert isinstance(result, Record)


def test_parse_transaction_maps_amount():
    """Transaction amount mapped correctly."""
    result = _parse_transaction(API_TRANSACTION_1)
    assert result.transaction_amount == "150.00"


def test_parse_transaction_maps_payer_email():
    """Payer email mapped correctly."""
    result = _parse_transaction(API_TRANSACTION_1)
    assert result.payer_email == EXPECTED_EMAIL


def test_parse_transaction_maps_payer_full_name():
    """Payer first and last name joined correctly."""
    result = _parse_transaction(API_TRANSACTION_1)
    assert result.payer_name == EXPECTED_NAME


def test_parse_transaction_maps_transaction_id():
    """Transaction ID mapped correctly."""
    result = _parse_transaction(API_TRANSACTION_1)
    assert result.transaction_id == TRANSACTION_ID


def test_parse_transaction_missing_required_field_raises():
    """Missing transaction_info raises KeyError."""
    with pytest.raises(KeyError):
        _parse_transaction({})


def test_parse_transaction_optional_fields_default():
    """Genuinely optional fields default when absent."""
    minimal_txn = {
        "transaction_info": {
            "transaction_id": TRANSACTION_ID,
            "transaction_initiation_date": "2024-01-15T10:30:00+0000",
            "transaction_amount": {"value": "150.00", "currency_code": "USD"},
            "transaction_status": EXPECTED_STATUS,
        },
    }
    result = _parse_transaction(minimal_txn)
    assert result.transaction_subject == ""
    assert result.payer_email == ""
    assert result.payer_name == ""
    assert result.fee_amount == "0"
    assert result.net_amount == "0"


def test_parse_transaction_returns_raw_instance():
    """Returns a Raw instance from an API transaction dict."""
    result = _parse_transaction(API_TRANSACTION_1)
    assert isinstance(result, Raw)


def test_raw_is_immutable():
    """Raw instances cannot be mutated."""
    with pytest.raises(ValidationError):
        RAW_ROW_1.transaction_id = "NEW_ID"


def test_record_date_validator_accepts_date_object():
    """Date validator passes through an existing date object unchanged."""
    existing_date = date(2024, 1, 15)
    record = Record(
        transaction_id=TRANSACTION_ID,
        transaction_date=existing_date,
        gross_amount_usd=EXPECTED_GROSS,
        currency_code="USD",
        transaction_status=EXPECTED_STATUS,
        transaction_subject=EXPECTED_SUBJECT,
        payer_email=EXPECTED_EMAIL,
        payer_name=EXPECTED_NAME,
        fee_amount_usd=EXPECTED_FEE,
        net_amount_usd=EXPECTED_NET,
    )
    assert record.transaction_date == existing_date


def test_record_invalid_date_raises():
    """Invalid date string raises ValidationError."""
    with pytest.raises(ValidationError):
        Record(
            transaction_id=TRANSACTION_ID,
            transaction_date="not-a-date",  # ty: ignore[invalid-argument-type]
            gross_amount_usd=EXPECTED_GROSS,
            currency_code="USD",
            transaction_status=EXPECTED_STATUS,
            transaction_subject=EXPECTED_SUBJECT,
            payer_email=EXPECTED_EMAIL,
            payer_name=EXPECTED_NAME,
            fee_amount_usd=EXPECTED_FEE,
            net_amount_usd=EXPECTED_NET,
        )


def test_to_table_column_count(records):
    """Table has correct number of columns."""
    result = to_table(records)
    assert result.num_columns == EXPECTED_COLUMN_COUNT


def test_to_table_column_names(records):
    """Table contains expected column names."""
    result = to_table(records)
    assert set(result.column_names) == {
        "transaction_id",
        "transaction_date",
        "gross_amount_usd",
        "currency_code",
        "transaction_status",
        "transaction_subject",
        "payer_email",
        "payer_name",
        "fee_amount_usd",
        "net_amount_usd",
    }


def test_to_table_date_values_correct(records):
    """Date column contains correct ISO format values."""
    result = to_table(records)
    assert result.column("transaction_date").to_pylist() == [
        "2024-01-15",
        "2024-01-15",
    ]


def test_to_table_empty_records_returns_empty_table():
    """Empty records list returns empty table."""
    result = to_table([])
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_to_table_gross_amount_values_correct(records):
    """Gross amount column contains correct float values."""
    result = to_table(records)
    assert result.column("gross_amount_usd").to_pylist() == [EXPECTED_GROSS, 75.00]


def test_to_table_returns_pyarrow_table(records):
    """Returns a pa.Table instance."""
    result = to_table(records)
    assert isinstance(result, pa.Table)


def test_to_table_row_count(records):
    """Table has one row per record."""
    result = to_table(records)
    assert result.num_rows == EXPECTED_ROW_COUNT
