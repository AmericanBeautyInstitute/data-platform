"""Tests for Stripe charge extraction."""

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pydantic import ValidationError

from extract.stripe.extract import (
    Raw,
    Record,
    _date_to_timestamp,
    _to_raw,
    extract,
    fetch,
    parse,
)
from extract.table import to_table

START_DATE = "2024-01-15"
END_DATE = "2024-01-15"
CHARGE_ID = "ch_abc123"
PAYMENT_INTENT_ID = "pi_abc123"
EXPECTED_ROW_COUNT = 2
EXPECTED_COLUMN_COUNT = 12
EXPECTED_GROSS = 150.00
EXPECTED_FEE = 4.65
EXPECTED_NET = 145.35
EXPECTED_AMOUNT_CENTS = 15000
EXPECTED_FEE_CENTS = 465
EXPECTED_STATUS = "succeeded"
EXPECTED_EMAIL = "student@example.com"
EXPECTED_NAME = "Alice Smith"
EXPECTED_DESCRIPTION = "Cosmetology Program Enrollment"
UNIX_TIMESTAMP_2024_01_15 = 1705276800

API_CHARGE_1 = {
    "id": CHARGE_ID,
    "created": UNIX_TIMESTAMP_2024_01_15,
    "amount": 15000,
    "amount_captured": 15000,
    "balance_transaction": {"fee": 465, "net": 14535},
    "currency": "usd",
    "status": EXPECTED_STATUS,
    "description": EXPECTED_DESCRIPTION,
    "billing_details": {"email": EXPECTED_EMAIL, "name": EXPECTED_NAME},
    "receipt_email": EXPECTED_EMAIL,
    "payment_intent": PAYMENT_INTENT_ID,
}

API_CHARGE_2 = {
    "id": "ch_def456",
    "created": UNIX_TIMESTAMP_2024_01_15,
    "amount": 7500,
    "amount_captured": 7500,
    "balance_transaction": {"fee": 248, "net": 7252},
    "currency": "usd",
    "status": "succeeded",
    "description": "Inventory Purchase",
    "billing_details": {"email": "vendor@example.com", "name": "Bob Jones"},
    "receipt_email": "vendor@example.com",
    "payment_intent": "pi_def456",
}

RAW_ROW_1 = Raw(
    charge_id=CHARGE_ID,
    created=UNIX_TIMESTAMP_2024_01_15,
    amount=15000,
    amount_captured=15000,
    fee=465,
    net=14535,
    currency="usd",
    status=EXPECTED_STATUS,
    description=EXPECTED_DESCRIPTION,
    customer_email=EXPECTED_EMAIL,
    customer_name=EXPECTED_NAME,
    payment_intent_id=PAYMENT_INTENT_ID,
)

RAW_ROW_2 = Raw(
    charge_id="ch_def456",
    created=UNIX_TIMESTAMP_2024_01_15,
    amount=7500,
    amount_captured=7500,
    fee=248,
    net=7252,
    currency="usd",
    status="succeeded",
    description="Inventory Purchase",
    customer_email="vendor@example.com",
    customer_name="Bob Jones",
    payment_intent_id="pi_def456",
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
    """Mocked Stripe API client returning single page of charges."""
    client = MagicMock()
    mock_charge_1 = MagicMock()
    mock_charge_2 = MagicMock()
    mock_charge_1.id = CHARGE_ID
    mock_charge_2.id = "ch_def456"
    mock_charge_1.__iter__ = MagicMock(return_value=iter(API_CHARGE_1.items()))
    mock_charge_2.__iter__ = MagicMock(return_value=iter(API_CHARGE_2.items()))
    response = MagicMock()
    response.data = [mock_charge_1, mock_charge_2]
    response.has_more = False
    client.charges.list.return_value = response
    return client


def test_date_to_timestamp_start_of_day():
    """Start of day timestamp is midnight UTC."""
    result = _date_to_timestamp("2024-01-15")
    assert result == UNIX_TIMESTAMP_2024_01_15


def test_date_to_timestamp_end_of_day():
    """End of day timestamp is 23:59:59 UTC."""
    result = _date_to_timestamp("2024-01-15", end_of_day=True)
    assert result == UNIX_TIMESTAMP_2024_01_15 + 86399


def test_to_raw_returns_raw_instance():
    """Returns a Raw instance from an API charge dict."""
    result = _to_raw(API_CHARGE_1)
    assert isinstance(result, Raw)


def test_to_raw_maps_charge_id():
    """Charge ID mapped correctly."""
    result = _to_raw(API_CHARGE_1)
    assert result.charge_id == CHARGE_ID


def test_to_raw_maps_amount():
    """Amount in cents mapped correctly."""
    result = _to_raw(API_CHARGE_1)
    assert result.amount == EXPECTED_AMOUNT_CENTS


def test_to_raw_maps_fee_from_balance_transaction():
    """Fee extracted from balance_transaction correctly."""
    result = _to_raw(API_CHARGE_1)
    assert result.fee == EXPECTED_FEE_CENTS


def test_to_raw_maps_customer_email_from_billing_details():
    """Customer email extracted from billing_details."""
    result = _to_raw(API_CHARGE_1)
    assert result.customer_email == EXPECTED_EMAIL


def test_to_raw_defaults_missing_fields():
    """Missing fields default gracefully."""
    result = _to_raw({})
    assert result.amount == 0
    assert result.fee == 0
    assert result.customer_email == ""
    assert result.description == ""


def test_to_raw_handles_null_balance_transaction():
    """None balance_transaction defaults fee and net to zero."""
    charge = {**API_CHARGE_1, "balance_transaction": None}
    result = _to_raw(charge)
    assert result.fee == 0
    assert result.net == 0


def test_fetch_returns_list_of_raw(mock_client):
    """Returns a list of Raw instances."""
    with patch(
        "extract.stripe.extract._to_raw", side_effect=lambda c: _to_raw(dict(c))
    ):
        result = fetch(mock_client, START_DATE, END_DATE)
    assert all(isinstance(r, Raw) for r in result)


def test_fetch_calls_charges_list(mock_client):
    """client.charges.list called with correct date range."""
    with patch(
        "extract.stripe.extract._to_raw", side_effect=lambda c: _to_raw(dict(c))
    ):
        fetch(mock_client, START_DATE, END_DATE)
    mock_client.charges.list.assert_called_once()
    call_params = (
        mock_client.charges.list.call_args[0][0]
        if mock_client.charges.list.call_args[0]
        else mock_client.charges.list.call_args[1].get(
            "params",
            mock_client.charges.list.call_args[0][0]
            if mock_client.charges.list.call_args[0]
            else next(iter(mock_client.charges.list.call_args[1].values())),
        )
    )
    assert "created" in call_params
    assert call_params["created"]["gte"] == UNIX_TIMESTAMP_2024_01_15


def test_fetch_paginates_multiple_pages():
    """Fetches all pages when has_more is True."""
    client = MagicMock()
    mock_charge_1 = MagicMock()
    mock_charge_2 = MagicMock()
    mock_charge_1.id = CHARGE_ID
    mock_charge_2.id = "ch_def456"

    page_1 = MagicMock()
    page_1.data = [mock_charge_1]
    page_1.has_more = True

    page_2 = MagicMock()
    page_2.data = [mock_charge_2]
    page_2.has_more = False

    client.charges.list.side_effect = [page_1, page_2]

    with patch("extract.stripe.extract._to_raw", side_effect=lambda c: RAW_ROW_1):
        result = fetch(client, START_DATE, END_DATE)

    assert len(result) == EXPECTED_ROW_COUNT
    assert client.charges.list.call_count == EXPECTED_ROW_COUNT


def test_fetch_empty_response_returns_empty_list():
    """Empty charges list returns empty list."""
    client = MagicMock()
    response = MagicMock()
    response.data = []
    response.has_more = False
    client.charges.list.return_value = response

    with patch("extract.stripe.extract._to_raw", side_effect=lambda c: RAW_ROW_1):
        result = fetch(client, START_DATE, END_DATE)

    assert result == []


def test_parse_returns_record(raw_rows):
    """Returns a Record instance."""
    result = parse(raw_rows[0])
    assert isinstance(result, Record)


def test_parse_timestamp_converted_to_date(raw_rows):
    """Unix timestamp is converted to date object."""
    result = parse(raw_rows[0])
    assert result.charge_date == date(2024, 1, 15)


def test_parse_cents_converted_to_dollars(raw_rows):
    """Amount in cents is converted to dollars."""
    result = parse(raw_rows[0])
    assert result.gross_amount_usd == EXPECTED_GROSS
    assert isinstance(result.gross_amount_usd, float)


def test_parse_fee_cents_converted_to_dollars(raw_rows):
    """Fee in cents is converted to dollars."""
    result = parse(raw_rows[0])
    assert result.fee_usd == EXPECTED_FEE
    assert isinstance(result.fee_usd, float)


def test_parse_net_cents_converted_to_dollars(raw_rows):
    """Net in cents is converted to dollars."""
    result = parse(raw_rows[0])
    assert result.net_usd == EXPECTED_NET
    assert isinstance(result.net_usd, float)


def test_parse_record_is_immutable(raw_rows):
    """Record instances cannot be mutated."""
    result = parse(raw_rows[0])
    with pytest.raises(ValidationError):
        result.charge_id = "NEW_ID"


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


def test_to_table_column_names(records):
    """Table contains expected column names."""
    result = to_table(records)
    assert set(result.column_names) == {
        "charge_id",
        "charge_date",
        "gross_amount_usd",
        "amount_captured_usd",
        "fee_usd",
        "net_usd",
        "currency",
        "status",
        "description",
        "customer_email",
        "customer_name",
        "payment_intent_id",
    }


def test_to_table_date_values_correct(records):
    """Date column contains correct ISO format values."""
    result = to_table(records)
    assert result.column("charge_date").to_pylist() == ["2024-01-15", "2024-01-15"]


def test_to_table_gross_amount_values_correct(records):
    """Gross amount column contains correct dollar values."""
    result = to_table(records)
    assert result.column("gross_amount_usd").to_pylist() == [EXPECTED_GROSS, 75.00]


def test_to_table_empty_records_returns_empty_table():
    """Empty records list returns empty table."""
    result = to_table([])
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_extract_returns_pyarrow_table(raw_rows):
    """Returns a pa.Table instance."""
    with patch("extract.stripe.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)
    assert isinstance(result, pa.Table)


def test_extract_row_count(raw_rows):
    """Table has correct number of rows."""
    with patch("extract.stripe.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)
    assert result.num_rows == EXPECTED_ROW_COUNT


def test_extract_composes_fetch_parse_to_table(raw_rows):
    """Result matches manually composing fetch, parse, and to_table."""
    parsed = [parse(r) for r in raw_rows]
    expected = to_table(parsed)

    with patch("extract.stripe.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)

    assert result.equals(expected)


def test_raw_is_immutable():
    """Raw instances cannot be mutated."""
    with pytest.raises(ValidationError):
        RAW_ROW_1.charge_id = "NEW_ID"


def test_record_date_validator_accepts_date_object():
    """Date validator passes through an existing date object unchanged."""
    existing_date = date(2024, 1, 15)
    record = Record(
        charge_id=CHARGE_ID,
        charge_date=existing_date,
        gross_amount_usd=EXPECTED_GROSS,
        amount_captured_usd=EXPECTED_GROSS,
        fee_usd=EXPECTED_FEE,
        net_usd=EXPECTED_NET,
        currency="usd",
        status=EXPECTED_STATUS,
        description=EXPECTED_DESCRIPTION,
        customer_email=EXPECTED_EMAIL,
        customer_name=EXPECTED_NAME,
        payment_intent_id=PAYMENT_INTENT_ID,
    )
    assert record.charge_date == existing_date


def test_record_invalid_timestamp_raises():
    """Non-integer timestamp raises ValidationError."""
    with pytest.raises(ValidationError):
        Record(
            charge_id=CHARGE_ID,
            charge_date="not-a-date",  # ty: ignore[invalid-argument-type]
            gross_amount_usd=EXPECTED_GROSS,
            amount_captured_usd=EXPECTED_GROSS,
            fee_usd=EXPECTED_FEE,
            net_usd=EXPECTED_NET,
            currency="usd",
            status=EXPECTED_STATUS,
            description=EXPECTED_DESCRIPTION,
            customer_email=EXPECTED_EMAIL,
            customer_name=EXPECTED_NAME,
            payment_intent_id=PAYMENT_INTENT_ID,
        )
