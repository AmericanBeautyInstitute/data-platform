"""Tests for Facebook Ads extraction."""

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pydantic import ValidationError

from extract.facebook_ads.extract import (
    Raw,
    Record,
    _extract_action,
    _to_raw,
    extract,
    fetch,
    parse,
    to_table,
)

START_DATE = "2024-01-15"
END_DATE = "2024-01-15"
CAMPAIGN_ID = "123456789"
CAMPAIGN_NAME = "ABI Spring Enrollment"
EXPECTED_ROW_COUNT = 2
EXPECTED_COLUMN_COUNT = 11
EXPECTED_IMPRESSIONS = 1000
EXPECTED_CLICKS = 50
EXPECTED_SPEND = 25.50
EXPECTED_REACH = 900
EXPECTED_FREQUENCY = 1.11
EXPECTED_LINK_CLICKS = 45
EXPECTED_LEADS = 3
EXPECTED_CONVERSIONS = 1

SAMPLE_ACTIONS = [
    {"action_type": "link_click", "value": "45"},
    {"action_type": "lead", "value": "3"},
    {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "1"},
]

RAW_ROW_1 = Raw(
    date_start=START_DATE,
    campaign_id=CAMPAIGN_ID,
    campaign_name=CAMPAIGN_NAME,
    impressions="1000",
    clicks="50",
    spend="25.50",
    reach="900",
    frequency="1.11",
    actions=SAMPLE_ACTIONS,
)

RAW_ROW_2 = Raw(
    date_start="2024-01-16",
    campaign_id="987654321",
    campaign_name="ABI Summer Enrollment",
    impressions="2000",
    clicks="100",
    spend="50.00",
    reach="1800",
    frequency="1.11",
    actions=[],
)

API_ROW_1 = {
    "date_start": START_DATE,
    "campaign_id": CAMPAIGN_ID,
    "campaign_name": CAMPAIGN_NAME,
    "impressions": "1000",
    "clicks": "50",
    "spend": "25.50",
    "reach": "900",
    "frequency": "1.11",
    "actions": SAMPLE_ACTIONS,
}

API_ROW_2 = {
    "date_start": "2024-01-16",
    "campaign_id": "987654321",
    "campaign_name": "ABI Summer Enrollment",
    "impressions": "2000",
    "clicks": "100",
    "spend": "50.00",
    "reach": "1800",
    "frequency": "1.11",
    "actions": [],
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
    """Mocked Facebook Ads API client."""
    client = MagicMock()
    mock_row_1 = MagicMock()
    mock_row_2 = MagicMock()
    mock_row_1.__iter__ = MagicMock(return_value=iter(API_ROW_1.items()))
    mock_row_2.__iter__ = MagicMock(return_value=iter(API_ROW_2.items()))
    client.get_insights.return_value = [mock_row_1, mock_row_2]
    return client


def test_extract_action_returns_correct_value():
    """Returns correct integer value for a matching action type."""
    result = _extract_action(SAMPLE_ACTIONS, "link_click")
    assert result == EXPECTED_LINK_CLICKS


def test_extract_action_returns_zero_for_missing_type():
    """Returns 0 when action type is not present."""
    result = _extract_action(SAMPLE_ACTIONS, "nonexistent_action")
    assert result == 0


def test_extract_action_returns_zero_for_empty_list():
    """Returns 0 for empty actions list."""
    result = _extract_action([], "link_click")
    assert result == 0


def test_to_raw_returns_raw_instance():
    """Returns a Raw instance from an API row dict."""
    result = _to_raw(API_ROW_1)
    assert isinstance(result, Raw)


def test_to_raw_maps_fields_correctly():
    """All fields mapped correctly from API row dict."""
    result = _to_raw(API_ROW_1)
    assert result.date_start == START_DATE
    assert result.campaign_id == CAMPAIGN_ID
    assert result.campaign_name == CAMPAIGN_NAME
    assert result.impressions == "1000"
    assert result.spend == "25.50"
    assert result.actions == SAMPLE_ACTIONS


def test_to_raw_defaults_missing_fields():
    """Missing fields default to zero strings."""
    result = _to_raw({})
    assert result.impressions == "0"
    assert result.clicks == "0"
    assert result.spend == "0"
    assert result.actions == []


def test_fetch_returns_list_of_raw(mock_client):
    """Returns a list of Raw instances."""
    result = fetch(mock_client, START_DATE, END_DATE)
    assert all(isinstance(r, Raw) for r in result)


def test_fetch_calls_get_insights_with_correct_params(mock_client):
    """get_insights called with correct level and time_range."""
    fetch(mock_client, START_DATE, END_DATE)
    call_params = mock_client.get_insights.call_args[1]["params"]
    assert call_params["level"] == "campaign"
    assert call_params["time_range"]["since"] == START_DATE
    assert call_params["time_range"]["until"] == END_DATE


def test_fetch_row_count(mock_client):
    """Returns correct number of rows."""
    result = fetch(mock_client, START_DATE, END_DATE)
    assert len(result) == EXPECTED_ROW_COUNT


def test_parse_returns_record(raw_rows):
    """Returns a Record instance."""
    result = parse(raw_rows[0])
    assert isinstance(result, Record)


def test_parse_date_converted_to_date_type(raw_rows):
    """Date string is parsed to date object."""
    result = parse(raw_rows[0])
    assert result.date == date(2024, 1, 15)


def test_parse_impressions_converted_to_int(raw_rows):
    """Impressions string is parsed to int."""
    result = parse(raw_rows[0])
    assert result.impressions == EXPECTED_IMPRESSIONS
    assert isinstance(result.impressions, int)


def test_parse_spend_converted_to_float(raw_rows):
    """Spend string is parsed to float."""
    result = parse(raw_rows[0])
    assert result.spend_usd == EXPECTED_SPEND
    assert isinstance(result.spend_usd, float)


def test_parse_link_clicks_extracted_from_actions(raw_rows):
    """link_clicks extracted correctly from actions list."""
    result = parse(raw_rows[0])
    assert result.link_clicks == EXPECTED_LINK_CLICKS


def test_parse_leads_extracted_from_actions(raw_rows):
    """Leads extracted correctly from actions list."""
    result = parse(raw_rows[0])
    assert result.leads == EXPECTED_LEADS


def test_parse_conversions_extracted_from_actions(raw_rows):
    """Conversions extracted correctly from actions list."""
    result = parse(raw_rows[0])
    assert result.conversions == EXPECTED_CONVERSIONS


def test_parse_missing_actions_defaults_to_zero(raw_rows):
    """Missing actions default to zero for all action fields."""
    result = parse(raw_rows[1])
    assert result.link_clicks == 0
    assert result.leads == 0
    assert result.conversions == 0


def test_parse_record_is_immutable(raw_rows):
    """Record instances cannot be mutated."""
    result = parse(raw_rows[0])
    with pytest.raises(ValidationError):
        result.impressions = 999


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
        "date",
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend_usd",
        "reach",
        "frequency",
        "link_clicks",
        "leads",
        "conversions",
    }


def test_to_table_date_values_correct(records):
    """Date column contains correct ISO format values."""
    result = to_table(records)
    assert result.column("date").to_pylist() == ["2024-01-15", "2024-01-16"]


def test_to_table_spend_values_correct(records):
    """Spend column contains correct float values."""
    result = to_table(records)
    assert result.column("spend_usd").to_pylist() == [EXPECTED_SPEND, 50.00]


def test_to_table_empty_records_returns_empty_table():
    """Empty records list returns empty table."""
    result = to_table([])
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_extract_returns_pyarrow_table(raw_rows):
    """Returns a pa.Table instance."""
    with patch("extract.facebook_ads.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)
    assert isinstance(result, pa.Table)


def test_extract_row_count(raw_rows):
    """Table has correct number of rows."""
    with patch("extract.facebook_ads.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)
    assert result.num_rows == EXPECTED_ROW_COUNT


def test_extract_composes_fetch_parse_to_table(raw_rows):
    """Result matches manually composing fetch, parse, and to_table."""
    parsed = [parse(r) for r in raw_rows]
    expected = to_table(parsed)

    with patch("extract.facebook_ads.extract.fetch", return_value=raw_rows):
        result = extract(MagicMock(), START_DATE, END_DATE)

    assert result.equals(expected)


def test_raw_is_immutable():
    """Raw instances cannot be mutated."""
    with pytest.raises(ValidationError):
        RAW_ROW_1.date_start = "2024-01-16"


def test_record_date_validator_accepts_date_object():
    """Date validator passes through an existing date object unchanged."""
    existing_date = date(2024, 1, 15)
    record = Record(
        date=existing_date,
        campaign_id=CAMPAIGN_ID,
        campaign_name=CAMPAIGN_NAME,
        impressions=EXPECTED_IMPRESSIONS,
        clicks=EXPECTED_CLICKS,
        spend_usd=EXPECTED_SPEND,
        reach=EXPECTED_REACH,
        frequency=EXPECTED_FREQUENCY,
        link_clicks=EXPECTED_LINK_CLICKS,
        leads=EXPECTED_LEADS,
        conversions=EXPECTED_CONVERSIONS,
    )
    assert record.date == existing_date


def test_record_invalid_date_raises():
    """Invalid date string raises ValidationError."""
    with pytest.raises(ValidationError):
        Record(
            date="not-a-date",  # type: ignore[arg-type]
            campaign_id=CAMPAIGN_ID,
            campaign_name=CAMPAIGN_NAME,
            impressions=EXPECTED_IMPRESSIONS,
            clicks=EXPECTED_CLICKS,
            spend_usd=EXPECTED_SPEND,
            reach=EXPECTED_REACH,
            frequency=EXPECTED_FREQUENCY,
            link_clicks=EXPECTED_LINK_CLICKS,
            leads=EXPECTED_LEADS,
            conversions=EXPECTED_CONVERSIONS,
        )
