"""Tests for the Stripe dlt source."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import dlt
import duckdb
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture
from stripe import Charge as StripeCharge

from sources.stripe import (
    _fetch,
    charges,
    parse,
)

START_DATE = date(2024, 1, 15)
END_DATE = date(2024, 1, 15)


@pytest.fixture
def charge() -> dict:
    """A single Stripe charge, as the charges API returns it."""
    return {
        "id": "ch_123",
        "created": 1705276800,
        "amount": 10000,
        "amount_captured": 10000,
        "balance_transaction": {"fee": 320, "net": 9680},
        "currency": "usd",
        "status": "succeeded",
        "description": "Tuition",
        "billing_details": {"email": "student@example.com", "name": "Jane Doe"},
        "receipt_email": None,
        "payment_intent": "pi_456",
    }


@pytest.fixture
def mock_client(charge: dict, mocker: MockerFixture) -> Any:
    """Stripe client whose charges.list returns one page of one charge."""
    return _mock_client(mocker, [charge])


def _mock_client(
    mocker: MockerFixture,
    page: list[dict],
    has_more: bool = False,
) -> Any:
    """Returns a mocked Stripe client with one response page."""
    client = mocker.MagicMock()
    response = mocker.MagicMock()
    response.data = [
        StripeCharge.construct_from(payload, "sk_test") for payload in page
    ]
    response.has_more = has_more
    client.charges.list.return_value = response
    return client


def test_charge_is_immutable(charge: dict) -> None:
    """Tests that Charge instances cannot be mutated."""
    result = parse(charge)

    with pytest.raises(ValidationError):
        result.gross_amount_usd = Decimal("999.00")


def test_fetch_paginates_with_cursor(
    charge: dict,
    mocker: MockerFixture,
) -> None:
    """Tests that fetch follows the starting_after cursor across pages."""
    expected_rows = 2
    second = {**charge, "id": "ch_456"}
    client = mocker.MagicMock()
    first_page = mocker.MagicMock(
        data=[StripeCharge.construct_from(charge, "sk_test")],
        has_more=True,
    )
    second_page = mocker.MagicMock(
        data=[StripeCharge.construct_from(second, "sk_test")],
        has_more=False,
    )
    client.charges.list.side_effect = [first_page, second_page]

    rows = list(_fetch(client, START_DATE, END_DATE))

    assert len(rows) == expected_rows
    second_call = client.charges.list.call_args_list[1]
    assert second_call.kwargs["params"]["starting_after"] == "ch_123"


def test_fetch_single_page_lists_once(mock_client: Any) -> None:
    """Tests that a single page of charges issues one API call."""
    list(_fetch(mock_client, START_DATE, END_DATE))

    mock_client.charges.list.assert_called_once()


def test_parse_casts_amounts_and_date(charge: dict) -> None:
    """Tests that cents are converted to dollars and the timestamp to a date."""
    expected_gross = Decimal("100.00")
    expected_fee = Decimal("3.20")
    expected_net = Decimal("96.80")

    result = parse(charge)

    assert result.charge_date == date(2024, 1, 15)
    assert result.gross_amount_usd == expected_gross
    assert result.fee_usd == expected_fee
    assert result.net_usd == expected_net


def test_parse_defaults_optional_fields(charge: dict) -> None:
    """Tests that omitted descriptive fields fall back to empty strings."""
    del charge["description"]
    del charge["payment_intent"]
    charge["billing_details"] = {}

    result = parse(charge)

    assert result.description == ""
    assert result.customer_name == ""
    assert result.payment_intent_id == ""


def test_parse_fails_loud_on_missing_id(charge: dict) -> None:
    """Tests that a charge with no id raises ValueError."""
    del charge["id"]

    with pytest.raises(ValueError, match="Failed to parse Stripe charge"):
        parse(charge)


def test_pipeline_loads_typed_rows(mock_client: Any, tmp_path: Path) -> None:
    """Tests that the pipeline lands typed columns in the destination."""
    expected_rows = 1
    expected_first_row = (
        "ch_123",
        date(2024, 1, 15),
        Decimal("100.00"),
        Decimal("3.20"),
    )
    db_path = str(tmp_path / "stripe.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT charge_id, charge_date, gross_amount_usd, fee_usd "
        "FROM raw.stripe_charges"
    ).fetchall()

    assert rows[0] == expected_first_row
    assert len(rows) == expected_rows


def _run_pipeline(client: Any, db_path: str, dlt_dir: str) -> None:
    """Runs the charges resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(charges(client, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: Any, tmp_path: Path) -> None:
    """Tests that re-running the same charge upserts rather than duplicating."""
    expected_rows = 1
    db_path = str(tmp_path / "stripe.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(mock_client, db_path, dlt_dir)
    _run_pipeline(mock_client, db_path, dlt_dir)

    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT count(*) FROM raw.stripe_charges").fetchone()
    count = result[0] if result else 0

    assert count == expected_rows


@pytest.mark.parametrize("minor_units", [True, 1.0, "100"])
def test_parse_rejects_noninteger_minor_units(
    charge: dict,
    minor_units: object,
) -> None:
    """Tests that Stripe accepts only integer minor units."""
    charge["amount"] = minor_units

    with pytest.raises(ValueError, match="Failed to parse Stripe charge"):
        parse(charge)


def test_parse_rejects_unsupported_currency(charge: dict) -> None:
    """Tests that Stripe rejects non-USD charges."""
    charge["currency"] = "eur"

    with pytest.raises(ValueError, match="Failed to parse Stripe charge"):
        parse(charge)


def test_parse_error_omits_payload(charge: dict) -> None:
    """Tests that parse errors omit provider payloads."""
    charge["id"] = ""

    with pytest.raises(ValueError) as exc_info:
        parse(charge)

    assert "student@example.com" not in str(exc_info.value)
