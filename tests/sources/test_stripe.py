"""Tests for the Stripe dlt source."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import dlt
import duckdb
import pytest
from pydantic import ValidationError

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
def mock_client(charge: dict) -> MagicMock:
    """Stripe client whose charges.list returns one page of one charge."""
    return _mock_client([charge])


def _mock_client(page: list[dict], has_more: bool = False) -> MagicMock:
    """Builds a Stripe client returning one charges.list page."""
    client = MagicMock()
    response = MagicMock()
    response.data = page
    response.has_more = has_more
    client.charges.list.return_value = response
    return client


def test_charge_is_immutable(charge: dict) -> None:
    """Tests that Charge instances cannot be mutated."""
    result = parse(charge)

    with pytest.raises(ValidationError):
        result.gross_amount_usd = 999.0


def test_fetch_paginates_with_cursor(charge: dict) -> None:
    """Tests that fetch follows the starting_after cursor across pages."""
    expected_rows = 2
    second = {**charge, "id": "ch_456"}
    client = MagicMock()
    first_page = MagicMock(data=[charge], has_more=True)
    second_page = MagicMock(data=[second], has_more=False)
    client.charges.list.side_effect = [first_page, second_page]

    rows = list(_fetch(client, START_DATE, END_DATE))

    assert len(rows) == expected_rows
    second_call = client.charges.list.call_args_list[1]
    assert second_call.kwargs["params"]["starting_after"] == "ch_123"


def test_fetch_single_page_lists_once(mock_client: MagicMock) -> None:
    """Tests that a single page of charges issues one API call."""
    list(_fetch(mock_client, START_DATE, END_DATE))

    mock_client.charges.list.assert_called_once()


def test_parse_casts_amounts_and_date(charge: dict) -> None:
    """Tests that cents are converted to dollars and the timestamp to a date."""
    expected_gross = 100.00
    expected_fee = 3.20
    expected_net = 96.80

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


def test_pipeline_loads_typed_rows(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that the pipeline lands typed columns in the destination."""
    expected_rows = 1
    expected_first_row = ("ch_123", date(2024, 1, 15), 100.00, 3.20)
    db_path = str(tmp_path / "stripe.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT charge_id, charge_date, gross_amount_usd, fee_usd "
        "FROM raw.stripe_charges"
    ).fetchall()

    assert rows[0] == expected_first_row
    assert len(rows) == expected_rows


def _run_pipeline(client: MagicMock, db_path: str, dlt_dir: str) -> None:
    """Runs the charges resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(charges(client, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: MagicMock, tmp_path: Path) -> None:
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
