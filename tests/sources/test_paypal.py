"""Tests for the PayPal dlt source."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import dlt
import duckdb
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from sources.paypal import (
    _fetch,
    parse,
    transactions,
)

START_DATE = date(2024, 1, 15)
END_DATE = date(2024, 1, 15)


@pytest.fixture
def transaction() -> dict:
    """A single PayPal transaction, as the reporting API returns it."""
    return {
        "transaction_info": {
            "transaction_id": "TXN123",
            "transaction_initiation_date": "2024-01-15T10:30:00+0000",
            "transaction_amount": {"value": "100.00", "currency_code": "USD"},
            "fee_amount": {"value": "-3.20", "currency_code": "USD"},
            "transaction_net_amount": {"value": "96.80", "currency_code": "USD"},
            "transaction_status": "S",
            "transaction_subject": "Tuition",
        },
        "payer_info": {
            "email_address": "student@example.com",
            "payer_name": {"given_name": "Jane", "surname": "Doe"},
        },
    }


@pytest.fixture
def mock_client(transaction: dict, mocker: MockerFixture) -> Any:
    """PayPal REST client whose paginate yields one page of one transaction."""
    client = mocker.MagicMock()
    client.paginate.return_value = [[transaction]]
    return client


def test_fetch_paginates_transaction_details(mock_client: Any) -> None:
    """Tests that fetch flattens paginated pages into transaction dicts."""
    expected_page_size = 500

    rows = list(_fetch(mock_client, START_DATE, END_DATE))

    assert rows[0]["transaction_info"]["transaction_id"] == "TXN123"
    params = mock_client.paginate.call_args.kwargs["params"]
    assert params["page_size"] == expected_page_size


def test_parse_builds_full_payer_name(transaction: dict) -> None:
    """Tests that the payer's given and surname are joined into one name."""
    result = parse(transaction)

    assert result.payer_name == "Jane Doe"


def test_parse_casts_amounts_and_date(transaction: dict) -> None:
    """Tests that money fields and the date are cast to their typed forms."""
    expected_gross = Decimal("100.00")
    expected_fee = Decimal("-3.20")
    expected_net = Decimal("96.80")

    result = parse(transaction)

    assert result.transaction_date == date(2024, 1, 15)
    assert result.gross_amount_usd == expected_gross
    assert result.fee_amount_usd == expected_fee
    assert result.net_amount_usd == expected_net


def test_parse_defaults_optional_fields(transaction: dict) -> None:
    """Tests that omitted descriptive and fee fields fall back to defaults."""
    del transaction["transaction_info"]["transaction_subject"]
    del transaction["transaction_info"]["fee_amount"]
    del transaction["payer_info"]["email_address"]

    result = parse(transaction)

    assert result.transaction_subject == ""
    assert result.payer_email == ""
    assert result.fee_amount_usd == Decimal("0.00")


def test_parse_fails_loud_on_missing_transaction_id(transaction: dict) -> None:
    """Tests that a transaction with no id raises ValueError."""
    del transaction["transaction_info"]["transaction_id"]

    with pytest.raises(ValueError, match="Failed to parse PayPal transaction"):
        parse(transaction)


def test_parse_keeps_reference_id_separate(transaction: dict) -> None:
    """Tests that PayPal reference identity remains separate."""
    transaction["transaction_info"]["paypal_reference_id"] = "REF999"

    result = parse(transaction)

    assert result.transaction_id == "TXN123"
    assert result.paypal_reference_id == "REF999"


def test_pipeline_loads_typed_rows(mock_client: Any, tmp_path: Path) -> None:
    """Tests that the pipeline lands typed columns in the destination."""
    expected_rows = 1
    expected_first_row = ("TXN123", date(2024, 1, 15), 100.00)
    db_path = str(tmp_path / "paypal.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT transaction_id, transaction_date, gross_amount_usd "
        "FROM raw.paypal_transactions"
    ).fetchall()

    assert rows[0] == expected_first_row
    assert len(rows) == expected_rows


def _run_pipeline(client: Any, db_path: str, dlt_dir: str) -> None:
    """Runs the transactions resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="paypal_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(transactions(client, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: Any, tmp_path: Path) -> None:
    """Tests that re-running the same transaction upserts rather than duplicating."""
    expected_rows = 1
    db_path = str(tmp_path / "paypal.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(mock_client, db_path, dlt_dir)
    _run_pipeline(mock_client, db_path, dlt_dir)

    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT count(*) FROM raw.paypal_transactions").fetchone()
    count = result[0] if result else 0

    assert count == expected_rows


def test_transaction_is_immutable(transaction: dict) -> None:
    """Tests that Transaction instances cannot be mutated."""
    result = parse(transaction)

    with pytest.raises(ValidationError):
        result.gross_amount_usd = Decimal("999.00")


@pytest.mark.parametrize("value", [1, 1.0, True, "NaN", "Infinity", "1.001"])
def test_parse_rejects_invalid_money(
    transaction: dict,
    value: object,
) -> None:
    """Tests that PayPal rejects invalid money representations."""
    transaction["transaction_info"]["transaction_amount"]["value"] = value

    with pytest.raises(ValueError, match="Failed to parse PayPal transaction"):
        parse(transaction)


@pytest.mark.parametrize(
    "field_name",
    ["transaction_amount", "fee_amount", "transaction_net_amount"],
)
def test_parse_validates_each_money_currency(
    transaction: dict,
    field_name: str,
) -> None:
    """Tests that every present PayPal money object uses USD."""
    transaction["transaction_info"][field_name]["currency_code"] = "EUR"

    with pytest.raises(ValueError, match="Failed to parse PayPal transaction"):
        parse(transaction)


@pytest.mark.parametrize(
    "field_name",
    ["transaction_amount", "transaction_net_amount"],
)
def test_parse_requires_gross_and_net(
    transaction: dict,
    field_name: str,
) -> None:
    """Tests that PayPal gross and net money objects are required."""
    del transaction["transaction_info"][field_name]

    with pytest.raises(ValueError, match="Failed to parse PayPal transaction"):
        parse(transaction)


def test_parse_error_omits_payload(transaction: dict) -> None:
    """Tests that parse errors omit provider payloads."""
    transaction["transaction_info"]["transaction_id"] = ""

    with pytest.raises(ValueError) as exc_info:
        parse(transaction)

    assert "student@example.com" not in str(exc_info.value)
