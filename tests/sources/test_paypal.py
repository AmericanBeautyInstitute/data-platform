"""Tests for the PayPal dlt source."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import dlt
import duckdb
import pytest
from pydantic import ValidationError

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
def mock_client(transaction: dict) -> MagicMock:
    """PayPal REST client whose paginate yields one page of one transaction."""
    client = MagicMock()
    client.paginate.return_value = [[transaction]]
    return client


def test_fetch_paginates_transaction_details(mock_client: MagicMock) -> None:
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
    expected_gross = 100.00
    expected_fee = -3.20
    expected_net = 96.80

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
    assert result.fee_amount_usd == 0.0


def test_parse_fails_loud_on_missing_transaction_id(transaction: dict) -> None:
    """Tests that a transaction with no id raises ValueError."""
    del transaction["transaction_info"]["transaction_id"]

    with pytest.raises(ValueError, match="Failed to parse PayPal transaction"):
        parse(transaction)


def test_parse_prefers_reference_id(transaction: dict) -> None:
    """Tests that paypal_reference_id takes precedence over transaction_id."""
    transaction["transaction_info"]["paypal_reference_id"] = "REF999"

    result = parse(transaction)

    assert result.transaction_id == "REF999"


def test_pipeline_loads_typed_rows(mock_client: MagicMock, tmp_path: Path) -> None:
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


def _run_pipeline(client: MagicMock, db_path: str, dlt_dir: str) -> None:
    """Runs the transactions resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="paypal_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(transactions(client, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: MagicMock, tmp_path: Path) -> None:
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
        result.gross_amount_usd = 999.0
