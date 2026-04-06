"""PayPal transaction data extractor."""

from datetime import date

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, field_validator

from extract.paypal.client import PayPalClient

PAGE_SIZE = 500


class Raw(BaseModel):
    """Mirrors a single transaction from the PayPal Transactions API response."""

    model_config = ConfigDict(frozen=True)

    transaction_id: str
    transaction_date: str
    transaction_amount: str
    currency_code: str
    transaction_status: str
    transaction_subject: str
    payer_email: str
    payer_name: str
    fee_amount: str
    net_amount: str


class Record(BaseModel):
    """A validated, typed PayPal transaction record."""

    model_config = ConfigDict(frozen=True)

    transaction_id: str
    transaction_date: date
    gross_amount_usd: float
    currency_code: str
    transaction_status: str
    transaction_subject: str
    payer_email: str
    payer_name: str
    fee_amount_usd: float
    net_amount_usd: float

    @field_validator("transaction_date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses ISO 8601 date string from PayPal API."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(v[:10])

    @field_validator(
        "gross_amount_usd", "fee_amount_usd", "net_amount_usd", mode="before"
    )
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses string float fields from PayPal API."""
        return float(v)


def extract(client: PayPalClient, start_date: str, end_date: str) -> pa.Table:
    """Extracts PayPal transactions into a PyArrow table."""
    raw_rows = fetch(client, start_date, end_date)
    records = [parse(r) for r in raw_rows]
    table = to_table(records)
    return table


def _parse_transaction(txn: dict) -> Raw:
    """Converts a PayPal API transaction dict into a Raw instance."""
    info = txn.get("transaction_info", {})
    payer = txn.get("payer_info", {})
    amount = info.get("transaction_amount", {})
    fee = info.get("fee_amount", {})
    net = info.get("transaction_net_amount", {})
    payer_name_obj = payer.get("payer_name", {})
    full_name = " ".join(
        filter(
            None,
            [
                payer_name_obj.get("given_name", ""),
                payer_name_obj.get("surname", ""),
            ],
        )
    )
    return Raw(
        transaction_id=info.get("paypal_reference_id", info.get("transaction_id", "")),
        transaction_date=info.get("transaction_initiation_date", ""),
        transaction_amount=amount.get("value", "0"),
        currency_code=amount.get("currency_code", "USD"),
        transaction_status=info.get("transaction_status", ""),
        transaction_subject=info.get("transaction_subject", ""),
        payer_email=payer.get("email_address", ""),
        payer_name=full_name,
        fee_amount=fee.get("value", "0"),
        net_amount=net.get("value", "0"),
    )


def fetch(client: PayPalClient, start_date: str, end_date: str) -> list[Raw]:
    """Fetches all transactions for the given date range from PayPal API."""
    raw_rows: list[Raw] = []
    page = 1

    while True:
        response = client.get(
            "/v1/reporting/transactions",
            params={
                "start_date": f"{start_date}T00:00:00-0000",
                "end_date": f"{end_date}T23:59:59-0000",
                "fields": "all",
                "page_size": PAGE_SIZE,
                "page": page,
            },
        )
        transactions = response.get("transaction_details", [])
        raw_rows.extend(_parse_transaction(t) for t in transactions)

        total_pages = response.get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1

    return raw_rows


def parse(raw: Raw) -> Record:
    """Converts a Raw PayPal transaction into a typed Record."""
    return Record(
        transaction_id=raw.transaction_id,
        transaction_date=raw.transaction_date,
        gross_amount_usd=raw.transaction_amount,
        currency_code=raw.currency_code,
        transaction_status=raw.transaction_status,
        transaction_subject=raw.transaction_subject,
        payer_email=raw.payer_email,
        payer_name=raw.payer_name,
        fee_amount_usd=raw.fee_amount,
        net_amount_usd=raw.net_amount,
    )


def to_table(records: list[Record]) -> pa.Table:
    """Converts a list of Records into a PyArrow table."""
    rows = [
        {
            "transaction_id": r.transaction_id,
            "transaction_date": r.transaction_date.isoformat(),
            "gross_amount_usd": r.gross_amount_usd,
            "currency_code": r.currency_code,
            "transaction_status": r.transaction_status,
            "transaction_subject": r.transaction_subject,
            "payer_email": r.payer_email,
            "payer_name": r.payer_name,
            "fee_amount_usd": r.fee_amount_usd,
            "net_amount_usd": r.net_amount_usd,
        }
        for r in records
    ]
    return pa.Table.from_pylist(rows)
