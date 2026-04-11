"""Stripe charge data extractor."""

from datetime import UTC, date, datetime

import pyarrow as pa
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)
from stripe import StripeClient

from extract.table import to_table

PAGE_SIZE = 100


class Raw(BaseModel):
    """Mirrors a single charge from the Stripe API response."""

    model_config = ConfigDict(frozen=True)

    charge_id: str
    created: int
    amount: int
    amount_captured: int
    fee: int
    net: int
    currency: str
    status: str
    description: str
    customer_email: str
    customer_name: str
    payment_intent_id: str


class Record(BaseModel):
    """A validated, typed Stripe charge record."""

    model_config = ConfigDict(frozen=True)

    charge_id: str
    charge_date: date = Field(
        validation_alias=AliasChoices("charge_date", "created"),
    )
    gross_amount_usd: float = Field(
        validation_alias=AliasChoices("gross_amount_usd", "amount"),
    )
    amount_captured_usd: float = Field(
        validation_alias=AliasChoices("amount_captured_usd", "amount_captured"),
    )
    fee_usd: float = Field(
        validation_alias=AliasChoices("fee_usd", "fee"),
    )
    net_usd: float = Field(
        validation_alias=AliasChoices("net_usd", "net"),
    )
    currency: str
    status: str
    description: str
    customer_email: str
    customer_name: str
    payment_intent_id: str

    @field_validator("charge_date", mode="before")
    @classmethod
    def parse_date(cls, v: int | date) -> date:
        """Converts a Unix timestamp to a date."""
        if isinstance(v, date):
            return v
        if not isinstance(v, int):
            raise ValueError(f"charge_date must be a Unix timestamp int, got {type(v)}")
        dt = datetime.fromtimestamp(v, tz=UTC)
        return dt.date()

    @field_validator(
        "gross_amount_usd",
        "amount_captured_usd",
        "fee_usd",
        "net_usd",
        mode="before",
    )
    @classmethod
    def cents_to_dollars(cls, v: int | float) -> float:
        """Converts Stripe integer cents to float dollars."""
        cents = int(v)
        dollars = round(cents / 100, 2)
        return dollars


def extract(client: StripeClient, start_date: date, end_date: date) -> pa.Table:
    """Extracts Stripe charges into a PyArrow table."""
    raw_rows = fetch(client, start_date, end_date)
    records = [parse(r) for r in raw_rows]
    table = to_table(records)
    return table


def _date_to_timestamp(d: date, end_of_day: bool = False) -> int:
    """Converts a date to a UTC Unix timestamp."""
    dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    timestamp = int(dt.timestamp())
    return timestamp


def _to_raw(charge: dict) -> Raw:
    """Converts a Stripe charge dict into a Raw instance."""
    billing = charge["billing_details"]
    balance_txn = charge.get("balance_transaction") or {}
    fee = balance_txn.get("fee", 0) if isinstance(balance_txn, dict) else 0
    net = balance_txn.get("net", 0) if isinstance(balance_txn, dict) else 0
    return Raw(
        charge_id=charge["id"],
        created=charge["created"],
        amount=charge["amount"],
        amount_captured=charge["amount_captured"],
        fee=fee,
        net=net,
        currency=charge["currency"],
        status=charge["status"],
        description=charge.get("description") or "",
        customer_email=billing.get("email") or charge.get("receipt_email") or "",
        customer_name=billing.get("name") or "",
        payment_intent_id=charge.get("payment_intent") or "",
    )


def fetch(client: StripeClient, start_date: date, end_date: date) -> list[Raw]:
    """Fetches all charges for the given date range from Stripe API."""
    raw_rows: list[Raw] = []
    start_ts = _date_to_timestamp(start_date)
    end_ts = _date_to_timestamp(end_date, end_of_day=True)
    has_more = True
    starting_after = None

    while has_more:
        params: dict = {
            "created": {"gte": start_ts, "lte": end_ts},
            "limit": PAGE_SIZE,
            "expand": ["data.balance_transaction"],
        }
        if starting_after:
            params["starting_after"] = starting_after

        response = client.charges.list(params=params)
        charges = response.data
        raw_rows.extend(_to_raw(dict(c)) for c in charges)
        has_more = response.has_more
        if has_more and charges:
            starting_after = charges[-1].id

    return raw_rows


def parse(raw: Raw) -> Record:
    """Converts a Raw Stripe charge into a typed Record."""
    try:
        return Record(**raw.model_dump())
    except ValidationError as exc:
        raise ValueError(f"Failed to parse Stripe charge: {raw}") from exc
