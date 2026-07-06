"""Stripe charges dlt source."""

from collections.abc import Iterator
from datetime import UTC, date, datetime

import dlt
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from stripe import StripeClient

PAGE_SIZE = 100
PRIMARY_KEY = "charge_id"


class Charge(BaseModel):
    """A validated, typed Stripe charge record."""

    model_config = ConfigDict(frozen=True)

    charge_id: str
    charge_date: date
    gross_amount_usd: float
    amount_captured_usd: float
    fee_usd: float
    net_usd: float
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
        return datetime.fromtimestamp(v, tz=UTC).date()

    @field_validator(
        "gross_amount_usd",
        "amount_captured_usd",
        "fee_usd",
        "net_usd",
        mode="before",
    )
    @classmethod
    def cents_to_dollars(cls, v: int | float) -> float:
        """Converts Stripe integer cents to float dollars.

        TODO: Assumes a two-decimal currency (USD). Zero-decimal currencies
        (e.g. JPY) report whole units, so this would divide incorrectly for
        non-USD accounts.
        """
        return round(int(v) / 100, 2)


@dlt.source(name="stripe")
def stripe_source(
    client: StripeClient,
    start_date: date,
    end_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the Stripe resources for a date range."""
    yield charges(client, start_date, end_date)


@dlt.resource(
    name="stripe_charges",
    write_disposition="merge",
    primary_key=PRIMARY_KEY,
    columns=Charge,
)
def charges(
    client: StripeClient,
    start_date: date,
    end_date: date,
) -> Iterator[Charge]:
    """Yields validated Stripe charges for the given date range."""
    for charge in _fetch(client, start_date, end_date):
        yield parse(charge)


def parse(charge: dict) -> Charge:
    """Converts a raw Stripe charge into a typed Charge.

    Identity and amount fields are required and fail loud; optional descriptive
    fields default to empty because Stripe returns them as null.
    """
    billing = charge.get("billing_details", {})
    balance_txn = charge.get("balance_transaction")
    fees = balance_txn if isinstance(balance_txn, dict) else {}
    try:
        return Charge(
            charge_id=charge["id"],
            charge_date=charge["created"],
            gross_amount_usd=charge["amount"],
            amount_captured_usd=charge.get("amount_captured", 0),
            fee_usd=fees.get("fee", 0),
            net_usd=fees.get("net", 0),
            currency=charge["currency"],
            status=charge["status"],
            description=charge.get("description") or "",
            customer_email=billing.get("email") or charge.get("receipt_email") or "",
            customer_name=billing.get("name") or "",
            payment_intent_id=charge.get("payment_intent") or "",
        )
    except (KeyError, ValidationError) as exc:
        raise ValueError(f"Failed to parse Stripe charge: {charge}") from exc


def _fetch(
    client: StripeClient,
    start_date: date,
    end_date: date,
) -> Iterator[dict]:
    """Yields raw Stripe charge dicts, paginating over the created date range."""
    created = {
        "gte": _to_timestamp(start_date),
        "lte": _to_timestamp(end_date, end_of_day=True),
    }
    starting_after = None
    while True:
        params: dict = {
            "created": created,
            "limit": PAGE_SIZE,
            "expand": ["data.balance_transaction"],
        }
        if starting_after:
            params["starting_after"] = starting_after
        response = client.charges.list(params=params)
        rows = [dict(charge) for charge in response.data]
        yield from rows
        if not response.has_more or not rows:
            break
        starting_after = rows[-1]["id"]


def _to_timestamp(d: date, end_of_day: bool = False) -> int:
    """Converts a date to a UTC Unix timestamp."""
    dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())
