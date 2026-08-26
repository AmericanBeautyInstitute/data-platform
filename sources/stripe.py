"""Stripe charges dlt source."""

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated, Literal

import dlt
from pydantic import BaseModel, ConfigDict, Field, field_validator
from stripe import Charge as StripeCharge
from stripe import StripeClient
from stripe.params import ChargeListParams

PAGE_SIZE = 100
PRIMARY_KEY = "charge_id"
MONEY_DECIMAL_PLACES = 2
MONEY_MAX_DIGITS = 38
USD_CENT = Decimal("0.01")
USD_MINOR_UNITS = Decimal(100)

MoneyAmount = Annotated[
    Decimal,
    Field(
        max_digits=MONEY_MAX_DIGITS,
        decimal_places=MONEY_DECIMAL_PLACES,
    ),
]


class Charge(BaseModel):
    """A validated, typed Stripe charge record."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    charge_id: str
    charge_date: date
    gross_amount_usd: MoneyAmount
    amount_captured_usd: MoneyAmount
    fee_usd: MoneyAmount
    net_usd: MoneyAmount
    currency: Literal["USD"]
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

    @field_validator("charge_id")
    @classmethod
    def validate_charge_id(cls, value: str) -> str:
        """Returns the canonical nonempty Stripe charge identifier."""
        charge_id = value.strip()
        if not charge_id:
            raise ValueError("charge_id must not be empty")
        return charge_id


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
            gross_amount_usd=_parse_minor_units(charge["amount"]),
            amount_captured_usd=_parse_minor_units(charge.get("amount_captured", 0)),
            fee_usd=_parse_minor_units(fees.get("fee", 0)),
            net_usd=_parse_minor_units(fees.get("net", 0)),
            currency=_parse_currency(charge["currency"]),
            status=charge["status"],
            description=charge.get("description") or "",
            customer_email=billing.get("email") or charge.get("receipt_email") or "",
            customer_name=billing.get("name") or "",
            payment_intent_id=charge.get("payment_intent") or "",
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Failed to parse Stripe charge") from exc


def _parse_minor_units(value: object) -> Decimal:
    """Returns Stripe integer minor units as exact USD."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("Stripe minor units must be integers")
    return (Decimal(value) / USD_MINOR_UNITS).quantize(USD_CENT)


def _parse_currency(value: object) -> Literal["USD"]:
    """Returns canonical USD or raises ValueError."""
    if not isinstance(value, str) or value.upper() != "USD":
        raise ValueError("Stripe charge currency must be USD")
    return "USD"


def _fetch(
    client: StripeClient,
    start_date: date,
    end_date: date,
) -> Iterator[dict[str, object]]:
    """Yields raw Stripe charge payloads for the created date range."""
    starting_after: str | None = None

    while True:
        params: ChargeListParams = {
            "created": {
                "gte": _to_timestamp(start_date),
                "lte": _to_timestamp(end_date, end_of_day=True),
            },
            "limit": PAGE_SIZE,
            "expand": ["data.balance_transaction"],
        }
        if starting_after is not None:
            params["starting_after"] = starting_after

        response = client.charges.list(params=params)
        rows: list[StripeCharge] = response.data

        for stripe_charge in rows:
            yield stripe_charge.to_dict_recursive()

        if not response.has_more or not rows:
            break

        starting_after = rows[-1].id


def _to_timestamp(d: date, end_of_day: bool = False) -> int:
    """Converts a date to a UTC Unix timestamp."""
    dt = datetime(d.year, d.month, d.day, tzinfo=UTC)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())
