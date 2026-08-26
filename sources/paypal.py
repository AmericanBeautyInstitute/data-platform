"""PayPal transaction search dlt source."""

from collections.abc import Iterator
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, cast

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from pydantic import BaseModel, ConfigDict, Field, field_validator

PAGE_SIZE = 500
PRIMARY_KEY = "transaction_id"
MONEY_DECIMAL_PLACES = 2
MONEY_MAX_DIGITS = 38
USD_CENT = Decimal("0.01")
ZERO_USD = Decimal("0.00")

MoneyAmount = Annotated[
    Decimal,
    Field(
        max_digits=MONEY_MAX_DIGITS,
        decimal_places=MONEY_DECIMAL_PLACES,
    ),
]


class Transaction(BaseModel):
    """A validated, typed PayPal transaction record."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    transaction_id: str
    paypal_reference_id: str
    transaction_date: date
    gross_amount_usd: MoneyAmount
    currency_code: Literal["USD"]
    transaction_status: str
    transaction_subject: str
    payer_email: str
    payer_name: str
    fee_amount_usd: MoneyAmount
    net_amount_usd: MoneyAmount

    @field_validator("transaction_date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses the leading ISO date out of a PayPal timestamp."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(v[:10])

    @field_validator("transaction_id")
    @classmethod
    def validate_transaction_id(cls, value: str) -> str:
        """Returns the canonical nonempty PayPal transaction ID."""
        transaction_id = value.strip()
        if not transaction_id:
            raise ValueError("transaction_id must not be empty")
        return transaction_id


@dlt.source(name="paypal")
def paypal_source(
    client: RESTClient,
    start_date: date,
    end_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the PayPal resources for a date range."""
    yield transactions(client, start_date, end_date)


@dlt.resource(
    name="paypal_transactions",
    write_disposition="merge",
    primary_key=PRIMARY_KEY,
    columns=Transaction,
)
def transactions(
    client: RESTClient,
    start_date: date,
    end_date: date,
) -> Iterator[Transaction]:
    """Yields validated PayPal transactions for the given date range."""
    for row in _fetch(client, start_date, end_date):
        yield parse(row)


def parse(transaction: dict) -> Transaction:
    """Converts a raw PayPal transaction into a typed Transaction.

    Identity and core money fields are required and fail loud; optional
    descriptive fields default to empty because PayPal omits them.
    """
    info = transaction.get("transaction_info", {})
    payer = transaction.get("payer_info", {})
    amount = info.get("transaction_amount")
    fee = info.get("fee_amount")
    net = info.get("transaction_net_amount")
    name = payer.get("payer_name", {})
    full_name = " ".join(
        filter(None, [name.get("given_name", ""), name.get("surname", "")])
    )
    try:
        return Transaction(
            transaction_id=info["transaction_id"],
            paypal_reference_id=info.get("paypal_reference_id") or "",
            transaction_date=info["transaction_initiation_date"],
            gross_amount_usd=_parse_money(amount, required=True),
            currency_code="USD",
            transaction_status=info["transaction_status"],
            transaction_subject=info.get("transaction_subject", ""),
            payer_email=payer.get("email_address", ""),
            payer_name=full_name,
            fee_amount_usd=_parse_money(fee, required=False),
            net_amount_usd=_parse_money(net, required=True),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Failed to parse PayPal transaction") from exc


def _parse_money(money: object, *, required: bool) -> Decimal:
    """Returns a PayPal money object as exact USD."""
    if money is None and not required:
        return ZERO_USD
    if not isinstance(money, dict):
        raise TypeError("PayPal money must be an object")

    money_by_field = cast(dict[str, object], money)
    if money_by_field.get("currency_code") != "USD":
        raise ValueError("PayPal money currency must be USD")

    value = money_by_field["value"]
    if not isinstance(value, str):
        raise TypeError("PayPal money value must be a string")

    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("PayPal money value is invalid") from exc

    if not amount.is_finite():
        raise ValueError("PayPal money value must be finite")

    exponent = amount.as_tuple().exponent
    if not isinstance(exponent, int):
        raise ValueError("PayPal money value must be finite")
    if exponent < -MONEY_DECIMAL_PLACES:
        raise ValueError("PayPal money value must be cent-exact")

    try:
        return amount.quantize(USD_CENT)
    except InvalidOperation as exc:
        raise ValueError("PayPal money value is invalid") from exc


def _fetch(
    client: RESTClient,
    start_date: date,
    end_date: date,
) -> Iterator[dict]:
    """Yields raw PayPal transaction dicts, paginating over the date range."""
    params = {
        "start_date": f"{start_date.isoformat()}T00:00:00-0000",
        "end_date": f"{end_date.isoformat()}T23:59:59-0000",
        "fields": "all",
        "page_size": PAGE_SIZE,
    }
    pages = client.paginate(
        "/v1/reporting/transactions",
        params=params,
        paginator=PageNumberPaginator(base_page=1, total_path="total_pages"),
        data_selector="transaction_details",
    )
    for page in pages:
        yield from page
