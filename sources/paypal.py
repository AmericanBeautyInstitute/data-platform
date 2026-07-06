"""PayPal transaction search dlt source."""

from collections.abc import Iterator
from datetime import date

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

PAGE_SIZE = 500
PRIMARY_KEY = "transaction_id"


class Transaction(BaseModel):
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
        """Parses the leading ISO date out of a PayPal timestamp."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(v[:10])

    @field_validator(
        "gross_amount_usd", "fee_amount_usd", "net_amount_usd", mode="before"
    )
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses a string money field from the PayPal API."""
        return float(v)


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
    amount = info.get("transaction_amount", {})
    fee = info.get("fee_amount", {})
    net = info.get("transaction_net_amount", {})
    name = payer.get("payer_name", {})
    full_name = " ".join(
        filter(None, [name.get("given_name", ""), name.get("surname", "")])
    )
    try:
        return Transaction(
            transaction_id=info.get("paypal_reference_id") or info["transaction_id"],
            transaction_date=info["transaction_initiation_date"],
            gross_amount_usd=amount["value"],
            currency_code=amount["currency_code"],
            transaction_status=info["transaction_status"],
            transaction_subject=info.get("transaction_subject", ""),
            payer_email=payer.get("email_address", ""),
            payer_name=full_name,
            fee_amount_usd=fee.get("value", 0),
            net_amount_usd=net.get("value", 0),
        )
    except (KeyError, ValidationError) as exc:
        raise ValueError(f"Failed to parse PayPal transaction: {transaction}") from exc


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
