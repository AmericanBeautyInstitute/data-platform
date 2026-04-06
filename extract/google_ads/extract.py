"""Google Ads data extractor."""

from datetime import date

import pyarrow as pa
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, field_validator


class Raw(BaseModel):
    """Mirrors the Google Ads API protobuf response for a performance row."""

    model_config = ConfigDict(frozen=True)

    date: str
    clicks: str
    impressions: str
    cost_micros: str
    conversions: str
    customer_id: str


class Record(BaseModel):
    """A validated, typed Google Ads performance record."""

    model_config = ConfigDict(frozen=True)

    date: date
    clicks: int
    impressions: int
    cost_micros: int
    conversions: float
    customer_id: str

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses ISO date string from Google Ads API."""
        if isinstance(v, date):
            return v
        parsed = date.fromisoformat(v)
        return parsed

    @field_validator("clicks", "impressions", "cost_micros", mode="before")
    @classmethod
    def parse_int(cls, v: str | int) -> int:
        """Parses string integer fields from Google Ads API."""
        parsed = int(v)
        return parsed

    @field_validator("conversions", mode="before")
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses string float fields from Google Ads API."""
        parsed = float(v)
        return parsed


def extract(
    client: GoogleAdsClient,
    customer_id: str,
    query: str,
) -> pa.Table:
    """Extracts Google Ads data into a PyArrow table."""
    raw_rows = fetch(client, customer_id, query)
    records = [parse(r) for r in raw_rows]
    table = to_table(records)
    return table


def _flatten_row(row_dict: dict) -> dict:
    """Flattens a nested protobuf dict into dot-notation keys.

    Example:
        {"segments": {"date": "2024-01-15"}, "metrics": {"clicks": "10"}}
        becomes {"segments.date": "2024-01-15", "metrics.clicks": "10"}
    """
    flattened: dict = {}
    for key, value in row_dict.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                flat_key = f"{key}.{nested_key}"
                flattened[flat_key] = nested_value
        else:
            flattened[key] = value
    return flattened


def _to_raw(row_dict: dict, customer_id: str) -> Raw:
    """Converts a flattened protobuf dict into a Raw instance."""
    raw = Raw(
        date=row_dict["segments.date"],
        clicks=row_dict["metrics.clicks"],
        impressions=row_dict["metrics.impressions"],
        cost_micros=row_dict["metrics.costMicros"],
        conversions=row_dict["metrics.conversions"],
        customer_id=customer_id,
    )
    return raw


def fetch(
    client: GoogleAdsClient,
    customer_id: str,
    query: str,
) -> list[Raw]:
    """Fetches raw data from the Google Ads API."""
    service = client.get_service("GoogleAdsService")
    response = service.search(customer_id=customer_id, query=query)
    raw_rows = [
        _to_raw(_flatten_row(MessageToDict(row._pb)), customer_id) for row in response
    ]
    return raw_rows


def parse(raw: Raw) -> Record:
    """Converts a Raw Google Ads row into a typed Record."""
    record = Record(
        date=raw.date,
        clicks=raw.clicks,
        impressions=raw.impressions,
        cost_micros=raw.cost_micros,
        conversions=raw.conversions,
        customer_id=raw.customer_id,
    )
    return record


def to_table(records: list[Record]) -> pa.Table:
    """Converts a list of Records into a PyArrow table."""
    rows = [
        {
            "date": record.date.isoformat(),
            "clicks": record.clicks,
            "impressions": record.impressions,
            "cost_micros": record.cost_micros,
            "conversions": record.conversions,
            "customer_id": record.customer_id,
        }
        for record in records
    ]
    table = pa.Table.from_pylist(rows)
    return table
