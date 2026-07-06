"""Google Ads dlt source."""

from collections.abc import Iterator
from datetime import date

import dlt
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

PRIMARY_KEY = ("date", "customer_id", "campaign_id")


class CampaignPerformance(BaseModel):
    """A validated, typed Google Ads campaign performance record for one day."""

    model_config = ConfigDict(frozen=True)

    date: date
    customer_id: str
    campaign_id: str
    campaign_name: str
    clicks: int
    impressions: int
    cost_micros: int
    conversions: float

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses an ISO date string from the Google Ads API."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(v)

    @field_validator("clicks", "impressions", "cost_micros", mode="before")
    @classmethod
    def parse_int(cls, v: str | int) -> int:
        """Parses a string integer field from the Google Ads API."""
        return int(v)

    @field_validator("conversions", mode="before")
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses a string float field from the Google Ads API."""
        return float(v)


@dlt.source(name="google_ads")
def google_ads_source(
    client: GoogleAdsClient,
    customer_id: str,
    start_date: date,
    end_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the Google Ads resources for a customer and date range."""
    yield campaign_performance(client, customer_id, start_date, end_date)


@dlt.resource(
    name="google_ads",
    write_disposition="merge",
    primary_key=PRIMARY_KEY,
    columns=CampaignPerformance,
)
def campaign_performance(
    client: GoogleAdsClient,
    customer_id: str,
    start_date: date,
    end_date: date,
) -> Iterator[CampaignPerformance]:
    """Yields validated Google Ads campaign performance for the given dates."""
    for row in _fetch(client, customer_id, start_date, end_date):
        yield parse(row)


def parse(row: dict) -> CampaignPerformance:
    """Converts a raw GoogleAdsRow dict into a typed CampaignPerformance.

    Identity fields (date, customer, campaign) are required and fail loud;
    every metric is guarded with a zero default because the Google Ads API
    omits zero-valued fields from a row.
    """
    flat = _flatten(row)
    try:
        return CampaignPerformance(
            date=flat["segments_date"],
            customer_id=flat["customer_id"],
            campaign_id=flat["campaign_id"],
            campaign_name=flat["campaign_name"],
            clicks=flat.get("metrics_clicks", 0),
            impressions=flat.get("metrics_impressions", 0),
            cost_micros=flat.get("metrics_costMicros", 0),
            conversions=flat.get("metrics_conversions", 0),
        )
    except (KeyError, ValidationError) as exc:
        raise ValueError(f"Failed to parse Google Ads row: {row}") from exc


def _fetch(
    client: GoogleAdsClient,
    customer_id: str,
    start_date: date,
    end_date: date,
) -> Iterator[dict]:
    """Yields raw GoogleAdsRow dicts, one per campaign per day."""
    service = client.get_service("GoogleAdsService")
    response = service.search(
        customer_id=customer_id,
        query=_build_query(start_date, end_date),
    )
    for row in response:
        yield MessageToDict(row._pb)


def _build_query(start_date: date, end_date: date) -> str:
    """Builds the GAQL query for campaign performance over a date range."""
    return f"""
        SELECT
            customer.id,
            campaign.id,
            campaign.name,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date.isoformat()}'
            AND '{end_date.isoformat()}'
    """


def _flatten(row: dict, prefix: str = "") -> dict:
    """Flattens a nested GoogleAdsRow dict into underscore-joined keys.

    Example: {"campaign": {"id": "1"}} becomes {"campaign_id": "1"}. Preserves
    the API's camelCase leaf names, so metrics.cost_micros arrives as
    metrics_costMicros.
    """
    flat: dict = {}
    for key, value in row.items():
        full_key = f"{prefix}{key}"
        if isinstance(value, dict):
            flat |= _flatten(value, f"{full_key}_")
        else:
            flat[full_key] = value
    return flat
