"""Facebook Ads data extractor."""

from datetime import date

import pyarrow as pa
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from extract.table import to_table

INSIGHT_FIELDS = [
    AdsInsights.Field.date_start,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.reach,
    AdsInsights.Field.frequency,
    AdsInsights.Field.actions,
]


class Action(BaseModel):
    """Mirrors a single entry from the Facebook Ads actions list."""

    model_config = ConfigDict(frozen=True)

    action_type: str
    value: str


class Raw(BaseModel):
    """Mirrors a single row from the Facebook Ads Insights API response."""

    model_config = ConfigDict(frozen=True)

    date_start: str
    campaign_id: str
    campaign_name: str
    impressions: str
    clicks: str
    spend: str
    reach: str
    frequency: str
    actions: list[Action]


class Record(BaseModel):
    """A validated, typed Facebook Ads campaign insights record."""

    model_config = ConfigDict(frozen=True)

    date: date
    campaign_id: str
    campaign_name: str
    impressions: int
    clicks: int
    spend_usd: float
    reach: int
    frequency: float
    link_clicks: int
    leads: int
    conversions: int

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses ISO date string from Facebook Ads API."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(v)

    @field_validator("impressions", "clicks", "reach", mode="before")
    @classmethod
    def parse_int(cls, v: str | int) -> int:
        """Parses string integer fields from Facebook Ads API."""
        return int(v)

    @field_validator("spend_usd", "frequency", mode="before")
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses string float fields from Facebook Ads API."""
        return float(v)


def extract(client: AdAccount, start_date: date, end_date: date) -> pa.Table:
    """Extracts Facebook Ads campaign insights into a PyArrow table."""
    raw_rows = fetch(client, start_date, end_date)
    records = [parse(r) for r in raw_rows]
    table = to_table(records)
    return table


def fetch(client: AdAccount, start_date: date, end_date: date) -> list[Raw]:
    """Fetches raw campaign insights from the Facebook Ads API."""
    params = {
        "level": "campaign",
        "time_range": {
            "since": start_date.isoformat(),
            "until": end_date.isoformat(),
        },
        "time_increment": 1,
    }
    insights = client.get_insights(fields=INSIGHT_FIELDS, params=params)
    raw_rows = [_to_raw(dict(row)) for row in insights]
    return raw_rows


def _to_raw(row: dict) -> Raw:
    """Converts a Facebook Ads API row dict into a Raw instance."""
    return Raw(
        date_start=row["date_start"],
        campaign_id=row["campaign_id"],
        campaign_name=row["campaign_name"],
        impressions=row["impressions"],
        clicks=row["clicks"],
        spend=row["spend"],
        reach=row["reach"],
        frequency=row["frequency"],
        actions=row.get("actions", []),
    )


def parse(raw: Raw) -> Record:
    """Converts a Raw Facebook Ads row into a typed Record."""
    actions = {a.action_type: a.value for a in raw.actions}
    try:
        return Record(
            date=raw.date_start,
            campaign_id=raw.campaign_id,
            campaign_name=raw.campaign_name,
            impressions=raw.impressions,
            clicks=raw.clicks,
            spend_usd=raw.spend,
            reach=raw.reach,
            frequency=raw.frequency,
            link_clicks=int(float(actions.get("link_click", "0"))),
            leads=int(float(actions.get("lead", "0"))),
            conversions=int(
                float(
                    actions.get(
                        "offsite_conversion.fb_pixel_purchase",
                        "0",
                    )
                )
            ),
        )
    except ValidationError as exc:
        raise ValueError(f"Failed to parse Facebook Ads row: {raw}") from exc
