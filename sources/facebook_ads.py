"""Facebook Ads dlt source."""

from collections.abc import Iterator
from datetime import date

import dlt
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

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
PRIMARY_KEY = ("date", "campaign_id")


class CampaignInsight(BaseModel):
    """A validated, typed Facebook Ads campaign insights record for one day."""

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
        """Parses an ISO date string from the Facebook Ads API."""
        if isinstance(v, date):
            return v
        return date.fromisoformat(v)

    @field_validator("impressions", "clicks", "reach", mode="before")
    @classmethod
    def parse_int(cls, v: str | int) -> int:
        """Parses a string integer field from the Facebook Ads API."""
        return int(v)

    @field_validator("spend_usd", "frequency", mode="before")
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses a string float field from the Facebook Ads API."""
        return float(v)


@dlt.source(name="facebook_ads")
def facebook_ads_source(
    client: AdAccount,
    start_date: date,
    end_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the Facebook Ads resources for an account and date range."""
    yield campaign_insights(client, start_date, end_date)


@dlt.resource(
    name="facebook_ads",
    write_disposition="merge",
    primary_key=PRIMARY_KEY,
    columns=CampaignInsight,
)
def campaign_insights(
    client: AdAccount,
    start_date: date,
    end_date: date,
) -> Iterator[CampaignInsight]:
    """Yields validated Facebook Ads campaign insights for the given dates."""
    for row in _fetch(client, start_date, end_date):
        yield parse(row)


def parse(row: dict) -> CampaignInsight:
    """Converts a raw insights row into a typed CampaignInsight.

    Identity fields (date, campaign) are required and fail loud; every metric
    is guarded with a zero default because the Graph API omits keys on rows
    with no activity.
    """
    actions = {a["action_type"]: a["value"] for a in row.get("actions", [])}
    try:
        return CampaignInsight(
            date=row["date_start"],
            campaign_id=row["campaign_id"],
            campaign_name=row["campaign_name"],
            impressions=row.get("impressions", 0),
            clicks=row.get("clicks", 0),
            spend_usd=row.get("spend", 0),
            reach=row.get("reach", 0),
            frequency=row.get("frequency", 0),
            link_clicks=int(float(actions.get("link_click", 0))),
            leads=int(float(actions.get("lead", 0))),
            conversions=int(
                float(actions.get("offsite_conversion.fb_pixel_purchase", 0))
            ),
        )
    except (KeyError, ValidationError) as exc:
        raise ValueError(f"Failed to parse Facebook Ads row: {row}") from exc


def _fetch(
    client: AdAccount,
    start_date: date,
    end_date: date,
) -> Iterator[dict]:
    """Yields raw campaign insights rows, one per campaign per day.

    TODO: This uses the synchronous insights call, which is fine for daily
    campaign-level pulls. If the account grows many campaigns or the window
    widens, Meta may time out the sync request; switch to the async report
    run (get_insights(..., is_async=True), poll async_status until "Job
    Completed", then iterate get_result()).
    """
    params = {
        "level": "campaign",
        "time_range": {
            "since": start_date.isoformat(),
            "until": end_date.isoformat(),
        },
        "time_increment": 1,
    }
    for row in client.get_insights(fields=INSIGHT_FIELDS, params=params):
        yield dict(row)
