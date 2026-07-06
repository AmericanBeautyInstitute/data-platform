"""Google Analytics 4 dlt source."""

from collections.abc import Iterator
from datetime import date

import dlt
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

DIMENSIONS = ("date", "sessionSource", "sessionMedium", "country")
METRICS = ("sessions", "screenPageViews", "bounceRate", "conversions")
PRIMARY_KEY = ("date", "session_source", "session_medium", "country")
_PAGE_SIZE = 100_000


class SessionStat(BaseModel):
    """A validated, typed GA4 sessions record for one day and acquisition grain."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    date: date
    session_source: str = Field(validation_alias="sessionSource")
    session_medium: str = Field(validation_alias="sessionMedium")
    country: str
    sessions: int
    screen_page_views: int = Field(validation_alias="screenPageViews")
    bounce_rate: float = Field(validation_alias="bounceRate")
    conversions: float

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses a GA4 YYYYMMDD date string into a date."""
        if isinstance(v, date):
            return v
        return date(int(v[:4]), int(v[4:6]), int(v[6:8]))

    @field_validator("sessions", "screen_page_views", mode="before")
    @classmethod
    def parse_int(cls, v: str | int) -> int:
        """Parses a GA4 string metric into an integer."""
        return int(v)

    @field_validator("bounce_rate", "conversions", mode="before")
    @classmethod
    def parse_float(cls, v: str | float) -> float:
        """Parses a GA4 string metric into a float."""
        return float(v)


@dlt.source(name="google_analytics")
def google_analytics_source(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: date,
    end_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the Google Analytics resources for a property and date range."""
    yield sessions(client, property_id, start_date, end_date)


@dlt.resource(
    name="google_analytics",
    write_disposition="merge",
    primary_key=PRIMARY_KEY,
    columns=SessionStat,
)
def sessions(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: date,
    end_date: date,
) -> Iterator[SessionStat]:
    """Yields validated GA4 sessions records for the given property and dates."""
    for payload in _fetch(client, property_id, start_date, end_date):
        yield parse(payload)


def parse(payload: dict) -> SessionStat:
    """Converts a header-keyed GA4 row into a typed SessionStat."""
    try:
        return SessionStat.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Failed to parse GA4 row: {payload}") from exc


def _fetch(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: date,
    end_date: date,
) -> Iterator[dict]:
    """Yields raw GA4 rows as header-keyed dicts, paginating over the row cap."""
    offset = 0
    while True:
        response = client.run_report(
            _build_request(property_id, start_date, end_date, offset)
        )
        headers = [h.name for h in response.dimension_headers]
        headers += [h.name for h in response.metric_headers]
        for row in response.rows:
            values = [v.value for v in row.dimension_values]
            values += [v.value for v in row.metric_values]
            yield dict(zip(headers, values, strict=True))
        offset += len(response.rows)
        if not response.rows or offset >= response.row_count:
            break


def _build_request(
    property_id: str,
    start_date: date,
    end_date: date,
    offset: int,
) -> RunReportRequest:
    """Builds a paginated GA4 RunReportRequest for the fixed sessions report."""
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[
            DateRange(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
        ],
        dimensions=[Dimension(name=name) for name in DIMENSIONS],
        metrics=[Metric(name=name) for name in METRICS],
        limit=_PAGE_SIZE,
        offset=offset,
    )
