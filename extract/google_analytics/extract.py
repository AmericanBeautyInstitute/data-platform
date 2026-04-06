"""Google Analytics data extractor."""

from datetime import date

import pyarrow as pa
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from pydantic import BaseModel, ConfigDict, field_validator


class ReportConfig(BaseModel):
    """Defines the dimensions and metrics for a GA4 report request."""

    model_config = ConfigDict(frozen=True)

    dimension_names: list[str]
    metric_names: list[str]


class Raw(BaseModel):
    """Mirrors a single row from the GA4 API response."""

    model_config = ConfigDict(frozen=True)

    date: str
    dimensions: list[str]
    metrics: list[str]


class Record(BaseModel):
    """A validated, typed GA4 performance record."""

    model_config = ConfigDict(frozen=True)

    date: date
    dimensions: dict[str, str]
    metrics: dict[str, str]

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date) -> date:
        """Parses date string in YYYYMMDD format from GA4 API."""
        if isinstance(v, date):
            return v
        return date(int(v[:4]), int(v[4:6]), int(v[6:]))


def extract(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str,
    config: ReportConfig,
) -> pa.Table:
    """Extracts Google Analytics data into a PyArrow table."""
    raw_rows = fetch(client, property_id, start_date, end_date, config)
    records = [parse(r, config) for r in raw_rows]
    table = to_table(records, config)
    return table


def _build_request(
    property_id: str,
    start_date: str,
    end_date: str,
    config: ReportConfig,
) -> RunReportRequest:
    """Builds a GA4 RunReportRequest."""
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in config.dimension_names],
        metrics=[Metric(name=m) for m in config.metric_names],
    )
    return request


def _parse_response(
    response: RunReportResponse,
    config: ReportConfig,
) -> list[Raw]:
    """Parses a GA4 API response into a list of Raw rows."""
    rows = []
    for row in response.rows:
        dimension_values = [v.value for v in row.dimension_values]
        metric_values = [v.value for v in row.metric_values]

        date_index = (
            config.dimension_names.index("date")
            if "date" in config.dimension_names
            else None
        )
        date_value = dimension_values[date_index] if date_index is not None else ""

        raw = Raw(
            date=date_value,
            dimensions=dimension_values,
            metrics=metric_values,
        )
        rows.append(raw)
    return rows


def fetch(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str,
    config: ReportConfig,
) -> list[Raw]:
    """Fetches raw data from the GA4 API."""
    request = _build_request(property_id, start_date, end_date, config)
    response = client.run_report(request)
    raw_rows = _parse_response(response, config)
    return raw_rows


def parse(raw: Raw, config: ReportConfig) -> Record:
    """Converts a Raw GA4 row into a typed Record."""
    dimensions = dict(zip(config.dimension_names, raw.dimensions, strict=True))
    metrics = dict(zip(config.metric_names, raw.metrics, strict=True))
    record = Record(
        date=raw.date,
        dimensions=dimensions,
        metrics=metrics,
    )
    return record


def to_table(records: list[Record], config: ReportConfig) -> pa.Table:
    """Converts a list of Records into a PyArrow table."""
    rows = []
    for record in records:
        row: dict[str, str] = {}
        row.update(record.dimensions)
        row.update(record.metrics)
        row["date"] = record.date.isoformat()
        rows.append(row)
    table = pa.Table.from_pylist(rows)
    return table
