"""Tests for the Google Analytics dlt source."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import dlt
import duckdb
import pytest
from pydantic import ValidationError

from sources.google_analytics import (
    DIMENSIONS,
    METRICS,
    _build_request,
    _fetch,
    parse,
    sessions,
)

PROPERTY_ID = "123456"
START_DATE = date(2024, 1, 1)
END_DATE = date(2024, 1, 31)


@pytest.fixture
def payload() -> dict[str, str]:
    """A single header-keyed GA4 row, as _fetch yields it."""
    return {
        "date": "20240101",
        "sessionSource": "google",
        "sessionMedium": "cpc",
        "country": "Japan",
        "sessions": "100",
        "screenPageViews": "500",
        "bounceRate": "0.25",
        "conversions": "3",
    }


@pytest.fixture
def mock_client() -> MagicMock:
    """GA4 client returning one page of two rows."""
    tokyo = _mock_row(
        ["20240101", "google", "cpc", "Japan"], ["100", "500", "0.25", "3"]
    )
    usa = _mock_row(
        ["20240102", "(direct)", "(none)", "USA"], ["200", "700", "0.1", "5"]
    )
    client = MagicMock()
    client.run_report.return_value = _mock_response([tokyo, usa])
    return client


def _mock_row(dimensions: list[str], metrics: list[str]) -> SimpleNamespace:
    """Builds a GA4 response row."""
    return SimpleNamespace(
        dimension_values=[SimpleNamespace(value=v) for v in dimensions],
        metric_values=[SimpleNamespace(value=v) for v in metrics],
    )


def _mock_response(rows: list, row_count: int | None = None) -> SimpleNamespace:
    """Builds a GA4 RunReportResponse."""
    return SimpleNamespace(
        dimension_headers=[_mock_header(name) for name in DIMENSIONS],
        metric_headers=[_mock_header(name) for name in METRICS],
        rows=rows,
        row_count=row_count if row_count is not None else len(rows),
    )


def _mock_header(name: str) -> SimpleNamespace:
    """Builds a GA4 response header."""
    return SimpleNamespace(name=name)


def test_build_request_sets_dimensions_and_metrics() -> None:
    """Tests that the request wraps the fixed dimension and metric names."""
    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, offset=0)

    assert tuple(d.name for d in request.dimensions) == DIMENSIONS
    assert tuple(m.name for m in request.metrics) == METRICS


def test_build_request_sets_offset() -> None:
    """Tests that the request carries the pagination offset."""
    offset = 500

    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, offset=offset)

    assert request.offset == offset


def test_build_request_sets_property_and_dates() -> None:
    """Tests that the request carries the property path and ISO date range."""
    request = _build_request(PROPERTY_ID, START_DATE, END_DATE, offset=0)

    assert request.property == f"properties/{PROPERTY_ID}"
    assert request.date_ranges[0].start_date == "2024-01-01"
    assert request.date_ranges[0].end_date == "2024-01-31"


def test_fetch_keys_rows_by_header(
    mock_client: MagicMock, payload: dict[str, str]
) -> None:
    """Tests that each raw row is a dict keyed by GA4 header names."""
    rows = list(_fetch(mock_client, PROPERTY_ID, START_DATE, END_DATE))

    assert rows[0] == payload


def test_fetch_paginates_until_row_count_reached() -> None:
    """Tests that fetch keeps requesting until the reported row_count is covered."""
    expected_rows = 2
    expected_pages = 2
    tokyo = _mock_row(
        ["20240101", "google", "cpc", "Japan"], ["100", "500", "0.25", "3"]
    )
    usa = _mock_row(
        ["20240102", "(direct)", "(none)", "USA"], ["200", "700", "0.1", "5"]
    )
    client = MagicMock()
    client.run_report.side_effect = [
        _mock_response([tokyo], row_count=expected_rows),
        _mock_response([usa], row_count=expected_rows),
    ]

    rows = list(_fetch(client, PROPERTY_ID, START_DATE, END_DATE))

    assert len(rows) == expected_rows
    assert client.run_report.call_count == expected_pages


def test_fetch_single_page_calls_api_once(mock_client: MagicMock) -> None:
    """Tests that a single page of results issues one API call."""
    list(_fetch(mock_client, PROPERTY_ID, START_DATE, END_DATE))

    mock_client.run_report.assert_called_once()


def test_parse_applies_camelcase_aliases(payload: dict[str, str]) -> None:
    """Tests that GA4 camelCase keys map onto snake_case fields."""
    result = parse(payload)

    assert result.session_source == "google"
    assert result.session_medium == "cpc"


def test_parse_casts_types(payload: dict[str, str]) -> None:
    """Tests that metrics and date are cast to their typed forms."""
    expected_sessions = 100
    expected_page_views = 500
    expected_bounce_rate = 0.25
    expected_conversions = 3.0

    result = parse(payload)

    assert result.date == date(2024, 1, 1)
    assert result.sessions == expected_sessions
    assert result.screen_page_views == expected_page_views
    assert result.bounce_rate == expected_bounce_rate
    assert result.conversions == expected_conversions


def test_parse_fails_loud_on_missing_field(payload: dict[str, str]) -> None:
    """Tests that a missing field raises a ValueError, not a silent default."""
    del payload["sessions"]

    with pytest.raises(ValueError, match="Failed to parse GA4 row"):
        parse(payload)


def test_pipeline_loads_typed_rows(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that the pipeline lands typed, snake_case columns in the destination."""
    expected_rows = 2
    expected_first_row = (date(2024, 1, 1), "google", 100, 0.25)
    db_path = str(tmp_path / "ga.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date, session_source, sessions, bounce_rate "
        "FROM raw.google_analytics ORDER BY date"
    ).fetchall()

    assert rows[0] == expected_first_row
    assert len(rows) == expected_rows


def _run_pipeline(client: MagicMock, db_path: str, dlt_dir: str) -> None:
    """Runs the sessions resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="ga_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(sessions(client, PROPERTY_ID, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that re-running the same partition upserts rather than duplicating rows."""
    expected_rows = 2
    db_path = str(tmp_path / "ga.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(mock_client, db_path, dlt_dir)
    _run_pipeline(mock_client, db_path, dlt_dir)

    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT count(*) FROM raw.google_analytics").fetchone()
    count = result[0] if result else 0

    assert count == expected_rows


def test_session_stat_is_immutable(payload: dict[str, str]) -> None:
    """Tests that SessionStat instances cannot be mutated."""
    result = parse(payload)

    with pytest.raises(ValidationError):
        result.sessions = 999
