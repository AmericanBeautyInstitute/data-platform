"""Tests for the Google Ads dlt source."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dlt
import duckdb
import pytest
from pydantic import ValidationError

from sources.google_ads import (
    _build_query,
    _fetch,
    _flatten,
    campaign_performance,
    parse,
)

CUSTOMER_ID = "1234567890"
START_DATE = date(2024, 1, 15)
END_DATE = date(2024, 1, 15)


@pytest.fixture
def api_row() -> dict:
    """A single GoogleAdsRow, as MessageToDict returns it."""
    return {
        "customer": {"id": CUSTOMER_ID},
        "campaign": {"id": "987654321", "name": "Brand"},
        "segments": {"date": "2024-01-15", "device": "MOBILE"},
        "metrics": {
            "impressions": "4210",
            "clicks": "88",
            "costMicros": "5230000",
            "conversions": 3.0,
        },
    }


@pytest.fixture
def mock_client(api_row: dict) -> MagicMock:
    """Google Ads client whose search returns one campaign row."""
    return _mock_client([api_row])


def _mock_client(rows: list[dict]) -> MagicMock:
    """Builds a Google Ads client whose search yields the given rows.

    Each row is wrapped so that row._pb is the dict; tests patch MessageToDict
    to the identity function, matching how _fetch unwraps the protobuf.
    """
    client = MagicMock()
    client.get_service.return_value.search.return_value = [
        SimpleNamespace(_pb=row) for row in rows
    ]
    return client


def test_build_query_filters_date_range() -> None:
    """Tests that the GAQL query filters on the requested date range."""
    query = _build_query(START_DATE, END_DATE)

    assert "FROM campaign" in query
    assert "BETWEEN '2024-01-15'" in query


def test_campaign_performance_is_immutable(api_row: dict) -> None:
    """Tests that CampaignPerformance instances cannot be mutated."""
    result = parse(api_row)

    with pytest.raises(ValidationError):
        result.clicks = 999


def test_fetch_yields_row_dicts(mock_client: MagicMock) -> None:
    """Tests that each GoogleAdsRow is yielded as a dict."""
    with patch("sources.google_ads.MessageToDict", side_effect=lambda pb: pb):
        rows = list(_fetch(mock_client, CUSTOMER_ID, START_DATE, END_DATE))

    assert rows[0]["campaign"]["id"] == "987654321"


def test_flatten_joins_nested_keys_with_underscore(api_row: dict) -> None:
    """Tests that nested keys flatten to underscore-joined leaf names."""
    flat = _flatten(api_row)

    assert flat["campaign_id"] == "987654321"
    assert flat["segments_date"] == "2024-01-15"
    assert flat["metrics_costMicros"] == "5230000"


def test_parse_casts_types(api_row: dict) -> None:
    """Tests that metrics and date are cast to their typed forms."""
    expected_clicks = 88
    expected_impressions = 4210
    expected_cost_micros = 5230000
    expected_conversions = 3.0

    result = parse(api_row)

    assert result.date == date(2024, 1, 15)
    assert result.clicks == expected_clicks
    assert result.impressions == expected_impressions
    assert result.cost_micros == expected_cost_micros
    assert result.conversions == expected_conversions


def test_parse_fails_loud_on_missing_identity_field(api_row: dict) -> None:
    """Tests that a missing identity field raises ValueError, not a default."""
    del api_row["campaign"]

    with pytest.raises(ValueError, match="Failed to parse Google Ads row"):
        parse(api_row)


def test_parse_guards_missing_metrics() -> None:
    """Tests that a row missing the metrics block defaults them to zero."""
    row = {
        "customer": {"id": CUSTOMER_ID},
        "campaign": {"id": "987654321", "name": "Brand"},
        "segments": {"date": "2024-01-15"},
    }

    result = parse(row)

    assert result.clicks == 0
    assert result.impressions == 0
    assert result.cost_micros == 0
    assert result.conversions == 0.0


def test_pipeline_loads_typed_rows(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that the pipeline lands typed, snake_case columns in the destination."""
    expected_rows = 1
    expected_first_row = (date(2024, 1, 15), CUSTOMER_ID, "987654321", 88, 5230000)
    db_path = str(tmp_path / "ads.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date, customer_id, campaign_id, clicks, cost_micros "
        "FROM raw.google_ads ORDER BY date"
    ).fetchall()

    assert rows[0] == expected_first_row
    assert len(rows) == expected_rows


def _run_pipeline(client: MagicMock, db_path: str, dlt_dir: str) -> None:
    """Runs the campaign_performance resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="ads_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    with patch("sources.google_ads.MessageToDict", side_effect=lambda pb: pb):
        pipeline.run(campaign_performance(client, CUSTOMER_ID, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that re-running the same partition upserts rather than duplicating rows."""
    expected_rows = 1
    db_path = str(tmp_path / "ads.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(mock_client, db_path, dlt_dir)
    _run_pipeline(mock_client, db_path, dlt_dir)

    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT count(*) FROM raw.google_ads").fetchone()
    count = result[0] if result else 0

    assert count == expected_rows
