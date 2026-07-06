"""Tests for the Facebook Ads dlt source."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import dlt
import duckdb
import pytest
from pydantic import ValidationError

from sources.facebook_ads import (
    _fetch,
    campaign_insights,
    parse,
)

START_DATE = date(2024, 1, 15)
END_DATE = date(2024, 1, 15)


@pytest.fixture
def api_row() -> dict:
    """A single Facebook Ads insights row, as _fetch yields it."""
    return {
        "date_start": "2024-01-15",
        "campaign_id": "601",
        "campaign_name": "Brand",
        "impressions": "1000",
        "clicks": "50",
        "spend": "25.50",
        "reach": "900",
        "frequency": "1.11",
        "actions": [
            {"action_type": "link_click", "value": "45"},
            {"action_type": "lead", "value": "3"},
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "1"},
        ],
    }


@pytest.fixture
def mock_client(api_row: dict) -> MagicMock:
    """Facebook Ads client returning two campaign rows."""
    summer = {
        "date_start": "2024-01-16",
        "campaign_id": "602",
        "campaign_name": "Summer",
        "impressions": "2000",
        "clicks": "100",
        "spend": "50.00",
        "reach": "1800",
        "frequency": "1.11",
        "actions": [],
    }
    client = MagicMock()
    client.get_insights.return_value = [api_row, summer]
    return client


def test_campaign_insight_is_immutable(api_row: dict) -> None:
    """Tests that CampaignInsight instances cannot be mutated."""
    result = parse(api_row)

    with pytest.raises(ValidationError):
        result.impressions = 999


def test_fetch_sets_time_range_and_level(mock_client: MagicMock) -> None:
    """Tests that fetch requests campaign-level insights for the date range."""
    list(_fetch(mock_client, START_DATE, END_DATE))

    params = mock_client.get_insights.call_args.kwargs["params"]
    assert params["level"] == "campaign"
    assert params["time_range"]["since"] == "2024-01-15"
    assert params["time_range"]["until"] == "2024-01-15"


def test_fetch_yields_row_dicts(mock_client: MagicMock) -> None:
    """Tests that each raw row is yielded as a dict."""
    rows = list(_fetch(mock_client, START_DATE, END_DATE))

    assert rows[0]["campaign_id"] == "601"


def test_parse_casts_types(api_row: dict) -> None:
    """Tests that metrics and date are cast to their typed forms."""
    expected_impressions = 1000
    expected_spend = 25.50
    expected_frequency = 1.11

    result = parse(api_row)

    assert result.date == date(2024, 1, 15)
    assert result.impressions == expected_impressions
    assert result.spend_usd == expected_spend
    assert result.frequency == expected_frequency


def test_parse_explodes_actions_into_columns(api_row: dict) -> None:
    """Tests that action types are exploded into named integer columns."""
    expected_link_clicks = 45
    expected_leads = 3
    expected_conversions = 1

    result = parse(api_row)

    assert result.link_clicks == expected_link_clicks
    assert result.leads == expected_leads
    assert result.conversions == expected_conversions


def test_parse_fails_loud_on_missing_identity_field(api_row: dict) -> None:
    """Tests that a missing identity field raises ValueError, not a default."""
    del api_row["campaign_id"]

    with pytest.raises(ValueError, match="Failed to parse Facebook Ads row"):
        parse(api_row)


def test_parse_guards_missing_metrics() -> None:
    """Tests that a ragged row missing metric keys defaults them to zero."""
    ragged_row = {
        "date_start": "2024-01-15",
        "campaign_id": "601",
        "campaign_name": "Brand",
    }

    result = parse(ragged_row)

    assert result.impressions == 0
    assert result.spend_usd == 0.0
    assert result.link_clicks == 0
    assert result.conversions == 0


def test_pipeline_loads_typed_rows(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that the pipeline lands typed, snake_case columns in the destination."""
    expected_rows = 2
    expected_first_row = (date(2024, 1, 15), "601", 1000, 25.50)
    db_path = str(tmp_path / "fb.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date, campaign_id, impressions, spend_usd "
        "FROM raw.facebook_ads ORDER BY date"
    ).fetchall()

    assert rows[0] == expected_first_row
    assert len(rows) == expected_rows


def _run_pipeline(client: MagicMock, db_path: str, dlt_dir: str) -> None:
    """Runs the campaign_insights resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="fb_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(campaign_insights(client, START_DATE, END_DATE))


def test_pipeline_merge_is_idempotent(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that re-running the same partition upserts rather than duplicating rows."""
    expected_rows = 2
    db_path = str(tmp_path / "fb.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(mock_client, db_path, dlt_dir)
    _run_pipeline(mock_client, db_path, dlt_dir)

    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT count(*) FROM raw.facebook_ads").fetchone()
    count = result[0] if result else 0

    assert count == expected_rows
