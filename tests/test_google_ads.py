"""Tests for GoogleAdsExtractor."""

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from extract.google_ads_extractor import GoogleAdsExtractor


def mock_google_ads_row(date: str, clicks: str, impressions: str) -> MagicMock:
    """Simulates a Google Ads protobuf row for testing."""
    row_pb = MagicMock()
    row_pb.segments = {"date": date}
    row_pb.metrics = {"clicks": clicks, "impressions": impressions}
    row = MagicMock(_pb=row_pb)
    return row


@pytest.fixture
def mock_google_ads_response() -> list[MagicMock]:
    """Creates a list of mocked Google Ads API response rows."""
    return [
        mock_google_ads_row("2025-01-01", "10", "100"),
        mock_google_ads_row("2025-01-02", "15", "150"),
    ]


@pytest.fixture
def mock_google_ads_client() -> MagicMock:
    """Creates a mocked Google Ads client."""
    mock_client = MagicMock()
    return mock_client


def test_google_ads_extractor(
    mocker: MockerFixture,
    mock_google_ads_client: MagicMock,
    mock_google_ads_response: list[MagicMock],
) -> None:
    """Tests successful extraction and conversion to PyArrow table."""
    mock_service = MagicMock()
    mock_service.search.return_value = mock_google_ads_response
    mock_google_ads_client.get_service.return_value = mock_service

    def protobuf_side_effect(protobuf_object: Any) -> dict[str, Any]:
        """Returns a protobuf side effect."""
        return {
            "segments": {"date": protobuf_object.segments["date"]},
            "metrics": {
                "clicks": protobuf_object.metrics["clicks"],
                "impressions": protobuf_object.metrics["impressions"],
            },
        }

    mocker.patch(
        target="extract.google_ads_extractor.MessageToDict",
        side_effect=protobuf_side_effect,
    )

    extractor = GoogleAdsExtractor(client=mock_google_ads_client)
    query = "SELECT segments.date, metrics.clicks, metrics.impressions FROM customer"
    result = extractor.extract(customer_id="123456", query=query)

    mock_google_ads_client.get_service.assert_called_once_with("GoogleAdsService")
    mock_service.search.assert_called_once_with(
        customer_id="123456",
        query=query,
        login_customer_id=None,
    )

    # Validate PyArrow table
    assert isinstance(result, pa.Table)
    expected_columns = ["metrics.clicks", "metrics.impressions", "segments.date"]
    assert set(result.column_names) == set(expected_columns)
    assert result.num_rows == len(mock_google_ads_response)

    # Validate data content
    assert result.column("segments.date").to_pylist() == ["2025-01-01", "2025-01-02"]
    assert result.column("metrics.clicks").to_pylist() == ["10", "15"]
    assert result.column("metrics.impressions").to_pylist() == ["100", "150"]
