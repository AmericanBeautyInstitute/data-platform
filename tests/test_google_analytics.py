"""Tests for GoogleAnalyticsExtractor."""

from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from extract.google_analytics_extractor import GoogleAnalyticsExtractor


@pytest.fixture
def mock_credentials_file_path() -> Path:
    """Creates a temporary credentials file path."""
    tmp_dir = Path("scratch/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    credentials_file_path = tmp_dir / "mock_gcp_service_account_key.json"
    credentials_file_path.write_text('{"type": "service_account"}')
    return credentials_file_path


@pytest.fixture
def mock_google_analytics_response() -> MagicMock:
    """Creates a mock Google Analytics API response with multiple metrics."""
    mock_row_1 = MagicMock()
    mock_row_1.dimension_values = [MagicMock(value="Japan")]
    mock_row_1.metric_values = [MagicMock(value="100"), MagicMock(value="500")]

    mock_row_2 = MagicMock()
    mock_row_2.dimension_values = [MagicMock(value="USA")]
    mock_row_2.metric_values = [MagicMock(value="200"), MagicMock(value="700")]

    mock_response = MagicMock()
    mock_response.rows = [mock_row_1, mock_row_2]
    return mock_response


def test_extract_ga_data(
    mocker: MockerFixture,
    mock_credentials_file_path: Path,
    mock_google_analytics_response: MagicMock,
) -> None:
    """Tests successful extraction from the Google Analytics API."""
    mock_client = MagicMock()
    mock_client.run_report.return_value = mock_google_analytics_response

    target = "extract.google_analytics_extractor.google_analytics_authenticator"
    mocker.patch(target=target, return_value=mock_client)

    extractor = GoogleAnalyticsExtractor(mock_credentials_file_path)

    metrics = ["sessions", "pageviews"]
    dimensions = ["country"]

    result = extractor.extract(
        property_id="123456",
        start_date="2025-01-01",
        end_date="2025-01-31",
        metrics=metrics,
        dimensions=dimensions,
    )

    mock_client.run_report.assert_called_once()

    expected_rows = len(mock_google_analytics_response.rows)
    expected_columns = len(metrics) + len(dimensions)

    assert isinstance(result, pa.Table)
    assert result.num_rows == expected_rows
    assert result.num_columns == expected_columns
    assert set(result.column_names) == set(dimensions + metrics)
    assert result.column("country").to_pylist() == ["Japan", "USA"]
    assert result.column("sessions").to_pylist() == ["100", "200"]
    assert result.column("pageviews").to_pylist() == ["500", "700"]
