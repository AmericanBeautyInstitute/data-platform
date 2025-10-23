"""Tests for GoogleSheetsExtractor."""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from extract.google_sheets_extractor import GoogleSheetsExtractor


@pytest.fixture
def mock_google_sheets_service() -> MagicMock:
    """Creates a mocked Google Sheets service."""
    mock_service = MagicMock()
    return mock_service


@pytest.fixture
def mock_sheets_data() -> dict[str, list[list[str]]]:
    """Mocks sample Google Sheets data."""
    return {
        "values": [
            ["name", "age", "city"],
            ["Alice", "25", "Tokyo"],
            ["Bob", "30", "New York"],
            ["Relena", "35", "Paris"],
        ]
    }


def test_extraction(
    mock_google_sheets_service: MagicMock,
    mock_sheets_data: dict[str, list[list[str]]],
) -> None:
    """Tests successful data extraction from Google Sheets."""
    mock_google_sheets_service.spreadsheets().values().get().execute.return_value = (
        mock_sheets_data
    )

    extractor = GoogleSheetsExtractor(client=mock_google_sheets_service)
    result = extractor.extract(spreadsheet_id="test_sheet_id", sheet_name="Sheet1")

    expected_rows = len(mock_sheets_data["values"]) - 1  # Exclude header
    expected_columns = len(mock_sheets_data["values"][0])

    assert isinstance(result, pa.Table)
    assert result.num_rows == expected_rows
    assert result.num_columns == expected_columns
    assert result.column_names == ["name", "age", "city"]
    assert result.column("name").to_pylist() == ["Alice", "Bob", "Relena"]
    assert result.column("age").to_pylist() == ["25", "30", "35"]
    assert result.column("city").to_pylist() == ["Tokyo", "New York", "Paris"]
