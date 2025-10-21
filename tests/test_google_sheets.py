"""Tests for GoogleSheetsExtractor."""

from pathlib import Path

import pyarrow as pa
import pytest
from pytest_mock import MockerFixture

from extract.google_sheets_extractor import GoogleSheetsExtractor


@pytest.fixture
def mock_credentials_file_path() -> Path:
    """Creates a temporary credentials file path."""
    tmp_dir = Path("scratch/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    credentials_file_path = tmp_dir / "mock_gcp_service_account_key.json"
    credentials_file_path.write_text('{"type": "service_account"}')
    return credentials_file_path


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
    mocker: MockerFixture,
    mock_credentials_file_path: Path,
    mock_sheets_data: dict[str, list[list[str]]],
) -> None:
    """Tests successful data extraction from Google Sheets."""
    mock_service = mocker.MagicMock()
    mock_service.spreadsheets().values().get().execute.return_value = mock_sheets_data

    target = "extract.google_sheets_extractor.google_sheets_authenticator"

    mocker.patch(target=target, return_value=mock_service)

    extractor = GoogleSheetsExtractor(mock_credentials_file_path)
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
