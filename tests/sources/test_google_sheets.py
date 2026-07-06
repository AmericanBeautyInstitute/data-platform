"""Tests for the Google Sheets dlt source."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import dlt
import duckdb
import pytest

from sources.google_sheets import (
    _fetch,
    inventory,
    programs,
    students,
)

SPREADSHEET_ID = "mock-spreadsheet-id"
SNAPSHOT_DATE = date(2024, 1, 1)


@pytest.fixture
def mock_client() -> MagicMock:
    """Sheets API client returning two rows with name and status columns."""
    return _mock_client([["name", "status"], ["Alice", "active"], ["Bob", "inactive"]])


def _mock_client(values: list) -> MagicMock:
    """Builds a Sheets API client returning the given values."""
    client = MagicMock()
    client.spreadsheets().values().get().execute.return_value = {"values": values}
    return client


def test_fetch_handles_short_rows() -> None:
    """Tests that rows shorter than the header are zipped without error."""
    client = _mock_client([["name", "status"], ["Alice"]])

    rows = list(_fetch(client, SPREADSHEET_ID, "students", SNAPSHOT_DATE))

    assert rows[0] == {"name": "Alice", "snapshot_date": SNAPSHOT_DATE.isoformat()}


def test_fetch_keys_rows_by_header(mock_client: MagicMock) -> None:
    """Tests that each row is a dict keyed by the header row."""
    rows = list(_fetch(mock_client, SPREADSHEET_ID, "students", SNAPSHOT_DATE))

    assert rows[0]["name"] == "Alice"
    assert rows[0]["status"] == "active"


def test_fetch_returns_empty_for_empty_sheet() -> None:
    """Tests that an empty sheet yields no rows."""
    client = _mock_client([])

    rows = list(_fetch(client, SPREADSHEET_ID, "students", SNAPSHOT_DATE))

    assert rows == []


def test_fetch_stamps_snapshot_date(mock_client: MagicMock) -> None:
    """Tests that each row carries snapshot_date as an ISO string."""
    expected_date = SNAPSHOT_DATE.isoformat()

    rows = list(_fetch(mock_client, SPREADSHEET_ID, "students", SNAPSHOT_DATE))

    assert all(row["snapshot_date"] == expected_date for row in rows)


def test_inventory_uses_inventory_sheet_name(mock_client: MagicMock) -> None:
    """Tests that inventory passes 'inventory' as the range to the Sheets API."""
    list(inventory(mock_client, SPREADSHEET_ID, SNAPSHOT_DATE))

    mock_client.spreadsheets().values().get.assert_called_with(
        spreadsheetId=SPREADSHEET_ID, range="inventory"
    )


def test_pipeline_appends_on_second_run(mock_client: MagicMock, tmp_path: Path) -> None:
    """Tests that a second run appends rows rather than replacing them."""
    expected_rows = 4
    db_path = str(tmp_path / "sheets.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(mock_client, db_path, dlt_dir)
    _run_pipeline(mock_client, db_path, dlt_dir)

    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT count(*) FROM raw.google_sheets_students").fetchone()
    count = result[0] if result else 0

    assert count == expected_rows


def _run_pipeline(client: MagicMock, db_path: str, dlt_dir: str) -> None:
    """Runs the students resource into a duckdb destination."""
    pipeline = dlt.pipeline(
        pipeline_name="sheets_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(students(client, SPREADSHEET_ID, SNAPSHOT_DATE))


def test_pipeline_loads_rows_with_snapshot_date(
    mock_client: MagicMock, tmp_path: Path
) -> None:
    """Tests that the pipeline lands rows with snapshot_date in the destination."""
    expected_rows = 2
    db_path = str(tmp_path / "sheets.duckdb")
    _run_pipeline(mock_client, db_path, str(tmp_path / "dlt"))

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT name, snapshot_date FROM raw.google_sheets_students ORDER BY name"
    ).fetchall()

    assert len(rows) == expected_rows
    assert rows[0] == ("Alice", SNAPSHOT_DATE.isoformat())


def test_programs_uses_programs_sheet_name(mock_client: MagicMock) -> None:
    """Tests that programs passes 'programs' as the range to the Sheets API."""
    list(programs(mock_client, SPREADSHEET_ID, SNAPSHOT_DATE))

    mock_client.spreadsheets().values().get.assert_called_with(
        spreadsheetId=SPREADSHEET_ID, range="programs"
    )


def test_students_uses_students_sheet_name(mock_client: MagicMock) -> None:
    """Tests that students passes 'students' as the range to the Sheets API."""
    list(students(mock_client, SPREADSHEET_ID, SNAPSHOT_DATE))

    mock_client.spreadsheets().values().get.assert_called_with(
        spreadsheetId=SPREADSHEET_ID, range="students"
    )
