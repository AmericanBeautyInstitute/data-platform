"""Tests for the Google Sheets dlt source."""

from datetime import date
from pathlib import Path
from typing import Any

import dlt
import duckdb
import pytest
from pytest_mock import MockerFixture

from sources.google_sheets import _STUDENTS, _fetch, students

SPREADSHEET_ID = "mock-spreadsheet-id"
SNAPSHOT_DATE = date(2024, 1, 1)
EXPECTED_STUDENT_ROWS = 2


def _mock_client(
    mocker: MockerFixture,
    values: list[list[str]],
) -> Any:
    """Returns a Sheets API mock serving the supplied rows."""
    client = mocker.MagicMock()
    client.spreadsheets().values().get().execute.return_value = {"values": values}
    return client


def _run_pipeline(client: Any, db_path: str, dlt_dir: str) -> None:
    """Runs the students resource into DuckDB."""
    pipeline = dlt.pipeline(
        pipeline_name="sheets_test",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
        pipelines_dir=dlt_dir,
    )
    pipeline.run(students(client, SPREADSHEET_ID, SNAPSHOT_DATE))


@pytest.fixture
def student_values() -> list[list[str]]:
    """Returns a header and two valid student rows."""
    return [
        list(_STUDENTS.headers),
        [
            "student-1",
            "Alice",
            "Adams",
            "alice@example.com",
            "555-0101",
            "program-1",
            "active",
            "2024-01-01",
            "2024-06-01",
            "",
        ],
        [
            "student-2",
            "Bob",
            "Brown",
            "bob@example.com",
            "555-0102",
            "program-1",
            "inactive",
            "2024-01-02",
            "2024-06-02",
            "2024-05-31",
        ],
    ]


def test_fetch_pads_omitted_trailing_cells(
    mocker: MockerFixture,
    student_values: list[list[str]],
) -> None:
    """Tests that omitted trailing cells become empty strings."""
    student_values[1].pop()
    client = _mock_client(mocker, student_values)

    rows = list(_fetch(client, SPREADSHEET_ID, _STUDENTS, SNAPSHOT_DATE))

    assert rows[0]["actual_grad_date"] == ""
    assert rows[0]["snapshot_date"] == SNAPSHOT_DATE.isoformat()


def test_fetch_rejects_duplicate_headers(
    mocker: MockerFixture,
    student_values: list[list[str]],
) -> None:
    """Tests that duplicate headers fail validation."""
    student_values[0][-1] = "student_id"
    client = _mock_client(mocker, student_values)

    with pytest.raises(ValueError, match="headers must be unique"):
        list(_fetch(client, SPREADSHEET_ID, _STUDENTS, SNAPSHOT_DATE))


def test_fetch_rejects_duplicate_record_ids(
    mocker: MockerFixture,
    student_values: list[list[str]],
) -> None:
    """Tests that a snapshot cannot contain duplicate student IDs."""
    student_values[2][0] = student_values[1][0]
    client = _mock_client(mocker, student_values)

    with pytest.raises(ValueError, match="duplicates student_id"):
        list(_fetch(client, SPREADSHEET_ID, _STUDENTS, SNAPSHOT_DATE))


def test_fetch_rejects_missing_headers(
    mocker: MockerFixture,
    student_values: list[list[str]],
) -> None:
    """Tests that missing contract headers fail validation."""
    student_values[0].pop()
    client = _mock_client(mocker, student_values)

    with pytest.raises(ValueError, match="headers do not match"):
        list(_fetch(client, SPREADSHEET_ID, _STUDENTS, SNAPSHOT_DATE))


def test_fetch_rejects_missing_record_id(
    mocker: MockerFixture,
    student_values: list[list[str]],
) -> None:
    """Tests that every snapshot row has an identifier."""
    student_values[1][0] = " "
    client = _mock_client(mocker, student_values)

    with pytest.raises(ValueError, match="requires student_id"):
        list(_fetch(client, SPREADSHEET_ID, _STUDENTS, SNAPSHOT_DATE))


def test_fetch_rejects_rows_wider_than_headers(
    mocker: MockerFixture,
    student_values: list[list[str]],
) -> None:
    """Tests that extra cells cannot be discarded silently."""
    student_values[1].append("unexpected")
    client = _mock_client(mocker, student_values)

    with pytest.raises(ValueError, match="more cells than headers"):
        list(_fetch(client, SPREADSHEET_ID, _STUDENTS, SNAPSHOT_DATE))


def test_pipeline_merge_is_idempotent(
    mocker: MockerFixture,
    student_values: list[list[str]],
    tmp_path: Path,
) -> None:
    """Tests that rerunning a snapshot preserves its row count."""
    client = _mock_client(mocker, student_values)
    db_path = str(tmp_path / "sheets.duckdb")
    dlt_dir = str(tmp_path / "dlt")

    _run_pipeline(client, db_path, dlt_dir)
    _run_pipeline(client, db_path, dlt_dir)

    with duckdb.connect(db_path) as connection:
        row = connection.execute(
            "SELECT count(*) FROM raw.google_sheets_students"
        ).fetchone()

    assert row is not None
    assert row[0] == EXPECTED_STUDENT_ROWS
