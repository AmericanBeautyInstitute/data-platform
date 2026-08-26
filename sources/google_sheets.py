"""Google Sheets dlt source."""

from collections.abc import Iterator
from datetime import date

import dlt
from googleapiclient.discovery import Resource
from pydantic import BaseModel, ConfigDict

_FIRST_DATA_ROW_NUMBER = 2


class _SheetSpec(BaseModel):
    """Immutable specification for one spreadsheet tab."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sheet_name: str
    headers: tuple[str, ...]
    record_id_header: str


_STUDENTS = _SheetSpec(
    sheet_name="students",
    headers=(
        "student_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "program_id",
        "enrollment_status",
        "enrolled_at",
        "expected_grad_date",
        "actual_grad_date",
    ),
    record_id_header="student_id",
)
_PROGRAMS = _SheetSpec(
    sheet_name="programs",
    headers=(
        "program_id",
        "program_name",
        "program_code",
        "duration_weeks",
        "max_enrollment",
        "is_active",
    ),
    record_id_header="program_id",
)
_INVENTORY = _SheetSpec(
    sheet_name="inventory",
    headers=(
        "sku_id",
        "sku_name",
        "program_id",
        "quantity_on_hand",
        "reorder_threshold",
        "reorder_quantity",
        "unit_cost_usd",
        "units_per_student",
    ),
    record_id_header="sku_id",
)


@dlt.source(name="google_sheets")
def google_sheets_source(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the resources for one dated spreadsheet snapshot."""
    yield students(client, spreadsheet_id, snapshot_date)
    yield programs(client, spreadsheet_id, snapshot_date)
    yield inventory(client, spreadsheet_id, snapshot_date)


@dlt.resource(
    name="google_sheets_students",
    primary_key=["student_id", "snapshot_date"],
    write_disposition="merge",
)
def students(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dict[str, str]]:
    """Yields a validated students snapshot."""
    yield from _fetch(client, spreadsheet_id, _STUDENTS, snapshot_date)


@dlt.resource(
    name="google_sheets_programs",
    primary_key=["program_id", "snapshot_date"],
    write_disposition="merge",
)
def programs(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dict[str, str]]:
    """Yields a validated programs snapshot."""
    yield from _fetch(client, spreadsheet_id, _PROGRAMS, snapshot_date)


@dlt.resource(
    name="google_sheets_inventory",
    primary_key=["sku_id", "snapshot_date"],
    write_disposition="merge",
)
def inventory(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dict[str, str]]:
    """Yields a validated inventory snapshot."""
    yield from _fetch(client, spreadsheet_id, _INVENTORY, snapshot_date)


def _fetch(
    client: Resource,
    spreadsheet_id: str,
    contract: _SheetSpec,
    snapshot_date: date,
) -> Iterator[dict[str, str]]:
    """Yields validated records from one spreadsheet tab."""
    response = (
        client.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=contract.sheet_name)
        .execute()
    )
    values = response.get("values", [])
    if not isinstance(values, list):
        raise ValueError(f"{contract.sheet_name} values must be a list")
    if not values:
        return

    headers = _validate_headers(values[0], contract)
    seen_record_ids: set[str] = set()

    for row_number, row in enumerate(
        values[1:],
        start=_FIRST_DATA_ROW_NUMBER,
    ):
        record = _parse_row(headers, row, contract, row_number)
        record_id = record[contract.record_id_header]
        if record_id in seen_record_ids:
            raise ValueError(
                f"{contract.sheet_name} row {row_number} duplicates "
                f"{contract.record_id_header}={record_id!r}"
            )

        seen_record_ids.add(record_id)
        record["snapshot_date"] = snapshot_date.isoformat()
        yield record


def _parse_row(
    headers: tuple[str, ...],
    row: object,
    contract: _SheetSpec,
    row_number: int,
) -> dict[str, str]:
    """Returns one width-checked row keyed by validated headers."""
    if not isinstance(row, list) or not all(isinstance(cell, str) for cell in row):
        raise ValueError(f"{contract.sheet_name} row {row_number} must contain strings")
    if len(row) > len(headers):
        raise ValueError(
            f"{contract.sheet_name} row {row_number} has more cells than headers"
        )

    missing_cell_count = len(headers) - len(row)
    padded_row = [*row, *("" for _ in range(missing_cell_count))]
    record = dict(zip(headers, padded_row, strict=True))

    record_id = record[contract.record_id_header].strip()
    if not record_id:
        raise ValueError(
            f"{contract.sheet_name} row {row_number} requires "
            f"{contract.record_id_header}"
        )

    record[contract.record_id_header] = record_id
    return record


def _validate_headers(
    raw_headers: object,
    contract: _SheetSpec,
) -> tuple[str, ...]:
    """Returns exact, unique headers or raises ValueError."""
    if not isinstance(raw_headers, list) or not all(
        isinstance(header, str) for header in raw_headers
    ):
        raise ValueError(f"{contract.sheet_name} headers must be strings")

    headers = tuple(raw_headers)
    if any(not header or header != header.strip() for header in headers):
        raise ValueError(f"{contract.sheet_name} headers must be nonempty and trimmed")
    if len(headers) != len(set(headers)):
        raise ValueError(f"{contract.sheet_name} headers must be unique")

    expected_headers = set(contract.headers)
    actual_headers = set(headers)
    missing_headers = sorted(expected_headers - actual_headers)
    unexpected_headers = sorted(actual_headers - expected_headers)

    if missing_headers or unexpected_headers:
        raise ValueError(
            f"{contract.sheet_name} headers do not match the contract: "
            f"missing={missing_headers}, unexpected={unexpected_headers}"
        )

    return headers
