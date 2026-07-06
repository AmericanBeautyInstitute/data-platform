"""Google Sheets dlt source."""

from collections.abc import Iterator
from datetime import date

import dlt
from googleapiclient.discovery import Resource

SPREADSHEET_SHEETS = ("students", "programs", "inventory")


@dlt.source(name="google_sheets")
def google_sheets_source(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dlt.sources.DltResource]:
    """Groups the Google Sheets resources for a spreadsheet and snapshot date."""
    yield students(client, spreadsheet_id, snapshot_date)
    yield programs(client, spreadsheet_id, snapshot_date)
    yield inventory(client, spreadsheet_id, snapshot_date)


@dlt.resource(name="students", write_disposition="append")
def students(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dict]:
    """Yields a snapshot of the students sheet."""
    yield from _fetch(client, spreadsheet_id, "students", snapshot_date)


@dlt.resource(name="programs", write_disposition="append")
def programs(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dict]:
    """Yields a snapshot of the programs sheet."""
    yield from _fetch(client, spreadsheet_id, "programs", snapshot_date)


@dlt.resource(name="inventory", write_disposition="append")
def inventory(
    client: Resource,
    spreadsheet_id: str,
    snapshot_date: date,
) -> Iterator[dict]:
    """Yields a snapshot of the inventory sheet."""
    yield from _fetch(client, spreadsheet_id, "inventory", snapshot_date)


def _fetch(
    client: Resource,
    spreadsheet_id: str,
    sheet_name: str,
    snapshot_date: date,
) -> Iterator[dict]:
    """Yields header-keyed rows from a sheet with snapshot_date stamped on each."""
    response = (
        client.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=sheet_name)
        .execute()
    )
    values = response.get("values", [])
    if not values:
        return
    headers = values[0]
    for row in values[1:]:
        record = dict(zip(headers, row, strict=False))
        record["snapshot_date"] = snapshot_date.isoformat()
        yield record
