"""Google Sheets data extraction."""

import pyarrow as pa
from pydantic import BaseModel, ConfigDict


class Raw(BaseModel):
    """Mirrors the raw row structure returned by the Sheets API."""

    model_config = ConfigDict(frozen=True)

    headers: list[str]
    rows: list[list[str]]


class Record(BaseModel):
    """A single validated row from a Google Sheet."""

    model_config = ConfigDict(frozen=True)

    data: dict[str, str]


def extract(client, spreadsheet_id: str, sheet_name: str) -> pa.Table:
    """Extracts Google Sheet data into a PyArrow table."""
    raw = fetch(client, spreadsheet_id, sheet_name)
    records = parse(raw)
    table = pa.Table.from_pylist([record.data for record in records])
    return table


def fetch(client, spreadsheet_id: str, sheet_name: str) -> Raw:
    """Fetches raw data from a Google Sheet."""
    sheet = client.spreadsheets()
    response = (
        sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    )
    values = response.get("values", [])
    headers = values[0]
    rows = values[1:]
    return Raw(headers=headers, rows=rows)


def parse(raw: Raw) -> list[Record]:
    """Converts a Raw sheet response into a list of Records."""
    records = [
        Record(data=dict(zip(raw.headers, row, strict=True))) for row in raw.rows
    ]
    return records
