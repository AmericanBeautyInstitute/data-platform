"""Extractor tasks."""

import pyarrow as pa

from extract.google_sheets_extractor import GoogleSheetsExtractor


def extract_from_google_sheets(
    spreadsheet_id: str,
    sheet_name: str,
    credentials_path: str,
) -> pa.Table:
    """Prefect task to extract data from Google Sheets."""
    extractor = GoogleSheetsExtractor(credentials_path)
    google_sheets_data = extractor.extract(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
    )
    return google_sheets_data
