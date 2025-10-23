"""Extractor tasks."""

import pyarrow as pa

from extract.google_sheets_extractor import GoogleSheetsExtractor
from helpers.authenticator import google_sheets_authenticator


def extract_from_google_sheets(
    spreadsheet_id: str,
    sheet_name: str,
    credentials_file_path: str,
) -> pa.Table:
    """Prefect task to extract data from Google Sheets."""
    google_sheets_client = google_sheets_authenticator(credentials_file_path)
    extractor = GoogleSheetsExtractor(google_sheets_client)
    google_sheets_data = extractor.extract(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
    )
    return google_sheets_data
