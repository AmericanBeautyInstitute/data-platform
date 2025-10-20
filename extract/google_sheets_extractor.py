"""Google Sheets data Extractor."""

import pyarrow as pa
from googleapiclient.discovery import Resource

from extract.extractor import Extractor
from helpers.google_cloud_platform import authenticate_google_sheets


class GoogleSheetsExtractor(Extractor):
    """Extracts data from Google Sheets."""

    def extract(self, spreadsheet_id: str, sheet_name: str) -> pa.Table:
        """Extracts data from a Google Sheet and returns it as a PyArrow table."""
        service = self.client
        data = self.get_google_sheets_data(service, spreadsheet_id, sheet_name)
        table = self._convert_list_to_table(data)
        return table

    def get_google_sheets_data(
        self,
        service: Resource,
        spreadsheet_id: str,
        sheet_name: str,
    ) -> list[list]:
        """Retrieves data from Google Sheets."""
        sheet = service.spreadsheets()
        response = (
            sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
        )
        sheet_rows = response.get("values", [])
        return sheet_rows

    def _authenticate(self) -> Resource:
        google_sheets_resource = authenticate_google_sheets(self.credentials_file_path)
        return google_sheets_resource

    def _convert_list_to_table(self, data: list[list]) -> pa.Table:
        """Converts data from Google Sheets to a PyArrow table."""
        headers = data[0]
        rows = data[1:]

        columns = list(zip(*rows, strict=True))
        table = pa.table(dict(zip(headers, columns, strict=True)))
        return table
