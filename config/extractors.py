"""Pydantic base models for extractor configurations."""

from pydantic import BaseModel

from config.env import get_secret


class GoogleSheetsExtractorConfig(BaseModel):
    """Configuration for the Google Sheets extractor."""

    spreadsheet_id: str = get_secret("GOOGLE_SHEETS_ID")
    sheet_names: list[str] = ["students", "programs", "inventory"]
    bucket_name: str = get_secret("BUCKET_NAME")
    blob_name: str = ""
    credentials_file_path: str = get_secret("GCP_SERVICE_ACCOUNT_KEY")
