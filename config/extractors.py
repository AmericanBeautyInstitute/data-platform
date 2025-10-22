"""Pydantic base models for extractor configurations."""

from pydantic import BaseModel

from config.env import get_secret


class GoogleSheetsConfig(BaseModel):
    """Configuration for the Google Sheets extractor."""

    spreadsheet_id: str = get_secret("GOOGLE_SHEETS_ID")
    sheet_name: str = get_secret("STUDENT_DATA")
    bucket_name: str = get_secret("BUCKET_NAME")
    blob_name: str = get_secret("STUDENT_DATA")
    credentials_path: str = get_secret("GCP_SERVICE_ACCOUNT_KEY")
