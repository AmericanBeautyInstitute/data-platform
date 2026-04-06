"""The Google Sheets API client."""

from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def build_client(credentials_path: str) -> Resource:
    """Builds an authenticated Google Sheets API client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=SCOPES,
    )
    client = build("sheets", "v4", credentials=credentials)
    return client
