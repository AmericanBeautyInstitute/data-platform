"""Helper functions for Google Cloud Platform related operations."""

from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build


def authenticate_google_sheets(credentials_file_path: Path | str) -> Resource:
    """Authenticates and builds a Google Sheets resource."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    google_sheets_credentials = build("sheets", "v4", credentials=credentials)
    return google_sheets_credentials


def authenticate_google_cloud_storage(
    credentials_file_path: Path | str,
) -> storage.Client:
    """Authenticates and returns Google Cloud Storage client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    gcs_client = storage.Client(credentials=credentials, project=credentials.project_id)
    return gcs_client
