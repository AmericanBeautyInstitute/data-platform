"""Helper functions for Google Cloud Platform related operations."""

from pathlib import Path

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build


def authenticate_google_ads(credentials_file_path: Path | str) -> GoogleAdsClient:
    """Authenticates and returns the Google Ads client.

    credentials_file_path: Path | str
        Path to the Google Ads YAML file.
    """
    google_ads_client = GoogleAdsClient.load_from_storage(str(credentials_file_path))
    return google_ads_client


def authenticate_google_cloud_storage(
    credentials_file_path: Path | str,
) -> storage.Client:
    """Authenticates and returns the Google Cloud Storage client.

    credentials_file_path: Path | str
        Path to the Google Cloud Platform Service Account JSON file.
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    gcs_client = storage.Client(credentials=credentials, project=credentials.project_id)
    return gcs_client


def authenticate_google_sheets(credentials_file_path: Path | str) -> Resource:
    """Authenticates and returns the Google Sheets resource.

    credentials_file_path: Path | str
        Path to the Google Cloud Platform service account JSON file.
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    google_sheets_credentials = build("sheets", "v4", credentials=credentials)
    return google_sheets_credentials
