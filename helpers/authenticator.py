"""Helper functions for Google Cloud Platform related operations."""

from pathlib import Path

from google.ads.googleads.client import GoogleAdsClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build


def bigquery_authenticator(credentials_file_path: Path | str) -> bigquery.Client:
    """Authenticates and returns the BigQuery client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
    bigquery_client = bigquery.Client(credentials=credentials)
    return bigquery_client


def google_ads_authenticator(credentials_file_path: Path | str) -> GoogleAdsClient:
    """Authenticates and returns the Google Ads client.

    credentials_file_path: Path | str
        Path to the Google Ads YAML file.
    """
    google_ads_client = GoogleAdsClient.load_from_storage(str(credentials_file_path))
    return google_ads_client


def google_analytics_authenticator(
    credentials_file_path: Path,
) -> BetaAnalyticsDataClient:
    """Authenticates and returns the Google Analytics client."""
    google_analytics_client = BetaAnalyticsDataClient.from_service_account_file(
        str(credentials_file_path)
    )
    return google_analytics_client


def google_cloud_storage_authenticator(
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
    google_cloud_storage_client = storage.Client(
        credentials=credentials, project=credentials.project_id
    )
    return google_cloud_storage_client


def google_sheets_authenticator(credentials_file_path: Path | str) -> Resource:
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
