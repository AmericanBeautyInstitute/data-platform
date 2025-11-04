"""Clients for Google Cloud Platform services."""

from pathlib import Path

from google.ads.googleads.client import GoogleAdsClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build


def build_bigquery_client(credentials_file_path: Path | str) -> bigquery.Client:
    """Builds the BigQuery client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
    bigquery_client = bigquery.Client(credentials=credentials)
    return bigquery_client


def build_google_ads_client(credentials_file_path: Path | str) -> GoogleAdsClient:
    """Builds the Google Ads client."""
    google_ads_client = GoogleAdsClient.load_from_storage(str(credentials_file_path))
    return google_ads_client


def build_google_analytics_client(
    credentials_file_path: Path,
) -> BetaAnalyticsDataClient:
    """Builds the Google Analytics client."""
    google_analytics_client = BetaAnalyticsDataClient.from_service_account_file(
        str(credentials_file_path)
    )
    return google_analytics_client


def build_google_cloud_storage_client(
    credentials_file_path: Path | str,
) -> storage.Client:
    """Builds the Google Cloud Storage client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    google_cloud_storage_client = storage.Client(
        credentials=credentials, project=credentials.project_id
    )
    return google_cloud_storage_client


def build_google_sheets_client(credentials_file_path: Path | str) -> Resource:
    """Builds the Google Sheets client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    google_sheets_credentials = build("sheets", "v4", credentials=credentials)
    return google_sheets_credentials
