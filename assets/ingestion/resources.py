"""Dagster resources for GCP infrastructure and source API clients."""

from dagster import ConfigurableResource, EnvVar
from dagster_gcp import BigQueryResource, GCSResource
from googleapiclient.discovery import Resource

from extract.google_ads import client as ads_client
from extract.google_ads.client import GoogleAdsClient
from extract.google_analytics import client as ga_client
from extract.google_sheets import client as sheets_client


class GoogleSheetsResource(ConfigurableResource):
    """Resource for authenticating with the Google Sheets API."""

    credentials_path: str
    spreadsheet_id: str

    def get_client(self) -> Resource:
        """Builds and returns an authenticated Google Sheets API client."""
        return sheets_client.build_client(self.credentials_path)


class GoogleAnalyticsResource(ConfigurableResource):
    """Resource for authenticating with the Google Analytics Data API."""

    credentials_path: str
    property_id: str

    def get_client(self):
        """Builds and returns an authenticated Google Analytics API client."""
        return ga_client.build_client(self.credentials_path)


class GoogleAdsResource(ConfigurableResource):
    """Resource for authenticating with the Google Ads API."""

    credentials_path: str
    customer_id: str

    def get_client(self) -> GoogleAdsClient:
        """Builds and returns an authenticated Google Ads API client."""
        return ads_client.build_client(self.credentials_path)


gcs_resource = GCSResource(project=EnvVar("GCP_PROJECT_ID"))
bigquery_resource = BigQueryResource(project=EnvVar("GCP_PROJECT_ID"))

google_sheets_resource = GoogleSheetsResource(
    credentials_path=EnvVar("GOOGLE_SHEETS_CREDENTIALS_PATH"),
    spreadsheet_id=EnvVar("GOOGLE_SHEETS_SPREADSHEET_ID"),
)

google_analytics_resource = GoogleAnalyticsResource(
    credentials_path=EnvVar("GOOGLE_ANALYTICS_CREDENTIALS_PATH"),
    property_id=EnvVar("GOOGLE_ANALYTICS_PROPERTY_ID"),
)

google_ads_resource = GoogleAdsResource(
    credentials_path=EnvVar("GOOGLE_ADS_CREDENTIALS_PATH"),
    customer_id=EnvVar("GOOGLE_ADS_CUSTOMER_ID"),
)
