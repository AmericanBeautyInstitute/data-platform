"""Dagster resources for GCP infrastructure and source API clients."""

from dagster import ConfigurableResource, EnvVar
from dagster_gcp import BigQueryResource, GCSResource
from googleapiclient.discovery import Resource
from stripe import StripeClient

from extract.facebook_ads import client as fb_client
from extract.facebook_ads.client import AdAccount
from extract.google_ads import client as ads_client
from extract.google_ads.client import GoogleAdsClient
from extract.google_analytics import client as ga_client
from extract.google_sheets import client as sheets_client
from extract.paypal import client as paypal_client
from extract.paypal.client import PayPalClient
from extract.stripe import client as stripe_client


class IngestionConfig(ConfigurableResource):
    """Shared GCP project and bucket config for all ingestion assets."""

    project: str
    bucket: str


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


class FacebookAdsResource(ConfigurableResource):
    """Resource for authenticating with the Facebook Marketing API."""

    access_token: str
    ad_account_id: str

    def get_client(self) -> AdAccount:
        """Builds and returns an authenticated Facebook Ads API client."""
        return fb_client.build_client(self.access_token, self.ad_account_id)


class PayPalResource(ConfigurableResource):
    """Resource for authenticating with the PayPal REST API."""

    client_id: str
    client_secret: str

    def get_client(self) -> PayPalClient:
        """Builds and returns an authenticated PayPal REST API client."""
        return paypal_client.build_client(self.client_id, self.client_secret)


class StripeResource(ConfigurableResource):
    """Resource for authenticating with the Stripe API."""

    secret_key: str

    def get_client(self) -> StripeClient:
        """Builds and returns an authenticated Stripe API client."""
        return stripe_client.build_client(self.secret_key)


ingestion_env = IngestionConfig(
    project=EnvVar("GCP_PROJECT_ID"),
    bucket=EnvVar("GCS_BUCKET"),
)

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

facebook_ads_resource = FacebookAdsResource(
    access_token=EnvVar("FACEBOOK_ADS_ACCESS_TOKEN"),
    ad_account_id=EnvVar("FACEBOOK_ADS_ACCOUNT_ID"),
)

paypal_resource = PayPalResource(
    client_id=EnvVar("PAYPAL_CLIENT_ID"),
    client_secret=EnvVar("PAYPAL_CLIENT_SECRET"),
)

stripe_resource = StripeResource(
    secret_key=EnvVar("STRIPE_SECRET_KEY"),
)
