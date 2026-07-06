"""Dagster resources for GCP infrastructure and source API clients."""

from dagster import ConfigurableResource, EnvVar
from dagster_gcp import BigQueryResource, GCSResource
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from google.ads.googleads.client import GoogleAdsClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from stripe import StripeClient


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
        creds = service_account.Credentials.from_service_account_file(
            self.credentials_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        return build("sheets", "v4", credentials=creds)


class GoogleAnalyticsResource(ConfigurableResource):
    """Resource for authenticating with the Google Analytics Data API."""

    credentials_path: str
    property_id: str

    def get_client(self) -> BetaAnalyticsDataClient:
        """Builds and returns an authenticated Google Analytics API client."""
        return BetaAnalyticsDataClient.from_service_account_file(self.credentials_path)


class GoogleAdsResource(ConfigurableResource):
    """Resource for authenticating with the Google Ads API."""

    credentials_path: str
    customer_id: str

    def get_client(self) -> GoogleAdsClient:
        """Builds and returns an authenticated Google Ads API client."""
        return GoogleAdsClient.load_from_storage(self.credentials_path)


class FacebookAdsResource(ConfigurableResource):
    """Resource for authenticating with the Facebook Marketing API."""

    access_token: str
    ad_account_id: str

    def get_client(self) -> AdAccount:
        """Builds and returns an authenticated Facebook Ads API client."""
        FacebookAdsApi.init(access_token=self.access_token)
        return AdAccount(self.ad_account_id)


class PayPalResource(ConfigurableResource):
    """Resource for authenticating with the PayPal REST API."""

    client_id: str
    client_secret: str

    def get_client(self) -> RESTClient:
        """Builds and returns an authenticated PayPal REST client."""
        base_url = "https://api-m.paypal.com"
        return RESTClient(
            base_url=base_url,
            auth=_PayPalOAuth(
                access_token_url=f"{base_url}/v1/oauth2/token",
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
        )


class _PayPalOAuth(OAuth2ClientCredentials):
    """OAuth2 client-credentials auth that sends PayPal's Basic-auth token request."""

    def build_access_token_request(self) -> dict:
        """Builds the token request using HTTP Basic auth, as PayPal requires."""
        return {
            "headers": {"Accept": "application/json"},
            "auth": (self.client_id, self.client_secret),
            "data": {"grant_type": "client_credentials"},
        }


class StripeResource(ConfigurableResource):
    """Resource for authenticating with the Stripe API."""

    secret_key: str

    def get_client(self) -> StripeClient:
        """Builds and returns an authenticated Stripe API client."""
        return StripeClient(self.secret_key)


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
