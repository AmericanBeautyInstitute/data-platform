"""Tests for ingestion layer resources."""

from unittest.mock import patch

from dagster_gcp import BigQueryResource, GCSResource

from orchestration.defs.ingestion.resources import (
    FacebookAdsResource,
    GoogleAdsResource,
    GoogleAnalyticsResource,
    GoogleSheetsResource,
    PayPalResource,
    StripeResource,
    bigquery_resource,
    facebook_ads_resource,
    gcs_resource,
    google_ads_resource,
    google_analytics_resource,
    google_sheets_resource,
    paypal_resource,
    stripe_resource,
)

_RESOURCES_MODULE = "orchestration.defs.ingestion.resources"

MOCK_CREDENTIALS_PATH = "/tmp/creds.json"
MOCK_SPREADSHEET_ID = "mock-spreadsheet-id"
MOCK_PROPERTY_ID = "123456"
MOCK_CUSTOMER_ID = "1234567890"
MOCK_ACCESS_TOKEN = "mock-access-token"
MOCK_AD_ACCOUNT_ID = "act_123456789"
MOCK_CLIENT_ID = "mock-client-id"
MOCK_CLIENT_SECRET = "mock-client-secret"
MOCK_SECRET_KEY = "sk_test_mock"


def test_bigquery_resource_is_correct_type():
    """bigquery_resource is a BigQueryResource instance."""
    assert isinstance(bigquery_resource, BigQueryResource)


def test_facebook_ads_resource_exposes_ad_account_id():
    """FacebookAdsResource exposes ad_account_id field."""
    resource = FacebookAdsResource(
        access_token=MOCK_ACCESS_TOKEN,
        ad_account_id=MOCK_AD_ACCOUNT_ID,
    )
    assert resource.ad_account_id == MOCK_AD_ACCOUNT_ID


def test_facebook_ads_resource_get_client_builds_ad_account():
    """get_client() inits the API and returns an AdAccount for the account id."""
    resource = FacebookAdsResource(
        access_token=MOCK_ACCESS_TOKEN,
        ad_account_id=MOCK_AD_ACCOUNT_ID,
    )
    with (
        patch(f"{_RESOURCES_MODULE}.FacebookAdsApi.init") as mock_init,
        patch(f"{_RESOURCES_MODULE}.AdAccount") as mock_ad_account,
    ):
        resource.get_client()
        mock_init.assert_called_once_with(access_token=MOCK_ACCESS_TOKEN)
        mock_ad_account.assert_called_once_with(MOCK_AD_ACCOUNT_ID)


def test_facebook_ads_resource_is_correct_type():
    """facebook_ads_resource is a FacebookAdsResource instance."""
    assert isinstance(facebook_ads_resource, FacebookAdsResource)


def test_gcs_resource_is_correct_type():
    """gcs_resource is a GCSResource instance."""
    assert isinstance(gcs_resource, GCSResource)


def test_google_ads_resource_exposes_customer_id():
    """GoogleAdsResource exposes customer_id field."""
    resource = GoogleAdsResource(
        credentials_path=MOCK_CREDENTIALS_PATH,
        customer_id=MOCK_CUSTOMER_ID,
    )
    assert resource.customer_id == MOCK_CUSTOMER_ID


def test_google_ads_resource_get_client_loads_from_storage():
    """get_client() loads the Google Ads client from the credentials file."""
    resource = GoogleAdsResource(
        credentials_path=MOCK_CREDENTIALS_PATH,
        customer_id=MOCK_CUSTOMER_ID,
    )
    with patch(f"{_RESOURCES_MODULE}.GoogleAdsClient.load_from_storage") as mock_load:
        resource.get_client()
        mock_load.assert_called_once_with(MOCK_CREDENTIALS_PATH)


def test_google_ads_resource_is_correct_type():
    """google_ads_resource is a GoogleAdsResource instance."""
    assert isinstance(google_ads_resource, GoogleAdsResource)


def test_google_analytics_resource_exposes_property_id():
    """GoogleAnalyticsResource exposes property_id field."""
    resource = GoogleAnalyticsResource(
        credentials_path=MOCK_CREDENTIALS_PATH,
        property_id=MOCK_PROPERTY_ID,
    )
    assert resource.property_id == MOCK_PROPERTY_ID


def test_google_analytics_resource_get_client_calls_build_client():
    """get_client() calls BetaAnalyticsDataClient.from_service_account_file."""
    resource = GoogleAnalyticsResource(
        credentials_path=MOCK_CREDENTIALS_PATH,
        property_id=MOCK_PROPERTY_ID,
    )
    with patch(
        f"{_RESOURCES_MODULE}.BetaAnalyticsDataClient.from_service_account_file"
    ) as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(MOCK_CREDENTIALS_PATH)


def test_google_analytics_resource_is_correct_type():
    """google_analytics_resource is a GoogleAnalyticsResource instance."""
    assert isinstance(google_analytics_resource, GoogleAnalyticsResource)


def test_google_sheets_resource_exposes_spreadsheet_id():
    """GoogleSheetsResource exposes spreadsheet_id field."""
    resource = GoogleSheetsResource(
        credentials_path=MOCK_CREDENTIALS_PATH,
        spreadsheet_id=MOCK_SPREADSHEET_ID,
    )
    assert resource.spreadsheet_id == MOCK_SPREADSHEET_ID


def test_google_sheets_resource_get_client_calls_build():
    """get_client() calls googleapiclient build with sheets v4 credentials."""
    resource = GoogleSheetsResource(
        credentials_path=MOCK_CREDENTIALS_PATH,
        spreadsheet_id=MOCK_SPREADSHEET_ID,
    )
    with (
        patch(
            f"{_RESOURCES_MODULE}.service_account.Credentials.from_service_account_file"
        ),
        patch(f"{_RESOURCES_MODULE}.build") as mock_build,
    ):
        resource.get_client()
        mock_build.assert_called_once()


def test_google_sheets_resource_is_correct_type():
    """google_sheets_resource is a GoogleSheetsResource instance."""
    assert isinstance(google_sheets_resource, GoogleSheetsResource)


def test_paypal_resource_exposes_client_id():
    """PayPalResource exposes client_id field."""
    resource = PayPalResource(
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
    )
    assert resource.client_id == MOCK_CLIENT_ID


def test_paypal_resource_get_client_calls_build_client():
    """get_client() delegates to paypal_client.build_client."""
    resource = PayPalResource(
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
    )
    with patch(f"{_RESOURCES_MODULE}.paypal_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(MOCK_CLIENT_ID, MOCK_CLIENT_SECRET)


def test_paypal_resource_is_correct_type():
    """paypal_resource is a PayPalResource instance."""
    assert isinstance(paypal_resource, PayPalResource)


def test_stripe_resource_exposes_secret_key():
    """StripeResource exposes secret_key field."""
    resource = StripeResource(secret_key=MOCK_SECRET_KEY)
    assert resource.secret_key == MOCK_SECRET_KEY


def test_stripe_resource_get_client_calls_build_client():
    """get_client() delegates to stripe_client.build_client."""
    resource = StripeResource(secret_key=MOCK_SECRET_KEY)
    with patch(f"{_RESOURCES_MODULE}.stripe_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(MOCK_SECRET_KEY)


def test_stripe_resource_is_correct_type():
    """stripe_resource is a StripeResource instance."""
    assert isinstance(stripe_resource, StripeResource)
