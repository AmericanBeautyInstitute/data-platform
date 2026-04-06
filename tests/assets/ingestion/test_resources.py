"""Tests for ingestion layer resources."""

from unittest.mock import patch

from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.resources import (
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

FAKE_CREDENTIALS_PATH = "/tmp/creds.json"
FAKE_SPREADSHEET_ID = "fake-spreadsheet-id"
FAKE_PROPERTY_ID = "123456"
FAKE_CUSTOMER_ID = "1234567890"
FAKE_ACCESS_TOKEN = "fake-access-token"
FAKE_AD_ACCOUNT_ID = "act_123456789"
FAKE_CLIENT_ID = "fake-client-id"
FAKE_CLIENT_SECRET = "fake-client-secret"
FAKE_SECRET_KEY = "sk_test_fake"


def test_gcs_resource_is_correct_type():
    """gcs_resource is a GCSResource instance."""
    assert isinstance(gcs_resource, GCSResource)


def test_bigquery_resource_is_correct_type():
    """bigquery_resource is a BigQueryResource instance."""
    assert isinstance(bigquery_resource, BigQueryResource)


def test_google_sheets_resource_is_correct_type():
    """google_sheets_resource is a GoogleSheetsResource instance."""
    assert isinstance(google_sheets_resource, GoogleSheetsResource)


def test_google_analytics_resource_is_correct_type():
    """google_analytics_resource is a GoogleAnalyticsResource instance."""
    assert isinstance(google_analytics_resource, GoogleAnalyticsResource)


def test_google_ads_resource_is_correct_type():
    """google_ads_resource is a GoogleAdsResource instance."""
    assert isinstance(google_ads_resource, GoogleAdsResource)


def test_facebook_ads_resource_is_correct_type():
    """facebook_ads_resource is a FacebookAdsResource instance."""
    assert isinstance(facebook_ads_resource, FacebookAdsResource)


def test_paypal_resource_is_correct_type():
    """paypal_resource is a PayPalResource instance."""
    assert isinstance(paypal_resource, PayPalResource)


def test_stripe_resource_is_correct_type():
    """stripe_resource is a StripeResource instance."""
    assert isinstance(stripe_resource, StripeResource)


def test_google_sheets_resource_exposes_spreadsheet_id():
    """GoogleSheetsResource exposes spreadsheet_id field."""
    resource = GoogleSheetsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        spreadsheet_id=FAKE_SPREADSHEET_ID,
    )
    assert resource.spreadsheet_id == FAKE_SPREADSHEET_ID


def test_google_analytics_resource_exposes_property_id():
    """GoogleAnalyticsResource exposes property_id field."""
    resource = GoogleAnalyticsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        property_id=FAKE_PROPERTY_ID,
    )
    assert resource.property_id == FAKE_PROPERTY_ID


def test_google_ads_resource_exposes_customer_id():
    """GoogleAdsResource exposes customer_id field."""
    resource = GoogleAdsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        customer_id=FAKE_CUSTOMER_ID,
    )
    assert resource.customer_id == FAKE_CUSTOMER_ID


def test_facebook_ads_resource_exposes_ad_account_id():
    """FacebookAdsResource exposes ad_account_id field."""
    resource = FacebookAdsResource(
        access_token=FAKE_ACCESS_TOKEN,
        ad_account_id=FAKE_AD_ACCOUNT_ID,
    )
    assert resource.ad_account_id == FAKE_AD_ACCOUNT_ID


def test_paypal_resource_exposes_client_id():
    """PayPalResource exposes client_id field."""
    resource = PayPalResource(
        client_id=FAKE_CLIENT_ID,
        client_secret=FAKE_CLIENT_SECRET,
    )
    assert resource.client_id == FAKE_CLIENT_ID


def test_stripe_resource_exposes_secret_key():
    """StripeResource exposes secret_key field."""
    resource = StripeResource(secret_key=FAKE_SECRET_KEY)
    assert resource.secret_key == FAKE_SECRET_KEY


def test_google_sheets_resource_get_client_calls_build_client():
    """get_client() delegates to sheets_client.build_client."""
    resource = GoogleSheetsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        spreadsheet_id=FAKE_SPREADSHEET_ID,
    )
    with patch("assets.ingestion.resources.sheets_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(FAKE_CREDENTIALS_PATH)


def test_google_analytics_resource_get_client_calls_build_client():
    """get_client() delegates to ga_client.build_client."""
    resource = GoogleAnalyticsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        property_id=FAKE_PROPERTY_ID,
    )
    with patch("assets.ingestion.resources.ga_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(FAKE_CREDENTIALS_PATH)


def test_google_ads_resource_get_client_calls_build_client():
    """get_client() delegates to ads_client.build_client."""
    resource = GoogleAdsResource(
        credentials_path=FAKE_CREDENTIALS_PATH,
        customer_id=FAKE_CUSTOMER_ID,
    )
    with patch("assets.ingestion.resources.ads_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(FAKE_CREDENTIALS_PATH)


def test_facebook_ads_resource_get_client_calls_build_client():
    """get_client() delegates to fb_client.build_client."""
    resource = FacebookAdsResource(
        access_token=FAKE_ACCESS_TOKEN,
        ad_account_id=FAKE_AD_ACCOUNT_ID,
    )
    with patch("assets.ingestion.resources.fb_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(FAKE_ACCESS_TOKEN, FAKE_AD_ACCOUNT_ID)


def test_paypal_resource_get_client_calls_build_client():
    """get_client() delegates to paypal_client.build_client."""
    resource = PayPalResource(
        client_id=FAKE_CLIENT_ID,
        client_secret=FAKE_CLIENT_SECRET,
    )
    with patch("assets.ingestion.resources.paypal_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(FAKE_CLIENT_ID, FAKE_CLIENT_SECRET)


def test_stripe_resource_get_client_calls_build_client():
    """get_client() delegates to stripe_client.build_client."""
    resource = StripeResource(secret_key=FAKE_SECRET_KEY)
    with patch("assets.ingestion.resources.stripe_client.build_client") as mock_build:
        resource.get_client()
        mock_build.assert_called_once_with(FAKE_SECRET_KEY)
