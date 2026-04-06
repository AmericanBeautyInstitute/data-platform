"""Tests for ingestion layer resources."""

from unittest.mock import patch

from dagster_gcp import BigQueryResource, GCSResource

from assets.ingestion.resources import (
    GoogleAdsResource,
    GoogleAnalyticsResource,
    GoogleSheetsResource,
    bigquery_resource,
    gcs_resource,
    google_ads_resource,
    google_analytics_resource,
    google_sheets_resource,
)

FAKE_CREDENTIALS_PATH = "/tmp/creds.json"
FAKE_SPREADSHEET_ID = "fake-spreadsheet-id"
FAKE_PROPERTY_ID = "123456"
FAKE_CUSTOMER_ID = "1234567890"


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
