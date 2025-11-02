"""Tests for authenticators."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from helpers.authenticator import (
    google_ads_authenticator,
    google_analytics_authenticator,
)


@pytest.fixture
def mock_google_ads_credentials_file_path(tmp_path: Path) -> Path:
    """Creates a temporary credentials file path for Google Ads."""
    credentials = """\
developer_token: abcdef123456
refresh_token: 1//0abcdefghijklABCDEF
client_id: 123456-abcdef.apps.googleusercontent.com
client_secret: aBcDeFgHiJkL
use_proto_plus: true
"""
    credentials_file_path = tmp_path / "mock_google-ads.yaml"
    credentials_file_path.write_text(credentials)
    return credentials_file_path


@pytest.fixture
def mock_gcp_service_account_key_file_path(tmp_path: Path) -> Path:
    """Creates a temporary credentials file path."""
    credentials_file_path = tmp_path / "mock_gcp_service_account_key.json"
    credentials_file_path.write_text('{"type": "service_account"}')
    return credentials_file_path


@pytest.fixture
def mock_google_ads_client() -> MagicMock:
    """Mocks the Google Ads client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def mock_google_analytics_client() -> MagicMock:
    """Mocks the Google Analytics client."""
    mock_client = MagicMock()
    return mock_client


def test_google_ads_authenticator(
    mocker: MockerFixture,
    mock_google_ads_credentials_file_path: Path,
    mock_google_ads_client: MagicMock,
) -> None:
    """Tests Google Ads authentication."""
    target = "helpers.authenticator.GoogleAdsClient"
    mocker.patch(target=target, return_value=mock_google_ads_client)

    google_ads_client = google_ads_authenticator(mock_google_ads_credentials_file_path)
    assert google_ads_client is not None


def test_google_analytics_authenticator(
    mocker: MockerFixture,
    mock_gcp_service_account_key_file_path: Path,
    mock_google_analytics_client: MagicMock,
) -> None:
    """Tests Google Analytics authentication."""
    target = "helpers.authenticator.BetaAnalyticsDataClient"
    mocker.patch(target=target, return_value=mock_google_analytics_client)

    google_analytics_client = google_analytics_authenticator(
        mock_gcp_service_account_key_file_path
    )
    assert google_analytics_client is not None
