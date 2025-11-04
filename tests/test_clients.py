"""Tests for GCP client builders."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from clients.gcp import (
    build_google_ads_client,
    build_google_analytics_client,
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
    return MagicMock()


@pytest.fixture
def mock_google_analytics_client() -> MagicMock:
    """Mocks the Google Analytics client."""
    return MagicMock()


def test_build_google_ads_client(
    mock_google_ads_credentials_file_path: Path,
    mock_google_ads_client: MagicMock,
) -> None:
    """Tests Google Ads client builder."""
    with patch(
        "clients.gcp.GoogleAdsClient.load_from_storage",
        return_value=mock_google_ads_client,
    ):
        client = build_google_ads_client(mock_google_ads_credentials_file_path)
        assert client is mock_google_ads_client


def test_build_google_analytics_client(
    mock_gcp_service_account_key_file_path: Path,
    mock_google_analytics_client: MagicMock,
) -> None:
    """Tests Google Analytics client builder."""
    with patch(
        "clients.gcp.BetaAnalyticsDataClient.from_service_account_file",
        return_value=mock_google_analytics_client,
    ):
        client = build_google_analytics_client(mock_gcp_service_account_key_file_path)
        assert client is mock_google_analytics_client
