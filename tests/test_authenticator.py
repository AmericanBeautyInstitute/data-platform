"""Tests for authenticators."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from helpers.google_cloud_platform import authenticate_google_ads


@pytest.fixture
def mock_credentials_file_path() -> Path:
    """Creates a temporary credentials file path."""
    tmp_dir = Path("scratch/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    credentials = """\
developer_token: abcdef123456
refresh_token: 1//0abcdefghijklABCDEF
client_id: 123456-abcdef.apps.googleusercontent.com
client_secret: aBcDeFgHiJkL
use_proto_plus: true
"""
    credentials_file_path = tmp_dir / "mock_google-ads.yaml"
    credentials_file_path.write_text(credentials)
    return credentials_file_path


@pytest.fixture
def mock_google_ads_client():
    """Mocks the Google Ads client."""
    mock_client = MagicMock()
    return mock_client


def test_authenticate_google_ads(
    mocker: MockerFixture,
    mock_credentials_file_path: Path,
    mock_google_ads_client: MagicMock,
) -> None:
    """Tests Google Ads authentication."""
    target = "helpers.google_cloud_platform.GoogleAdsClient"
    mocker.patch(target=target, return_value=mock_google_ads_client)

    google_ads_client = authenticate_google_ads(mock_credentials_file_path)
    assert google_ads_client is not None
