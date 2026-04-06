"""Google Ads API client."""

from google.ads.googleads.client import GoogleAdsClient


def build_client(credentials_path: str) -> GoogleAdsClient:
    """Builds an authenticated Google Ads API client."""
    client = GoogleAdsClient.load_from_storage(credentials_path)
    return client
