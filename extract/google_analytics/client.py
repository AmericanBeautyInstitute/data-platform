"""Google Analytics Data API client."""

from google.analytics.data_v1beta import BetaAnalyticsDataClient


def build_client(credentials_path: str) -> BetaAnalyticsDataClient:
    """Builds an authenticated Google Analytics Data API client."""
    client = BetaAnalyticsDataClient.from_service_account_file(credentials_path)
    return client
