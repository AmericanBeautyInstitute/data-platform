"""Facebook Ads API client."""

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi


def build_client(access_token: str, ad_account_id: str) -> AdAccount:
    """Builds an authenticated Facebook Ads API client."""
    FacebookAdsApi.init(access_token=access_token)
    return AdAccount(ad_account_id)
