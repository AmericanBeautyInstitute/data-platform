"""PayPal REST API client."""

import requests

PAYPAL_BASE_URL = "https://api-m.paypal.com"


class PayPalClient:
    """Authenticated PayPal REST API client."""

    def __init__(self, client_id: str, client_secret: str) -> None:
        """Initializes the client and fetches an access token."""
        self.base_url = PAYPAL_BASE_URL
        self._access_token = self._fetch_token(client_id, client_secret)

    def _fetch_token(self, client_id: str, client_secret: str) -> str:
        """Exchanges client credentials for an OAuth2 access token."""
        response = requests.post(
            f"{self.base_url}/v1/oauth2/token",
            headers={"Accept": "application/json"},
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def get(self, path: str, params: dict | None = None) -> dict:
        """Makes an authenticated GET request to the PayPal API."""
        response = requests.get(
            f"{self.base_url}{path}",
            headers={"Authorization": f"Bearer {self._access_token}"},
            params=params or {},
        )
        response.raise_for_status()
        return response.json()


def build_client(client_id: str, client_secret: str) -> PayPalClient:
    """Builds an authenticated PayPal REST API client."""
    return PayPalClient(client_id, client_secret)
