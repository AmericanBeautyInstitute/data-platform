"""Stripe API client."""

from stripe import StripeClient


def build_client(secret_key: str) -> StripeClient:
    """Builds an authenticated Stripe API client."""
    return StripeClient(secret_key)
