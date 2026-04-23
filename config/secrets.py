"""Google Cloud Secret Manager client."""

import functools
import os

from google.api_core.exceptions import GoogleAPIError
from google.cloud import secretmanager

_state: dict[str, secretmanager.SecretManagerServiceClient] = {}


@functools.cache
def get_secret(name: str, project: str) -> str:
    """Retrieves a secret from GCP Secret Manager.

    Falls back to environment variables for local development.
    Each unique (name, project) pair is fetched at most once
    per process lifetime via functools.cache.

    Raises ValueError if the secret is not found in either
    the environment or Secret Manager.
    """
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value

    client = _get_client()
    secret_path = f"projects/{project}/secrets/{name}/versions/latest"

    try:
        response = client.access_secret_version(name=secret_path)
        secret_value = response.payload.data.decode("utf-8")
        return secret_value
    except GoogleAPIError as e:
        raise ValueError(
            f"Secret '{name}' not found in Secret Manager "
            f"and no environment variable fallback exists."
        ) from e


def _get_client() -> secretmanager.SecretManagerServiceClient:
    """Returns a shared Secret Manager client, created on first use."""
    if "client" not in _state:
        _state["client"] = secretmanager.SecretManagerServiceClient()
    return _state["client"]
