"""Google Cloud Secret Manager client."""

import functools
import os

from google.cloud import secretmanager


@functools.cache
def get_secret(name: str, project: str) -> str:
    """Retrieves a secret from GCP Secret Manager.

    Falls back to environment variables for local development.
    Each unique (name, project) pair is fetched at most once
    per process lifetime via lru_cache.

    Raises ValueError if the secret is not found in either
    the environment or Secret Manager.
    """
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value

    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project}/secrets/{name}/versions/latest"

    try:
        response = client.access_secret_version(name=secret_path)
        secret_value = response.payload.data.decode("utf-8")
        return secret_value
    except Exception as e:
        raise ValueError(
            f"Secret '{name}' not found in Secret Manager "
            f"and no environment variable fallback exists."
        ) from e
