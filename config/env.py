"""Retrieves secrets from environment variables."""

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


def get_secret(variable_name: str) -> str:
    """Retrieves value of a secret from environment variables."""
    secret_value = os.getenv(variable_name)
    assert secret_value is not None
    return secret_value
