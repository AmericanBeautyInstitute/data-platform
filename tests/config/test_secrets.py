"""Tests for Google Cloud Secret Manager."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound

import config.secrets
from config.secrets import get_secret

EXPECTED_SINGLE_CALL_COUNT = 1
EXPECTED_DOUBLE_CALL_COUNT = 2


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear functools.cache and shared client between tests."""
    get_secret.cache_clear()
    config.secrets._state.clear()
    yield
    get_secret.cache_clear()
    config.secrets._state.clear()


def test_cache_prevents_repeated_secret_manager_calls(monkeypatch):
    """Second call with same args hits cache, not Secret Manager."""
    monkeypatch.delenv("MY_SECRET", raising=False)

    mock_client = MagicMock()
    mock_client.access_secret_version.return_value.payload.data = b"value"

    with patch("config.secrets._get_client", return_value=mock_client):
        get_secret("MY_SECRET", "my-project")
        get_secret("MY_SECRET", "my-project")

    assert mock_client.access_secret_version.call_count == EXPECTED_SINGLE_CALL_COUNT


def test_calls_secret_manager_when_no_env_variable(monkeypatch):
    """Falls through to Secret Manager when no env variable exists."""
    monkeypatch.delenv("MY_SECRET", raising=False)

    mock_client = MagicMock()
    mock_client.access_secret_version.return_value.payload.data = b"secret_value"

    with patch("config.secrets._get_client", return_value=mock_client):
        result = get_secret("MY_SECRET", "my-project")

    assert result == "secret_value"


def test_different_secrets_each_call_secret_manager(monkeypatch):
    """Different secret names each make their own Secret Manager call."""
    monkeypatch.delenv("SECRET_A", raising=False)
    monkeypatch.delenv("SECRET_B", raising=False)

    mock_client = MagicMock()
    mock_client.access_secret_version.return_value.payload.data = b"value"

    with patch("config.secrets._get_client", return_value=mock_client):
        get_secret("SECRET_A", "my-project")
        get_secret("SECRET_B", "my-project")

    assert mock_client.access_secret_version.call_count == EXPECTED_DOUBLE_CALL_COUNT


def test_env_variable_does_not_call_secret_manager(monkeypatch):
    """Secret Manager is never called when env variable exists."""
    monkeypatch.setenv("MY_SECRET", "env_value")

    with patch("config.secrets._get_client") as mock:
        get_secret("MY_SECRET", "my-project")
        mock.assert_not_called()


def test_raises_value_error_preserves_original_cause(monkeypatch):
    """ValueError chains the original exception for debugging."""
    monkeypatch.delenv("MY_SECRET", raising=False)

    original_error = NotFound("Original cause")
    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = original_error

    with (
        patch("config.secrets._get_client", return_value=mock_client),
        pytest.raises(ValueError) as exc_info,
    ):
        get_secret("MY_SECRET", "my-project")

    assert exc_info.value.__cause__ is original_error


def test_raises_value_error_when_secret_not_found(monkeypatch):
    """Raises ValueError with clear message when secret is missing."""
    monkeypatch.delenv("MY_SECRET", raising=False)

    mock_client = MagicMock()
    mock_client.access_secret_version.side_effect = NotFound("Not found")

    with (
        patch("config.secrets._get_client", return_value=mock_client),
        pytest.raises(ValueError, match="MY_SECRET"),
    ):
        get_secret("MY_SECRET", "my-project")


def test_returns_env_variable_when_set(monkeypatch):
    """Environment variable takes precedence over Secret Manager."""
    monkeypatch.setenv("MY_SECRET", "env_value")

    result = get_secret("MY_SECRET", "my-project")

    assert result == "env_value"


def test_secret_manager_called_with_correct_path(monkeypatch):
    """Secret Manager is called with the correct resource path."""
    monkeypatch.delenv("MY_SECRET", raising=False)

    mock_client = MagicMock()
    mock_client.access_secret_version.return_value.payload.data = b"value"

    with patch("config.secrets._get_client", return_value=mock_client):
        get_secret("MY_SECRET", "my-project")

    mock_client.access_secret_version.assert_called_once_with(
        name="projects/my-project/secrets/MY_SECRET/versions/latest"
    )
