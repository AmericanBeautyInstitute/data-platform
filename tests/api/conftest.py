"""Shared fixtures for API tests."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.config import Settings
from api.dependencies import get_dagster_client
from api.main import create_app

TEST_API_KEY = "test-key"


@pytest.fixture
def mock_dagster_client():
    """Mocked Dagster GraphQL client."""
    return MagicMock()


@pytest.fixture
def client(mock_dagster_client):
    """TestClient with settings and Dagster client overridden for testing."""
    app = create_app(settings=Settings(api_key=TEST_API_KEY))
    app.dependency_overrides[get_dagster_client] = lambda: mock_dagster_client
    with TestClient(app) as c:
        yield c


@pytest.fixture
def authed_headers():
    """Authorization headers with the test API key."""
    return {"X-Api-Key": TEST_API_KEY}
