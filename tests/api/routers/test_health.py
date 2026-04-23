"""Tests for the health router."""

from http import HTTPStatus


def test_health_returns_200(client):
    """Tests that the health endpoint returns HTTP 200."""
    response = client.get("/v1/health")

    assert response.status_code == HTTPStatus.OK


def test_health_body_contains_ok_status(client):
    """Tests that the health response body contains status ok."""
    response = client.get("/v1/health")

    assert response.json()["status"] == "ok"


def test_health_requires_no_auth(client):
    """Tests that the health endpoint is accessible without an API key."""
    response = client.get("/v1/health")

    assert response.status_code == HTTPStatus.OK
