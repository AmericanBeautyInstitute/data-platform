"""Tests for the pipelines router."""

from http import HTTPStatus

from dagster import DagsterRunStatus

TRIGGER_URL = "/v1/pipelines/runs"


def test_trigger_run_returns_201(client, authed_headers, mock_dagster_client):
    """Tests that a successful run trigger returns HTTP 201."""
    mock_dagster_client.submit_job_execution.return_value = "run-abc"

    response = client.post(
        TRIGGER_URL,
        json={"job_name": "ingestion_job"},
        headers=authed_headers,
    )

    assert response.status_code == HTTPStatus.CREATED


def test_trigger_run_returns_run_id(client, authed_headers, mock_dagster_client):
    """Tests that the trigger response contains the Dagster run ID."""
    mock_dagster_client.submit_job_execution.return_value = "run-abc"

    response = client.post(
        TRIGGER_URL,
        json={"job_name": "ingestion_job"},
        headers=authed_headers,
    )

    assert response.json()["run_id"] == "run-abc"


def test_trigger_run_passes_job_name_to_dagster(
    client, authed_headers, mock_dagster_client
):
    """Tests that the job name is forwarded to the Dagster client."""
    mock_dagster_client.submit_job_execution.return_value = "run-abc"

    client.post(
        TRIGGER_URL,
        json={"job_name": "ingestion_job"},
        headers=authed_headers,
    )

    mock_dagster_client.submit_job_execution.assert_called_once_with(
        "ingestion_job",
        run_config={},
        tags={},
    )


def test_trigger_run_returns_401_without_api_key(client):
    """Tests that the trigger endpoint returns 401 without an API key."""
    response = client.post(
        TRIGGER_URL,
        json={"job_name": "ingestion_job"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED


def test_trigger_run_returns_401_for_invalid_api_key(client, mock_dagster_client):
    """Tests that the trigger endpoint returns 401 for a wrong API key."""
    mock_dagster_client.submit_job_execution.return_value = "run-abc"

    response = client.post(
        TRIGGER_URL,
        json={"job_name": "ingestion_job"},
        headers={"X-Api-Key": "wrong-key"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED


def test_get_run_returns_200(client, authed_headers, mock_dagster_client):
    """Tests that a valid run ID returns HTTP 200."""
    mock_dagster_client.get_run_status.return_value = DagsterRunStatus.SUCCESS

    response = client.get("/v1/pipelines/runs/run-abc", headers=authed_headers)

    assert response.status_code == HTTPStatus.OK


def test_get_run_returns_status(client, authed_headers, mock_dagster_client):
    """Tests that the run status is mapped and returned correctly."""
    mock_dagster_client.get_run_status.return_value = DagsterRunStatus.SUCCESS

    response = client.get("/v1/pipelines/runs/run-abc", headers=authed_headers)

    assert response.json()["status"] == "SUCCESS"
