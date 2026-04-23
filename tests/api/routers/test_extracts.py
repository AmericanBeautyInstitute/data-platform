"""Tests for the extracts router."""

from http import HTTPStatus

from dagster import DagsterRunStatus

TRIGGER_URL = "/v1/extracts/runs"


def test_trigger_extract_returns_201(client, authed_headers, mock_dagster_client):
    """Tests that a successful extract trigger returns HTTP 201."""
    mock_dagster_client.submit_job_execution.return_value = "run-xyz"

    response = client.post(
        TRIGGER_URL,
        json={"source": "google_analytics"},
        headers=authed_headers,
    )

    assert response.status_code == HTTPStatus.CREATED


def test_trigger_extract_returns_run_id(client, authed_headers, mock_dagster_client):
    """Tests that the trigger response contains the Dagster run ID."""
    mock_dagster_client.submit_job_execution.return_value = "run-xyz"

    response = client.post(
        TRIGGER_URL,
        json={"source": "google_analytics"},
        headers=authed_headers,
    )

    assert response.json()["run_id"] == "run-xyz"


def test_trigger_extract_returns_source(client, authed_headers, mock_dagster_client):
    """Tests that the trigger response echoes back the requested source."""
    mock_dagster_client.submit_job_execution.return_value = "run-xyz"

    response = client.post(
        TRIGGER_URL,
        json={"source": "google_analytics"},
        headers=authed_headers,
    )

    assert response.json()["source"] == "google_analytics"


def test_trigger_extract_tags_source(client, authed_headers, mock_dagster_client):
    """Tests that the source is passed as a run tag to Dagster."""
    mock_dagster_client.submit_job_execution.return_value = "run-xyz"

    client.post(
        TRIGGER_URL,
        json={"source": "google_analytics"},
        headers=authed_headers,
    )

    _, kwargs = mock_dagster_client.submit_job_execution.call_args
    assert kwargs["tags"]["source"] == "google_analytics"


def test_trigger_extract_rejects_invalid_source(client, authed_headers):
    """Tests that an unrecognized source returns HTTP 422."""
    response = client.post(
        TRIGGER_URL,
        json={"source": "invalid_source"},
        headers=authed_headers,
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_get_extract_run_returns_200(client, authed_headers, mock_dagster_client):
    """Tests that a valid run ID returns HTTP 200."""
    mock_dagster_client.get_run_status.return_value = DagsterRunStatus.STARTED

    response = client.get("/v1/extracts/runs/run-xyz", headers=authed_headers)

    assert response.status_code == HTTPStatus.OK


def test_get_extract_run_returns_status(client, authed_headers, mock_dagster_client):
    """Tests that the run status is returned correctly."""
    mock_dagster_client.get_run_status.return_value = DagsterRunStatus.STARTED

    response = client.get("/v1/extracts/runs/run-xyz", headers=authed_headers)

    assert response.json()["status"] == "STARTED"


def test_get_extract_run_source_populated_after_trigger(
    client, authed_headers, mock_dagster_client
):
    """Tests that source is populated in GET response after a prior trigger."""
    mock_dagster_client.submit_job_execution.return_value = "run-xyz"
    mock_dagster_client.get_run_status.return_value = DagsterRunStatus.STARTED

    client.post(TRIGGER_URL, json={"source": "stripe"}, headers=authed_headers)
    response = client.get("/v1/extracts/runs/run-xyz", headers=authed_headers)

    assert response.json()["source"] == "stripe"


def test_get_extract_run_source_none_for_unknown_run(
    client, authed_headers, mock_dagster_client
):
    """Tests that source is null for a run ID not triggered through this API."""
    mock_dagster_client.get_run_status.return_value = DagsterRunStatus.SUCCESS

    response = client.get("/v1/extracts/runs/unknown-run", headers=authed_headers)

    assert response.json()["source"] is None
