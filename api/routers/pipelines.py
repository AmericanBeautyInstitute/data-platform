"""Pipeline management router."""

from typing import Annotated

from dagster import DagsterGraphQLClient, DagsterGraphQLClientError
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_dagster_client, verify_api_key
from api.schemas.pipeline import RunResponse, RunStatus, TriggerRunRequest

router = APIRouter(
    prefix="/pipelines",
    tags=["pipelines"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("/runs", status_code=201)
def trigger_run(
    body: TriggerRunRequest,
    client: Annotated[DagsterGraphQLClient, Depends(get_dagster_client)],
) -> RunResponse:
    """Triggers a Dagster pipeline run and returns the run ID."""
    run_id = _submit_run(client, body)
    return RunResponse(run_id=run_id, job_name=body.job_name, status=RunStatus.STARTED)


def _submit_run(client: DagsterGraphQLClient, body: TriggerRunRequest) -> str:
    """Submits a job execution to Dagster and returns the run ID."""
    try:
        run_id = client.submit_job_execution(
            body.job_name,
            run_config=body.run_config,
            tags=body.tags,
        )
        return run_id
    except DagsterGraphQLClientError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/runs/{run_id}")
def get_run(
    run_id: str,
    client: Annotated[DagsterGraphQLClient, Depends(get_dagster_client)],
) -> RunResponse:
    """Returns the current status of a pipeline run."""
    status = _get_run_status(client, run_id)
    return RunResponse(run_id=run_id, job_name=None, status=status)


def _get_run_status(client: DagsterGraphQLClient, run_id: str) -> RunStatus:
    """Fetches and maps the Dagster run status for a given run ID."""
    try:
        dagster_status = client.get_run_status(run_id)
        return RunStatus(dagster_status.value)
    except DagsterGraphQLClientError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
