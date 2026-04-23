"""Extract management router."""

from typing import Annotated

from dagster import DagsterGraphQLClient, DagsterGraphQLClientError
from fastapi import APIRouter, Depends, HTTPException, Request

from api.dependencies import get_dagster_client, verify_api_key
from api.schemas.extract import ExtractRunResponse, TriggerExtractRequest
from api.schemas.pipeline import RunStatus

_INGESTION_JOB = "ingestion_job"

router = APIRouter(
    prefix="/extracts",
    tags=["extracts"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("/runs", status_code=201)
def trigger_extract(
    body: TriggerExtractRequest,
    request: Request,
    client: Annotated[DagsterGraphQLClient, Depends(get_dagster_client)],
) -> ExtractRunResponse:
    """Triggers an ingestion run for a specific extract source."""
    tags = {**body.tags, "source": body.source.value}
    run_id = _submit_extract(client, body, tags)
    request.app.state.run_sources[run_id] = body.source
    return ExtractRunResponse(
        run_id=run_id, source=body.source, status=RunStatus.STARTED
    )


def _submit_extract(
    client: DagsterGraphQLClient,
    body: TriggerExtractRequest,
    tags: dict[str, str],
) -> str:
    """Submits an ingestion job run tagged with the extract source."""
    try:
        run_id = client.submit_job_execution(
            _INGESTION_JOB,
            run_config=body.run_config,
            tags=tags,
        )
        return run_id
    except DagsterGraphQLClientError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/runs/{run_id}")
def get_extract_run(
    run_id: str,
    request: Request,
    client: Annotated[DagsterGraphQLClient, Depends(get_dagster_client)],
) -> ExtractRunResponse:
    """Returns the current status of an extract run."""
    source = request.app.state.run_sources.get(run_id)
    status = _get_extract_status(client, run_id)
    return ExtractRunResponse(run_id=run_id, source=source, status=status)


def _get_extract_status(client: DagsterGraphQLClient, run_id: str) -> RunStatus:
    """Fetches and maps the Dagster run status for an extract run."""
    try:
        dagster_status = client.get_run_status(run_id)
        return RunStatus(dagster_status.value)
    except DagsterGraphQLClientError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
