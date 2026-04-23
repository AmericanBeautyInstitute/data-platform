"""Pipeline request and response schemas."""

from enum import Enum

from pydantic import BaseModel, Field


class RunStatus(str, Enum):
    """Possible states for a Dagster pipeline run."""

    CANCELED = "CANCELED"
    CANCELING = "CANCELING"
    FAILURE = "FAILURE"
    MANAGED = "MANAGED"
    NOT_STARTED = "NOT_STARTED"
    QUEUED = "QUEUED"
    STARTED = "STARTED"
    STARTING = "STARTING"
    SUCCESS = "SUCCESS"


class TriggerRunRequest(BaseModel):
    """Request body for triggering a pipeline run."""

    job_name: str
    run_config: dict = Field(default_factory=dict)
    tags: dict[str, str] = Field(default_factory=dict)


class RunResponse(BaseModel):
    """Response for a pipeline run operation."""

    run_id: str
    job_name: str | None
    status: RunStatus
