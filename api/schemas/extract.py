"""Extract request and response schemas."""

from enum import StrEnum

from pydantic import BaseModel, Field

from api.schemas.pipeline import RunStatus


class ExtractSource(StrEnum):
    """Available data extract sources."""

    FACEBOOK_ADS = "facebook_ads"
    GOOGLE_ADS = "google_ads"
    GOOGLE_ANALYTICS = "google_analytics"
    GOOGLE_SHEETS = "google_sheets"
    PAYPAL = "paypal"
    STRIPE = "stripe"


class TriggerExtractRequest(BaseModel):
    """Request body for triggering an extract run."""

    source: ExtractSource
    run_config: dict = Field(default_factory=dict)
    tags: dict[str, str] = Field(default_factory=dict)


class ExtractRunResponse(BaseModel):
    """Response for an extract run operation."""

    run_id: str
    source: ExtractSource | None
    status: RunStatus
