"""Health check router."""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/health", tags=["health"])


class HealthResponse(BaseModel):
    """Health check response."""

    status: str


@router.get("")
def get_health() -> HealthResponse:
    """Returns API health status."""
    return HealthResponse(status="ok")
