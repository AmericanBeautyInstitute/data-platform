"""Shared FastAPI dependencies."""

from typing import Annotated

from dagster import DagsterGraphQLClient
from fastapi import Depends, Header, HTTPException

from api.config import Settings, get_settings


def get_dagster_client(
    settings: Annotated[Settings, Depends(get_settings)],
) -> DagsterGraphQLClient:
    """Returns a Dagster GraphQL client."""
    return DagsterGraphQLClient(
        settings.dagster_host,
        port_number=settings.dagster_port,
    )


def verify_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """Verifies the X-Api-Key header against the configured API key."""
    if x_api_key is None or x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
