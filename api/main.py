"""FastAPI application factory."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import Settings, get_settings
from api.routers import extracts, health, pipelines


def create_app(settings: Settings | None = None) -> FastAPI:
    """Creates and configures the FastAPI application."""
    resolved = settings or get_settings()
    app = FastAPI(title="Data Platform API", lifespan=_lifespan)
    app.dependency_overrides[get_settings] = lambda: resolved
    app.add_middleware(
        CORSMiddleware,
        allow_origins=resolved.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health.router, prefix="/v1")
    app.include_router(pipelines.router, prefix="/v1")
    app.include_router(extracts.router, prefix="/v1")
    return app


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Manages application startup and shutdown."""
    app.state.run_sources = {}
    yield


app = create_app()
