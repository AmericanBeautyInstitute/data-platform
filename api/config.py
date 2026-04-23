"""API-level application settings."""

import functools

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str
    cors_origins: list[str] = ["*"]
    dagster_host: str = "localhost"
    dagster_port: int = 3000
    environment: str = "development"


@functools.lru_cache
def get_settings() -> Settings:
    """Returns cached application settings."""
    return Settings()
