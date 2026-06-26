from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGENTOPS_", env_file=".env", extra="ignore")

    environment: str = "development"
    api_v1_prefix: str = "/api/v1"
    database_url: str = "sqlite+aiosqlite:///./agentops.db"
    redis_url: str = "redis://localhost:6379/0"
    temporal_address: str = "localhost:7233"
    temporal_task_queue: str = "agentops-runs"
    inline_run_execution: bool = True
    provider_timeout_seconds: int = 30
    openai_api_key: str | None = None
    jwt_secret: str = "dev-only-change-me"
    access_token_minutes: int = 30
    refresh_token_days: int = 14


@lru_cache
def get_settings() -> Settings:
    return Settings()
