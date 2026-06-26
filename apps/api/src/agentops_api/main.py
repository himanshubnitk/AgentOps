from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from agentops_api.config import get_settings
from agentops_api.errors import register_exception_handlers
from agentops_api.middleware import register_middleware
from agentops_api.routers import api, health


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.started = True
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="AgentOps API",
        version="0.1.0",
        lifespan=lifespan,
        openapi_url=f"{settings.api_v1_prefix}/openapi.json",
        docs_url=f"{settings.api_v1_prefix}/docs",
    )
    register_middleware(app)
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(api.router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
