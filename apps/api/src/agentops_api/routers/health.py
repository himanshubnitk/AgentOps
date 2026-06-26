from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from agentops_api.dependencies import get_session
from agentops_api.errors import ApiError

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/startup")
async def startup() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def ready(session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    try:
        await session.execute(text("select 1"))
    except Exception as exc:
        raise ApiError("DEPENDENCY_UNAVAILABLE", "Database is not ready.", 503) from exc
    return {"status": "ok", "database": "ok", "redis": "skipped"}
