from __future__ import annotations

from collections.abc import AsyncIterator

from agentops_persistence.models import User
from agentops_persistence.session import make_session_factory
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agentops_api.config import get_settings
from agentops_api.errors import ApiError
from agentops_api.security import decode_token

_settings = get_settings()
SessionLocal: async_sessionmaker[AsyncSession] = make_session_factory(_settings.database_url)
bearer = HTTPBearer(auto_error=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    session: AsyncSession = Depends(get_session),
) -> User:
    if credentials is None:
        raise ApiError("UNAUTHENTICATED", "Authentication required.", 401)

    data = decode_token(credentials.credentials, settings=get_settings(), token_type="access")
    user = await session.scalar(
        select(User).where(User.id == data["sub"], User.is_active.is_(True))
    )
    if user is None:
        raise ApiError("UNAUTHENTICATED", "Authentication required.", 401)
    return user
