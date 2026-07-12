from __future__ import annotations

import asyncio

from agentops_domain.enums import RunStatus
from agentops_persistence.models import ApprovalRequest, Run, now_utc
from agentops_persistence.session import make_session_factory
from sqlalchemy import and_, or_, select

from agentops_api.config import get_settings
from agentops_api.execution import execute_run


async def run_once(limit: int = 10) -> int:
    settings = get_settings()
    session_factory = make_session_factory(settings.database_url)
    async with session_factory() as session:
        result = await session.scalars(
            select(Run.id)
            .outerjoin(ApprovalRequest, ApprovalRequest.run_id == Run.id)
            .where(
                or_(
                    Run.status.in_([RunStatus.QUEUED.value, RunStatus.RUNNING.value]),
                    and_(
                        Run.status == RunStatus.WAITING_FOR_APPROVAL.value,
                        ApprovalRequest.status == "pending",
                        ApprovalRequest.expires_at.is_not(None),
                        ApprovalRequest.expires_at <= now_utc(),
                    ),
                )
            )
            .order_by(Run.created_at.asc())
            .limit(limit)
        )
        run_ids = list(result.all())

    for run_id in run_ids:
        async with session_factory() as session:
            await execute_run(session, run_id, settings)
    return len(run_ids)


async def worker_loop(poll_interval_seconds: float = 2.0) -> None:
    while True:
        processed = await run_once()
        if processed == 0:
            await asyncio.sleep(poll_interval_seconds)


def main() -> None:
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()
