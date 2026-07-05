from __future__ import annotations

import argparse
import asyncio
from typing import Any, Protocol

from agentops_persistence.models import OutboxEvent, now_utc
from agentops_persistence.session import make_session_factory
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from agentops_api.config import get_settings
from agentops_api.redis_live import RedisPublisher


class EventPublisher(Protocol):
    async def publish(self, channel: str, payload: dict[str, Any]) -> None: ...


def run_channel(run_id: str) -> str:
    return f"runs:{run_id}"


async def publish_outbox_batch(
    session: AsyncSession,
    publisher: EventPublisher,
    *,
    limit: int = 100,
) -> dict[str, int]:
    statement = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at.asc())
        .limit(limit)
    )
    if session.get_bind().dialect.name != "sqlite":
        statement = statement.with_for_update(skip_locked=True)

    events = list((await session.scalars(statement)).all())
    metrics = {"published": 0, "failed": 0}
    for event in events:
        event.attempt_count += 1
        try:
            payload = dict(event.payload)
            payload["outbox_event_id"] = event.id
            await publisher.publish(run_channel(event.aggregate_id), payload)
        except Exception as exc:  # noqa: BLE001
            event.last_error = str(exc)[:2000]
            metrics["failed"] += 1
            continue

        event.published_at = now_utc()
        event.last_error = None
        metrics["published"] += 1

    await session.commit()
    return metrics


async def relay_forever(*, batch_size: int = 100, idle_sleep_seconds: float = 1.0) -> None:
    settings = get_settings()
    session_factory = make_session_factory(settings.database_url)
    publisher = RedisPublisher(settings.redis_url)
    try:
        while True:
            async with session_factory() as session:
                metrics = await publish_outbox_batch(session, publisher, limit=batch_size)
            if metrics["published"] == 0:
                await asyncio.sleep(idle_sleep_seconds)
    finally:
        await publisher.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish AgentOps outbox events to Redis.")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--idle-sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()
    asyncio.run(
        relay_forever(
            batch_size=args.batch_size,
            idle_sleep_seconds=args.idle_sleep_seconds,
        )
    )
