from __future__ import annotations

import json
from types import TracebackType
from typing import Any

from redis.asyncio import Redis


class RedisPublisher:
    def __init__(self, redis_url: str) -> None:
        self._redis: Any = Redis.from_url(redis_url, decode_responses=True)

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        await self._redis.publish(channel, json.dumps(payload, default=str))

    async def close(self) -> None:
        await self._redis.aclose()


class RedisRunSubscription:
    def __init__(self, redis_url: str, channel: str) -> None:
        self._redis: Any = Redis.from_url(redis_url, decode_responses=True)
        self._pubsub: Any = self._redis.pubsub()
        self._channel = channel

    async def __aenter__(self) -> RedisRunSubscription:
        await self._pubsub.subscribe(self._channel)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._pubsub.unsubscribe(self._channel)
        await self._pubsub.aclose()
        await self._redis.aclose()

    async def get_message(self, timeout: float) -> dict[str, Any] | None:
        message = await self._pubsub.get_message(
            ignore_subscribe_messages=True,
            timeout=timeout,
        )
        if message is None:
            return None
        data = message.get("data")
        if not isinstance(data, str):
            return None
        loaded = json.loads(data)
        return loaded if isinstance(loaded, dict) else None


def open_run_event_subscription(redis_url: str, channel: str) -> RedisRunSubscription:
    return RedisRunSubscription(redis_url, channel)
