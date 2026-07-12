from __future__ import annotations

import asyncio
import os
import queue
import threading
import time
from collections.abc import Callable
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx

PayloadRedactor = Callable[[dict[str, Any]], dict[str, Any]]
_trace_context: ContextVar[_Trace | _AsyncTrace | None] = ContextVar("agentops_trace", default=None)
_span_context: ContextVar[str | None] = ContextVar("agentops_span", default=None)


def _event(
    trace: _Trace | _AsyncTrace,
    event_type: str,
    payload: dict[str, Any],
    span_id: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "run_id": trace.trace_id,
        "sequence_number": trace.next_sequence(),
        "event_type": event_type,
        "timestamp": datetime.now(UTC).isoformat(),
        "payload": payload,
        "trace_id": trace.trace_id,
        "span_id": span_id,
    }


class _Scope:
    def __init__(self, trace: Any, name: str, kind: str, parent_span_id: str | None) -> None:
        self.trace = trace
        self.name = name
        self.kind = kind
        self.span_id = str(uuid4())
        self.parent_span_id = parent_span_id
        self.output: Any = None
        self.usage: dict[str, int] | None = None
        self._span_token: Token[str | None] | None = None

    def set_output(self, output: Any) -> None:
        self.output = output

    def set_usage(self, **usage: int) -> None:
        self.usage = usage

    def _start_payload(self) -> dict[str, Any]:
        return {"name": self.name, "parent_span_id": self.parent_span_id}

    def _end_payload(self, error: BaseException | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name, "output": self.output}
        if self.usage is not None:
            payload["usage"] = self.usage
        if error is not None:
            payload["error"] = {"type": type(error).__name__, "message": str(error)}
        return payload


class Span(_Scope):
    def __enter__(self) -> Span:
        self.trace.client._record(
            _event(self.trace, f"{self.kind}.started", self._start_payload(), self.span_id)
        )
        self._span_token = _span_context.set(self.span_id)
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        self.trace.client._record(
            _event(
                self.trace,
                f"{self.kind}.failed" if exc else f"{self.kind}.completed",
                self._end_payload(exc),
                self.span_id,
            )
        )
        if self._span_token is not None:
            _span_context.reset(self._span_token)


class _Trace:
    def __init__(self, client: AgentOps, name: str) -> None:
        self.client = client
        self.name = name
        self.trace_id = str(uuid4())
        self._sequence = 0
        self._trace_token: Token[_Trace | _AsyncTrace | None] | None = None

    def next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def __enter__(self) -> _Trace:
        self._trace_token = _trace_context.set(self)
        self.client._record(_event(self, "trace.started", {"name": self.name}))
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        self.client._record(
            _event(
                self,
                "trace.failed" if exc else "trace.completed",
                {"name": self.name, "error": str(exc) if exc else None},
            )
        )
        if self._trace_token is not None:
            _trace_context.reset(self._trace_token)

    def span(self, name: str) -> Span:
        return Span(self, name, "span", _span_context.get())

    def model(self, provider: str, model: str) -> Span:
        span = Span(self, model, "model", _span_context.get())
        span._start_payload = lambda: {  # type: ignore[method-assign]
            "provider": provider,
            "model": model,
            "parent_span_id": span.parent_span_id,
        }
        return span


class AgentOps:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        batch_size: int = 50,
        queue_size: int = 1000,
        redact: PayloadRedactor | None = None,
    ) -> None:
        self.api_key = str(api_key or os.getenv("AGENTOPS_API_KEY") or "")
        self.endpoint = str(
            endpoint or os.getenv("AGENTOPS_ENDPOINT") or "http://127.0.0.1:8000"
        ).rstrip("/")
        self.batch_size = max(1, batch_size)
        self.redact = redact or (lambda payload: payload)
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=max(1, queue_size))
        self._closed = threading.Event()
        self._worker = threading.Thread(target=self._run, name="agentops-sdk", daemon=True)
        self._worker.start()

    def trace(self, name: str) -> _Trace:
        return _Trace(self, name)

    def _record(self, event: dict[str, Any]) -> None:
        if self._closed.is_set() or not self.api_key:
            return
        event["payload"] = self.redact(dict(event["payload"]))
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            pass

    def _run(self) -> None:
        while True:
            try:
                first = self._queue.get(timeout=0.1)
            except queue.Empty:
                if self._closed.is_set():
                    return
                continue
            if first is None:
                self._queue.task_done()
                return
            batch = [first]
            while len(batch) < self.batch_size:
                try:
                    queued = self._queue.get_nowait()
                except queue.Empty:
                    break
                if queued is None:
                    self._queue.task_done()
                    self._closed.set()
                    break
                batch.append(queued)
            self._send(batch)
            for _ in batch:
                self._queue.task_done()

    def _send(self, events: list[dict[str, Any]]) -> None:
        for attempt in range(3):
            try:
                response = httpx.post(
                    f"{self.endpoint}/api/v1/ingestion/events:batch",
                    headers={"X-AgentOps-Api-Key": self.api_key},
                    json={"events": events},
                    timeout=5.0,
                )
                if response.is_success:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1 * (2**attempt))

    def flush(self) -> None:
        self._queue.join()

    def shutdown(self) -> None:
        if self._closed.is_set():
            return
        self.flush()
        self._closed.set()
        self._queue.put(None)
        self._worker.join(timeout=1)


class AsyncSpan(_Scope):
    def __init__(
        self, trace: _AsyncTrace, name: str, kind: str, parent_span_id: str | None
    ) -> None:
        super().__init__(trace, name, kind, parent_span_id)

    async def __aenter__(self) -> AsyncSpan:
        await self.trace.client._record(
            _event(self.trace, f"{self.kind}.started", self._start_payload(), self.span_id)
        )
        self._span_token = _span_context.set(self.span_id)
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, traceback: object
    ) -> None:
        await self.trace.client._record(
            _event(
                self.trace,
                f"{self.kind}.failed" if exc else f"{self.kind}.completed",
                self._end_payload(exc),
                self.span_id,
            )
        )
        if self._span_token is not None:
            _span_context.reset(self._span_token)


class _AsyncTrace:
    def __init__(self, client: AsyncAgentOps, name: str) -> None:
        self.client = client
        self.name = name
        self.trace_id = str(uuid4())
        self._sequence = 0
        self._trace_token: Token[_Trace | _AsyncTrace | None] | None = None

    def next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    async def __aenter__(self) -> _AsyncTrace:
        self._trace_token = _trace_context.set(self)
        await self.client._record(_event(self, "trace.started", {"name": self.name}))
        return self

    async def __aexit__(
        self, exc_type: object, exc: BaseException | None, traceback: object
    ) -> None:
        await self.client._record(
            _event(
                self,
                "trace.failed" if exc else "trace.completed",
                {"name": self.name, "error": str(exc) if exc else None},
            )
        )
        if self._trace_token is not None:
            _trace_context.reset(self._trace_token)

    def span(self, name: str) -> AsyncSpan:
        return AsyncSpan(self, name, "span", _span_context.get())

    def model(self, provider: str, model: str) -> AsyncSpan:
        span = AsyncSpan(self, model, "model", _span_context.get())
        span._start_payload = lambda: {  # type: ignore[method-assign]
            "provider": provider,
            "model": model,
            "parent_span_id": span.parent_span_id,
        }
        return span


class AsyncAgentOps:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        batch_size: int = 50,
        queue_size: int = 1000,
        redact: PayloadRedactor | None = None,
    ) -> None:
        self.api_key = str(api_key or os.getenv("AGENTOPS_API_KEY") or "")
        self.endpoint = str(
            endpoint or os.getenv("AGENTOPS_ENDPOINT") or "http://127.0.0.1:8000"
        ).rstrip("/")
        self.batch_size = max(1, batch_size)
        self.redact = redact or (lambda payload: payload)
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=max(1, queue_size))
        self._worker: asyncio.Task[None] | None = None
        self._closed = False

    def trace(self, name: str) -> _AsyncTrace:
        return _AsyncTrace(self, name)

    async def _record(self, event: dict[str, Any]) -> None:
        if self._closed or not self.api_key:
            return
        if self._worker is None:
            self._worker = asyncio.create_task(self._run())
        event["payload"] = self.redact(dict(event["payload"]))
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    async def _run(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            while not self._closed or not self._queue.empty():
                event = await self._queue.get()
                batch = [event]
                while len(batch) < self.batch_size and not self._queue.empty():
                    batch.append(self._queue.get_nowait())
                for attempt in range(3):
                    try:
                        response = await client.post(
                            f"{self.endpoint}/api/v1/ingestion/events:batch",
                            headers={"X-AgentOps-Api-Key": self.api_key},
                            json={"events": batch},
                        )
                        if response.is_success:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1 * (2**attempt))
                for _ in batch:
                    self._queue.task_done()

    async def flush(self) -> None:
        await self._queue.join()

    async def shutdown(self) -> None:
        await self.flush()
        self._closed = True
        if self._worker is not None:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
