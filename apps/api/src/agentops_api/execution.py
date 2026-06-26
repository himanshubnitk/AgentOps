from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from agentops_domain.enums import (
    FailureCode,
    RunEventActor,
    RunStatus,
    RunStepStatus,
    RunStepType,
    ToolSideEffectLevel,
    VersionStatus,
)
from agentops_persistence.models import (
    AgentVersion,
    OutboxEvent,
    Run,
    RunEvent,
    RunStep,
    Tool,
    ToolVersion,
    new_id,
    now_utc,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from agentops_api.config import Settings
from agentops_api.errors import ApiError
from agentops_api.providers import ToolCall, ToolSpec, provider_for
from agentops_api.tools import ToolBinding, ToolExecutor, tool_output_preview

TERMINAL_RUN_STATUSES = {
    RunStatus.SUCCEEDED.value,
    RunStatus.FAILED.value,
    RunStatus.CANCELLED.value,
}


@dataclass(frozen=True)
class RunFailure:
    code: str
    message: str
    details: dict[str, Any]


async def append_run_event(
    session: AsyncSession,
    *,
    run: Run,
    event_type: str,
    actor_type: RunEventActor,
    payload: dict[str, Any],
    step: RunStep | None = None,
    actor_id: str | None = None,
) -> RunEvent:
    latest = await session.scalar(
        select(func.coalesce(func.max(RunEvent.sequence_number), 0)).where(
            RunEvent.run_id == run.id
        )
    )
    sequence_number = int(latest or 0) + 1
    event_id = new_id()
    event = RunEvent(
        id=event_id,
        run_id=run.id,
        step_id=step.id if step is not None else None,
        sequence_number=sequence_number,
        event_type=event_type,
        actor_type=actor_type.value,
        actor_id=actor_id,
        payload=payload,
        trace_id=run.id,
        span_id=step.id if step is not None else None,
    )
    session.add(event)
    session.add(
        OutboxEvent(
            id=new_id(),
            aggregate_type="run",
            aggregate_id=run.id,
            event_type=event_type,
            payload={
                "event_id": event_id,
                "run_id": run.id,
                "sequence_number": sequence_number,
                "event_type": event_type,
                "payload": payload,
            },
        )
    )
    await session.flush()
    return event


async def execute_run(session: AsyncSession, run_id: str, settings: Settings) -> Run:
    run = await session.get(Run, run_id)
    if run is None:
        raise ApiError("RUN_NOT_FOUND", "Run not found.", 404)
    if run.status in TERMINAL_RUN_STATUSES:
        return run
    if run.agent_version_id is None:
        return await fail_run(
            session,
            run,
            RunFailure(
                code=FailureCode.INTERNAL_ERROR.value,
                message="Run is missing an agent version.",
                details={},
            ),
        )

    agent_version = await session.get(AgentVersion, run.agent_version_id)
    if agent_version is None or agent_version.status != VersionStatus.PUBLISHED.value:
        return await fail_run(
            session,
            run,
            RunFailure(
                code=FailureCode.INTERNAL_ERROR.value,
                message="Run references a missing or unpublished agent version.",
                details={"agent_version_id": run.agent_version_id},
            ),
        )

    if run.status == RunStatus.QUEUED.value:
        run.status = RunStatus.RUNNING.value
        run.started_at = run.started_at or now_utc()
        await append_run_event(
            session,
            run=run,
            event_type="run.started",
            actor_type=RunEventActor.WORKFLOW,
            payload={"agent_version_id": agent_version.id},
        )
        await session.commit()

    try:
        output = await run_agent_loop(session, run, agent_version, settings)
    except ApiError as exc:
        await session.rollback()
        run = await session.get(Run, run_id)
        if run is None:
            raise
        return await fail_run(session, run, failure_from_api_error(exc))

    run.output = output
    run.status = RunStatus.SUCCEEDED.value
    run.completed_at = now_utc()
    await append_run_event(
        session,
        run=run,
        event_type="run.succeeded",
        actor_type=RunEventActor.WORKFLOW,
        payload={"output": output},
    )
    await session.commit()
    await session.refresh(run)
    return run


async def run_agent_loop(
    session: AsyncSession,
    run: Run,
    agent_version: AgentVersion,
    settings: Settings,
) -> dict[str, Any]:
    await recover_incomplete_steps(session, run)
    tool_bindings = await load_tool_bindings(session, agent_version.tool_version_ids)
    tool_specs = [tool_spec_from_binding(binding) for binding in tool_bindings]
    provider = provider_for(agent_version, settings)
    executor = ToolExecutor()
    tool_results = await load_completed_tool_results(session, run.id)
    final_output = await load_latest_final_output(session, run.id)
    if final_output is not None:
        return final_output

    step_index_start = await next_model_step_index(session, run.id)
    for step_index in range(step_index_start, agent_version.max_steps + 1):
        await session.refresh(run)
        if run.status == RunStatus.CANCELLED.value:
            raise ApiError(FailureCode.CANCELLED.value, "Run was cancelled.", 409)

        model_step = await start_step(
            session,
            run=run,
            node_key=f"model:{step_index}",
            step_type=RunStepType.MODEL,
            name="Model invocation",
            payload={"input": run.input, "tool_results": tool_results},
        )
        await session.commit()
        result = await provider.invoke(
            agent_version=agent_version,
            run_input=run.input,
            tools=tool_specs,
            tool_results=tool_results,
            step_index=step_index,
        )
        model_step.status = RunStepStatus.SUCCEEDED.value
        model_step.output = {
            "message": result.message,
            "final_output": result.final_output,
            "tool_calls": [call.as_dict() for call in result.tool_calls],
            "usage": result.usage.as_dict(),
        }
        model_step.completed_at = now_utc()
        await append_run_event(
            session,
            run=run,
            step=model_step,
            event_type="model.completed",
            actor_type=RunEventActor.AGENT,
            payload=model_step.output,
        )
        await session.commit()

        if result.final_output is not None:
            return result.final_output
        if not result.tool_calls:
            return {"answer": result.message, "tool_results": tool_results}

        tool_results.extend(
            await execute_tool_calls(
                session,
                run=run,
                tool_bindings=tool_bindings,
                executor=executor,
                tool_calls=result.tool_calls,
            )
        )

    raise ApiError(
        FailureCode.MAX_STEPS_EXCEEDED.value,
        "Agent exceeded its maximum step count.",
        409,
        {"max_steps": agent_version.max_steps},
    )


async def execute_tool_calls(
    session: AsyncSession,
    *,
    run: Run,
    tool_bindings: list[ToolBinding],
    executor: ToolExecutor,
    tool_calls: list[ToolCall],
) -> list[dict[str, Any]]:
    bindings_by_id = {binding.version_id: binding for binding in tool_bindings}
    started: list[tuple[ToolCall, ToolBinding, RunStep]] = []
    for index, call in enumerate(tool_calls, start=1):
        binding = bindings_by_id.get(call.tool_version_id)
        if binding is None:
            raise ApiError(
                "TOOL_NOT_ATTACHED",
                "Model requested a tool that is not attached to the agent version.",
                422,
                {"tool_version_id": call.tool_version_id},
            )
        step = await start_step(
            session,
            run=run,
            node_key=f"tool:{call.call_id}:{index}",
            step_type=RunStepType.TOOL,
            name=binding.name,
            payload={"arguments": call.arguments, "tool_version_id": binding.version_id},
        )
        started.append((call, binding, step))
    await session.commit()

    if all(
        binding.side_effect_level == ToolSideEffectLevel.READ_ONLY.value
        for _, binding, _ in started
    ):
        raw_results: list[dict[str, Any] | BaseException] = list(
            await asyncio.gather(
                *(executor.execute(binding, call.arguments) for call, binding, _ in started),
                return_exceptions=True,
            )
        )
    else:
        raw_results = []
        for call, binding, _ in started:
            try:
                raw_results.append(await executor.execute(binding, call.arguments))
            except Exception as exc:  # noqa: BLE001
                raw_results.append(exc)

    tool_results: list[dict[str, Any]] = []
    for (call, binding, step), raw_result in zip(started, raw_results, strict=True):
        if isinstance(raw_result, BaseException):
            failure = failure_from_exception(raw_result)
            step.status = RunStepStatus.FAILED.value
            step.error = {
                "code": failure.code,
                "message": failure.message,
                "details": failure.details,
            }
            step.completed_at = now_utc()
            await append_run_event(
                session,
                run=run,
                step=step,
                event_type="tool.failed",
                actor_type=RunEventActor.TOOL,
                payload=step.error,
            )
            await session.commit()
            raise ApiError(failure.code, failure.message, 502, failure.details)

        step.status = RunStepStatus.SUCCEEDED.value
        step.output = {"output": tool_output_preview(raw_result)}
        step.completed_at = now_utc()
        await append_run_event(
            session,
            run=run,
            step=step,
            event_type="tool.completed",
            actor_type=RunEventActor.TOOL,
            payload={
                "tool_version_id": binding.version_id,
                "output": tool_output_preview(raw_result),
            },
        )
        await session.commit()
        tool_results.append(
            {
                "tool_version_id": binding.version_id,
                "name": binding.name,
                "arguments": call.arguments,
                "output": raw_result,
            }
        )
    return tool_results


async def start_step(
    session: AsyncSession,
    *,
    run: Run,
    node_key: str,
    step_type: RunStepType,
    name: str,
    payload: dict[str, Any],
) -> RunStep:
    step = RunStep(
        run_id=run.id,
        node_key=node_key,
        step_type=step_type.value,
        name=name,
        status=RunStepStatus.RUNNING.value,
        attempt=1,
        input=payload,
        started_at=now_utc(),
    )
    session.add(step)
    await session.flush()
    await append_run_event(
        session,
        run=run,
        step=step,
        event_type="step.started",
        actor_type=RunEventActor.WORKFLOW,
        payload={"node_key": node_key, "step_type": step_type.value, "name": name},
    )
    return step


async def recover_incomplete_steps(session: AsyncSession, run: Run) -> None:
    result = await session.scalars(
        select(RunStep)
        .where(RunStep.run_id == run.id, RunStep.status == RunStepStatus.RUNNING.value)
        .order_by(RunStep.created_at.asc())
    )
    incomplete_steps = list(result.all())
    for step in incomplete_steps:
        step.status = RunStepStatus.FAILED.value
        step.error = {
            "code": "WORKER_RECOVERY",
            "message": "Step was incomplete when the worker resumed the run.",
            "details": {},
        }
        step.completed_at = now_utc()
        await append_run_event(
            session,
            run=run,
            step=step,
            event_type="step.recovered",
            actor_type=RunEventActor.WORKFLOW,
            payload=step.error,
        )
    if incomplete_steps:
        await session.commit()


async def load_completed_tool_results(session: AsyncSession, run_id: str) -> list[dict[str, Any]]:
    result = await session.scalars(
        select(RunStep)
        .where(
            RunStep.run_id == run_id,
            RunStep.step_type == RunStepType.TOOL.value,
            RunStep.status == RunStepStatus.SUCCEEDED.value,
        )
        .order_by(RunStep.created_at.asc())
    )
    tool_results: list[dict[str, Any]] = []
    for step in result.all():
        step_input = step.input
        step_output = step.output or {}
        arguments = step_input.get("arguments") if isinstance(step_input, dict) else {}
        output = step_output.get("output") if isinstance(step_output, dict) else {}
        tool_version_id = (
            step_input.get("tool_version_id") if isinstance(step_input, dict) else None
        )
        tool_results.append(
            {
                "tool_version_id": str(tool_version_id or ""),
                "name": step.name,
                "arguments": arguments if isinstance(arguments, dict) else {},
                "output": output if isinstance(output, dict) else {"value": output},
            }
        )
    return tool_results


async def load_latest_final_output(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    result = await session.scalars(
        select(RunStep)
        .where(
            RunStep.run_id == run_id,
            RunStep.step_type == RunStepType.MODEL.value,
            RunStep.status == RunStepStatus.SUCCEEDED.value,
        )
        .order_by(RunStep.created_at.desc())
    )
    for step in result.all():
        output = step.output or {}
        final_output = output.get("final_output") if isinstance(output, dict) else None
        if isinstance(final_output, dict):
            return final_output
    return None


async def next_model_step_index(session: AsyncSession, run_id: str) -> int:
    count = await session.scalar(
        select(func.count())
        .select_from(RunStep)
        .where(
            RunStep.run_id == run_id,
            RunStep.step_type == RunStepType.MODEL.value,
        )
    )
    return int(count or 0) + 1


async def fail_run(session: AsyncSession, run: Run, failure: RunFailure) -> Run:
    run.status = RunStatus.FAILED.value
    run.error_code = failure.code
    run.error_message = failure.message
    run.completed_at = now_utc()
    await append_run_event(
        session,
        run=run,
        event_type="run.failed",
        actor_type=RunEventActor.WORKFLOW,
        payload={
            "code": failure.code,
            "message": failure.message,
            "details": failure.details,
        },
    )
    await session.commit()
    await session.refresh(run)
    return run


async def cancel_run(session: AsyncSession, run: Run, actor_id: str) -> Run:
    if run.status in TERMINAL_RUN_STATUSES:
        return run
    run.status = RunStatus.CANCELLED.value
    run.error_code = FailureCode.CANCELLED.value
    run.error_message = "Run was cancelled by the user."
    run.completed_at = now_utc()
    await append_run_event(
        session,
        run=run,
        event_type="run.cancelled",
        actor_type=RunEventActor.USER,
        actor_id=actor_id,
        payload={"reason": "user_requested"},
    )
    await session.commit()
    await session.refresh(run)
    return run


async def load_tool_bindings(
    session: AsyncSession, tool_version_ids: list[str]
) -> list[ToolBinding]:
    if not tool_version_ids:
        return []
    rows = await session.execute(
        select(ToolVersion, Tool)
        .join(Tool, Tool.id == ToolVersion.tool_id)
        .where(
            ToolVersion.id.in_(tool_version_ids),
            ToolVersion.status == VersionStatus.PUBLISHED.value,
        )
    )
    bindings_by_id = {
        version.id: ToolBinding.from_version(
            version=version,
            name=tool.name,
            slug=tool.slug,
            description=tool.description,
        )
        for version, tool in rows.all()
    }
    return [
        bindings_by_id[version_id]
        for version_id in tool_version_ids
        if version_id in bindings_by_id
    ]


def tool_spec_from_binding(binding: ToolBinding) -> ToolSpec:
    return ToolSpec(
        version_id=binding.version_id,
        name=binding.name,
        slug=binding.slug,
        description=binding.description,
        input_schema=binding.input_schema,
        side_effect_level=binding.side_effect_level,
    )


def failure_from_exception(exc: BaseException) -> RunFailure:
    if isinstance(exc, ApiError):
        return failure_from_api_error(exc)
    return RunFailure(
        code=FailureCode.TOOL_EXECUTION_ERROR.value,
        message=str(exc) or "Tool execution failed.",
        details={},
    )


def failure_from_api_error(exc: ApiError) -> RunFailure:
    if exc.code == FailureCode.CANCELLED.value:
        code = FailureCode.CANCELLED.value
    elif exc.code in {FailureCode.MAX_STEPS_EXCEEDED.value, "MAX_STEPS_EXCEEDED"}:
        code = FailureCode.MAX_STEPS_EXCEEDED.value
    elif exc.code == "TOOL_VALIDATION_ERROR":
        code = FailureCode.TOOL_VALIDATION_ERROR.value
    elif exc.code.startswith("PROVIDER") or exc.code == "UNSUPPORTED_PROVIDER":
        code = FailureCode.PROVIDER_ERROR.value
    elif "TOOL" in exc.code:
        code = FailureCode.TOOL_EXECUTION_ERROR.value
    else:
        code = FailureCode.INTERNAL_ERROR.value
    return RunFailure(code=code, message=exc.message, details=exc.details)
