from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    ApprovalRequest,
    OutboxEvent,
    Run,
    RunEvent,
    RunStep,
    Tool,
    ToolVersion,
    WorkflowVersion,
    new_id,
    now_utc,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agentops_api.config import Settings
from agentops_api.errors import ApiError
from agentops_api.observability import duration_ms, redact_payload, usage_record_for
from agentops_api.providers import ToolCall, ToolSpec, provider_for
from agentops_api.tools import ToolBinding, ToolExecutor, tool_output_preview
from agentops_api.workflows import edge_allows, incoming_edges, resolve_value, topological_layers

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
    event_id: str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    timestamp: datetime | None = None,
) -> RunEvent:
    safe_payload = redact_payload(payload)
    latest = await session.scalar(
        select(func.coalesce(func.max(RunEvent.sequence_number), 0)).where(
            RunEvent.run_id == run.id
        )
    )
    sequence_number = int(latest or 0) + 1
    event_id = event_id or new_id()
    event = RunEvent(
        id=event_id,
        run_id=run.id,
        step_id=step.id if step is not None else None,
        sequence_number=sequence_number,
        event_type=event_type,
        timestamp=timestamp or now_utc(),
        actor_type=actor_type.value,
        actor_id=actor_id,
        payload=safe_payload,
        trace_id=trace_id or run.id,
        span_id=span_id or (step.id if step is not None else None),
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
                "payload": safe_payload,
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
    if run.status == RunStatus.WAITING_FOR_APPROVAL.value:
        expired_approval = await session.scalar(
            select(ApprovalRequest.id).where(
                ApprovalRequest.run_id == run.id,
                ApprovalRequest.status == "pending",
                ApprovalRequest.expires_at.is_not(None),
                ApprovalRequest.expires_at <= now_utc(),
            )
        )
        if expired_approval is None:
            return run
        run.status = RunStatus.QUEUED.value
    if run.agent_version_id is None and run.workflow_version_id is None:
        return await fail_run(
            session,
            run,
            RunFailure(
                code=FailureCode.INTERNAL_ERROR.value,
                message="Run is missing an agent or workflow version.",
                details={},
            ),
        )

    agent_version: AgentVersion | None = None
    workflow_version: WorkflowVersion | None = None
    if run.workflow_version_id is not None:
        workflow_version = await session.get(WorkflowVersion, run.workflow_version_id)
        if workflow_version is None or workflow_version.status != VersionStatus.PUBLISHED.value:
            return await fail_run(
                session,
                run,
                RunFailure(
                    code=FailureCode.INTERNAL_ERROR.value,
                    message="Run references a missing or unpublished workflow version.",
                    details={"workflow_version_id": run.workflow_version_id},
                ),
            )
    else:
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
            payload={
                "agent_version_id": agent_version.id if agent_version is not None else None,
                "workflow_version_id": workflow_version.id
                if workflow_version is not None
                else None,
            },
        )
        await session.commit()

    try:
        if workflow_version is not None:
            output = await run_workflow(session, run, workflow_version, settings)
        else:
            assert agent_version is not None
            output = await run_agent_loop(session, run, agent_version, settings)
    except ApiError as exc:
        await session.rollback()
        run = await session.get(Run, run_id)
        if run is None:
            raise
        return await fail_run(session, run, failure_from_api_error(exc))

    if output is None:
        await session.refresh(run)
        return run

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
        session.add(
            usage_record_for(
                run=run,
                step=model_step,
                agent_version=agent_version,
                usage=result.usage,
            )
        )
        await append_run_event(
            session,
            run=run,
            step=model_step,
            event_type="model.completed",
            actor_type=RunEventActor.AGENT,
            payload={
                **model_step.output,
                "latency_ms": duration_ms(model_step.started_at, model_step.completed_at),
            },
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


async def run_workflow(
    session: AsyncSession,
    run: Run,
    workflow_version: WorkflowVersion,
    settings: Settings,
) -> dict[str, Any] | None:
    await recover_incomplete_steps(session, run)
    definition = workflow_version.definition
    nodes = {str(node["id"]): node for node in definition["nodes"]}
    edges_by_target = incoming_edges(definition)
    outputs, completed = await workflow_node_state(session, run.id)

    for layer in topological_layers(definition):
        if len(layer) > 1:
            await append_run_event(
                session,
                run=run,
                event_type="workflow.parallel.ready",
                actor_type=RunEventActor.WORKFLOW,
                payload={"node_ids": layer},
            )
            await session.commit()
        if can_run_parallel_agents(session, run, layer, nodes, edges_by_target, outputs, completed):
            parallel_outputs = await execute_parallel_agent_layer(
                session, run, layer, nodes, outputs, settings
            )
            outputs.update(parallel_outputs)
            completed.update(parallel_outputs)
            continue
        for node_id in layer:
            if node_id in completed:
                continue
            node = nodes[node_id]
            inbound = edges_by_target.get(node_id, [])
            if inbound and not any(edge_allows(edge, run.input, outputs) for edge in inbound):
                await skip_workflow_node(session, run, node)
                completed.add(node_id)
                continue

            resolved_input = resolve_value(node.get("input", {}), run.input, outputs)
            if not isinstance(resolved_input, dict):
                raise ApiError(
                    "INVALID_WORKFLOW",
                    "Workflow node input must resolve to an object.",
                    422,
                    {"node_id": node_id},
                )
            result = await execute_workflow_node(session, run, node, resolved_input, settings)
            if result is None:
                return None
            outputs[node_id] = result
            completed.add(node_id)

    terminal_nodes = {
        node_id
        for node_id in nodes
        if not any(str(edge["from"]) == node_id for edge in definition.get("edges", []))
    }
    terminal_output = (
        outputs.get(next(iter(terminal_nodes)))
        if len(terminal_nodes) == 1
        else {node_id: outputs.get(node_id) for node_id in sorted(terminal_nodes)}
    )
    return {"output": terminal_output, "nodes": outputs}


def can_run_parallel_agents(
    session: AsyncSession,
    run: Run,
    layer: list[str],
    nodes: dict[str, dict[str, Any]],
    edges_by_target: dict[str, list[dict[str, Any]]],
    outputs: dict[str, Any],
    completed: set[str],
) -> bool:
    if len(layer) < 2 or any(node_id in completed for node_id in layer):
        return False
    if any(str(nodes[node_id]["type"]) != "agent" for node_id in layer):
        return False
    if any(
        edges_by_target.get(node_id)
        and not any(edge_allows(edge, run.input, outputs) for edge in edges_by_target[node_id])
        for node_id in layer
    ):
        return False
    return session.bind is not None


async def execute_parallel_agent_layer(
    session: AsyncSession,
    run: Run,
    layer: list[str],
    nodes: dict[str, dict[str, Any]],
    outputs: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    prepared: list[tuple[str, RunStep, str]] = []
    for node_id in layer:
        node_input = resolve_value(nodes[node_id].get("input", {}), run.input, outputs)
        if not isinstance(node_input, dict):
            raise ApiError(
                "INVALID_WORKFLOW",
                "Workflow node input must resolve to an object.",
                422,
                {"node_id": node_id},
            )
        step, child_id = await prepare_agent_node(session, run, nodes[node_id], node_input)
        prepared.append((node_id, step, child_id))

    bind = session.bind
    if bind is None:
        raise ApiError("INTERNAL_ERROR", "Workflow session has no database bind.", 500)
    if bind.dialect.name == "sqlite":
        children = [await execute_run(session, child_id, settings) for _, _, child_id in prepared]
    else:
        factory = async_sessionmaker(bind, expire_on_commit=False)

        async def execute_isolated(child_id: str) -> Run:
            async with factory() as child_session:
                return await execute_run(child_session, child_id, settings)

        children = await asyncio.gather(
            *(execute_isolated(child_id) for _, _, child_id in prepared)
        )

    results: dict[str, Any] = {}
    for (node_id, step, child_id), child_run in zip(prepared, children, strict=True):
        results[node_id] = await finish_agent_node(session, run, step, child_id, child_run)
    return results


async def workflow_node_state(
    session: AsyncSession, run_id: str
) -> tuple[dict[str, Any], set[str]]:
    rows = await session.scalars(
        select(RunStep)
        .where(
            RunStep.run_id == run_id,
            RunStep.step_type.in_(
                [
                    RunStepType.AGENT.value,
                    RunStepType.TOOL.value,
                    RunStepType.APPROVAL.value,
                    RunStepType.BRANCH.value,
                ]
            ),
        )
        .order_by(RunStep.created_at.asc(), RunStep.attempt.asc())
    )
    outputs: dict[str, Any] = {}
    completed: set[str] = set()
    for step in rows:
        if step.status == RunStepStatus.SUCCEEDED.value:
            output = step.output or {}
            outputs[step.node_key] = output.get("output", output)
            completed.add(step.node_key)
        elif step.status == RunStepStatus.SKIPPED.value:
            completed.add(step.node_key)
    return outputs, completed


async def execute_workflow_node(
    session: AsyncSession,
    run: Run,
    node: dict[str, Any],
    node_input: dict[str, Any],
    settings: Settings,
) -> dict[str, Any] | None:
    node_type = str(node["type"])
    if node_type == "agent":
        return await execute_agent_node(session, run, node, node_input, settings)
    if node_type == "tool":
        return await execute_tool_node(session, run, node, node_input)
    return await execute_approval_node(session, run, node, node_input)


async def execute_agent_node(
    session: AsyncSession,
    run: Run,
    node: dict[str, Any],
    node_input: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    step, child_id = await prepare_agent_node(session, run, node, node_input)
    child_run = await execute_run(session, child_id, settings)
    return await finish_agent_node(session, run, step, child_id, child_run)


async def prepare_agent_node(
    session: AsyncSession,
    run: Run,
    node: dict[str, Any],
    node_input: dict[str, Any],
) -> tuple[RunStep, str]:
    step = await start_step(
        session,
        run=run,
        node_key=str(node["id"]),
        step_type=RunStepType.AGENT,
        name=str(node.get("name") or node["id"]),
        payload=node_input,
    )
    child_id = new_id()
    child_run = Run(
        id=child_id,
        organization_id=run.organization_id,
        project_id=run.project_id,
        agent_version_id=str(node["agent_version_id"]),
        workflow_version_id=None,
        parent_run_id=run.id,
        root_run_id=run.root_run_id,
        temporal_workflow_id=f"local-run-{child_id}",
        temporal_run_id=None,
        idempotency_key=None,
        status=RunStatus.QUEUED.value,
        input=node_input,
        output=None,
        created_by=run.created_by,
    )
    session.add(child_run)
    await session.flush()
    await append_run_event(
        session,
        run=child_run,
        event_type="run.queued",
        actor_type=RunEventActor.WORKFLOW,
        payload={"parent_run_id": run.id, "agent_version_id": child_run.agent_version_id},
    )
    await append_run_event(
        session,
        run=run,
        step=step,
        event_type="agent.handoff.started",
        actor_type=RunEventActor.WORKFLOW,
        payload={"child_run_id": child_id, "agent_version_id": child_run.agent_version_id},
    )
    await session.commit()
    return step, child_id


async def finish_agent_node(
    session: AsyncSession,
    run: Run,
    step: RunStep,
    child_id: str,
    child_run: Run,
) -> dict[str, Any]:
    if child_run.status != RunStatus.SUCCEEDED.value:
        raise ApiError(
            "CHILD_RUN_FAILED",
            "Workflow child agent did not succeed.",
            502,
            {"child_run_id": child_id, "status": child_run.status},
        )
    result = child_run.output or {}
    await complete_workflow_step(
        session,
        run,
        step,
        {"child_run_id": child_id, "output": result},
        event_type="agent.handoff.completed",
    )
    return result


async def execute_tool_node(
    session: AsyncSession,
    run: Run,
    node: dict[str, Any],
    node_input: dict[str, Any],
) -> dict[str, Any]:
    tool_version_id = str(node["tool_version_id"])
    bindings = await load_tool_bindings(session, [tool_version_id])
    if not bindings:
        raise ApiError("INVALID_WORKFLOW", "Workflow tool version is unavailable.", 422)
    binding = bindings[0]
    step = await start_step(
        session,
        run=run,
        node_key=str(node["id"]),
        step_type=RunStepType.TOOL,
        name=binding.name,
        payload=node_input,
    )
    await session.commit()
    try:
        result = await ToolExecutor().execute(binding, node_input)
    except ApiError as exc:
        step.status = RunStepStatus.FAILED.value
        step.error = {"code": exc.code, "message": exc.message, "details": exc.details}
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
        raise
    await complete_workflow_step(
        session,
        run,
        step,
        {"output": result},
        event_type="tool.completed",
    )
    return result


async def execute_approval_node(
    session: AsyncSession,
    run: Run,
    node: dict[str, Any],
    node_input: dict[str, Any],
) -> dict[str, Any] | None:
    node_key = str(node["id"])
    approval = await session.scalar(
        select(ApprovalRequest).where(
            ApprovalRequest.run_id == run.id, ApprovalRequest.node_key == node_key
        )
    )
    if approval is None:
        step = await start_step(
            session,
            run=run,
            node_key=node_key,
            step_type=RunStepType.APPROVAL,
            name=str(node.get("name") or node_key),
            payload=node_input,
        )
        step.status = RunStepStatus.WAITING.value
        timeout = node.get("timeout_seconds")
        expires_at = now_utc() + timedelta(seconds=int(timeout)) if timeout else None
        approval = ApprovalRequest(
            organization_id=run.organization_id,
            project_id=run.project_id,
            run_id=run.id,
            step_id=step.id,
            node_key=node_key,
            prompt=str(node.get("prompt") or "Approval required."),
            input=node_input,
            expires_at=expires_at,
            created_by=run.created_by,
        )
        session.add(approval)
        await session.flush()
        await append_run_event(
            session,
            run=run,
            step=step,
            event_type="approval.requested",
            actor_type=RunEventActor.WORKFLOW,
            payload={
                "approval_id": approval.id,
                "prompt": approval.prompt,
                "expires_at": expires_at.isoformat() if expires_at else None,
            },
        )
        run.status = RunStatus.WAITING_FOR_APPROVAL.value
        await append_run_event(
            session,
            run=run,
            event_type="run.waiting_for_approval",
            actor_type=RunEventActor.WORKFLOW,
            payload={"approval_id": approval.id},
        )
        await session.commit()
        return None

    approval_step = await session.get(RunStep, approval.step_id)
    if approval_step is None:
        raise ApiError("APPROVAL_NOT_FOUND", "Approval step no longer exists.", 404)
    if approval.status == "pending" and approval.expires_at and approval.expires_at <= now_utc():
        approval.status = "expired"
        approval.decision = {"approved": False, "comment": "Approval timed out."}
        approval.decided_at = now_utc()
        await append_run_event(
            session,
            run=run,
            step=approval_step,
            event_type="approval.expired",
            actor_type=RunEventActor.WORKFLOW,
            payload={"approval_id": approval.id},
        )
    if approval.status == "pending":
        run.status = RunStatus.WAITING_FOR_APPROVAL.value
        await session.commit()
        return None

    decision = approval.decision or {"approved": approval.status == "approved"}
    approval_step.status = RunStepStatus.SUCCEEDED.value
    approval_step.output = {"output": decision}
    approval_step.completed_at = now_utc()
    await append_run_event(
        session,
        run=run,
        step=approval_step,
        event_type="step.succeeded",
        actor_type=RunEventActor.WORKFLOW,
        payload={"node_key": node_key, "output": decision},
    )
    await session.commit()
    return decision


async def complete_workflow_step(
    session: AsyncSession,
    run: Run,
    step: RunStep,
    output: dict[str, Any],
    *,
    event_type: str,
) -> None:
    step.status = RunStepStatus.SUCCEEDED.value
    step.output = output
    step.completed_at = now_utc()
    await append_run_event(
        session,
        run=run,
        step=step,
        event_type=event_type,
        actor_type=RunEventActor.WORKFLOW,
        payload={"node_key": step.node_key, "output": output},
    )
    await append_run_event(
        session,
        run=run,
        step=step,
        event_type="step.succeeded",
        actor_type=RunEventActor.WORKFLOW,
        payload={"node_key": step.node_key},
    )
    await session.commit()


async def skip_workflow_node(session: AsyncSession, run: Run, node: dict[str, Any]) -> None:
    step = await start_step(
        session,
        run=run,
        node_key=str(node["id"]),
        step_type=RunStepType.BRANCH,
        name=str(node.get("name") or node["id"]),
        payload={},
    )
    step.status = RunStepStatus.SKIPPED.value
    step.completed_at = now_utc()
    await append_run_event(
        session,
        run=run,
        step=step,
        event_type="step.skipped",
        actor_type=RunEventActor.WORKFLOW,
        payload={"node_key": step.node_key, "reason": "edge_condition"},
    )
    await session.commit()


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
                payload={
                    **step.error,
                    "latency_ms": duration_ms(step.started_at, step.completed_at),
                },
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
                "latency_ms": duration_ms(step.started_at, step.completed_at),
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
    attempt = await session.scalar(
        select(func.coalesce(func.max(RunStep.attempt), 0)).where(
            RunStep.run_id == run.id,
            RunStep.node_key == node_key,
        )
    )
    step = RunStep(
        run_id=run.id,
        node_key=node_key,
        step_type=step_type.value,
        name=name,
        status=RunStepStatus.RUNNING.value,
        attempt=int(attempt or 0) + 1,
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
    approvals = await session.scalars(
        select(ApprovalRequest).where(
            ApprovalRequest.run_id == run.id, ApprovalRequest.status == "pending"
        )
    )
    for approval in approvals:
        approval.status = "cancelled"
        approval.decision = {"approved": False, "comment": "Run was cancelled."}
        approval.decided_by = actor_id
        approval.decided_at = now_utc()
        await append_run_event(
            session,
            run=run,
            event_type="approval.cancelled",
            actor_type=RunEventActor.USER,
            actor_id=actor_id,
            payload={"approval_id": approval.id},
        )
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
