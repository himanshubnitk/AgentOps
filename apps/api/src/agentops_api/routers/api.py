from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
import secrets
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from agentops_domain.enums import MembershipRole, RunEventActor, RunStatus, VersionStatus
from agentops_domain.hashing import canonical_hash
from agentops_persistence.models import (
    Agent,
    AgentVersion,
    ApiKey,
    ApprovalRequest,
    Dataset,
    DatasetCase,
    Experiment,
    ExperimentRun,
    Organization,
    OrganizationMembership,
    Project,
    Run,
    RunEvent,
    RunStep,
    Tool,
    ToolVersion,
    UsageRecord,
    User,
    Workflow,
    WorkflowVersion,
    new_id,
    now_utc,
)
from fastapi import APIRouter, Depends, Header, Response, WebSocket, WebSocketDisconnect, status
from jsonschema import Draft202012Validator, SchemaError
from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from agentops_api.config import get_settings
from agentops_api.dependencies import get_current_user, get_session
from agentops_api.errors import ApiError
from agentops_api.evaluations import deterministic_score, model_judge_score
from agentops_api.execution import append_run_event, cancel_run, execute_run
from agentops_api.observability import duration_ms
from agentops_api.redis_live import open_run_event_subscription
from agentops_api.relay import run_channel
from agentops_api.schemas import (
    AgentRead,
    AgentVersionCreate,
    AgentVersionRead,
    ApiKeyCreate,
    ApiKeyRead,
    ApprovalDecision,
    ApprovalRead,
    DatasetCaseCreate,
    DatasetCaseRead,
    DatasetRead,
    ExperimentCreate,
    ExperimentRead,
    ExperimentRunRead,
    ExternalTraceEvent,
    LoginRequest,
    NamedCreate,
    OrganizationRead,
    ProjectRead,
    RefreshRequest,
    RunCostRead,
    RunCreate,
    RunEventRead,
    RunRead,
    RunStepRead,
    RunTraceRead,
    TokenPair,
    ToolRead,
    ToolVersionCreate,
    ToolVersionRead,
    TraceBatchCreate,
    TraceBatchRead,
    TraceTreeNode,
    UsageRecordRead,
    UserCreate,
    UserRead,
    WebSocketTicketRead,
    WorkflowRead,
    WorkflowVersionCreate,
    WorkflowVersionRead,
)
from agentops_api.security import create_token, decode_token, hash_password, verify_password
from agentops_api.workflows import validate_definition

router = APIRouter()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "item"


def token_pair(user: User) -> TokenPair:
    settings = get_settings()
    return TokenPair(
        access_token=create_token(
            subject=user.id,
            token_type="access",
            settings=settings,
            expires_delta=timedelta(minutes=settings.access_token_minutes),
        ),
        refresh_token=create_token(
            subject=user.id,
            token_type="refresh",
            settings=settings,
            expires_delta=timedelta(days=settings.refresh_token_days),
        ),
    )


async def commit(session: AsyncSession) -> None:
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ApiError("CONFLICT", "Resource already exists.", 409) from exc


async def user_project(session: AsyncSession, user: User, project_id: str) -> Project:
    statement = (
        select(Project)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(Project.id == project_id, OrganizationMembership.user_id == user.id)
    )
    project = await session.scalar(statement)
    if project is None:
        raise ApiError("PROJECT_NOT_FOUND", "Project not found.", 404)
    return project


async def member_organization(
    session: AsyncSession, user: User, organization_id: str
) -> Organization:
    statement = (
        select(Organization)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Organization.id,
        )
        .where(Organization.id == organization_id, OrganizationMembership.user_id == user.id)
    )
    organization = await session.scalar(statement)
    if organization is None:
        raise ApiError("ORGANIZATION_NOT_FOUND", "Organization not found.", 404)
    return organization


async def user_agent(session: AsyncSession, user: User, agent_id: str) -> Agent:
    statement = (
        select(Agent)
        .join(Project, Project.id == Agent.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(
            Agent.id == agent_id,
            Agent.archived_at.is_(None),
            OrganizationMembership.user_id == user.id,
        )
    )
    agent = await session.scalar(statement)
    if agent is None:
        raise ApiError("AGENT_NOT_FOUND", "Agent not found.", 404)
    return agent


async def user_tool(session: AsyncSession, user: User, tool_id: str) -> Tool:
    statement = (
        select(Tool)
        .join(Project, Project.id == Tool.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(Tool.id == tool_id, OrganizationMembership.user_id == user.id)
    )
    tool = await session.scalar(statement)
    if tool is None:
        raise ApiError("TOOL_NOT_FOUND", "Tool not found.", 404)
    return tool


async def user_agent_version(session: AsyncSession, user: User, version_id: str) -> AgentVersion:
    statement = (
        select(AgentVersion)
        .join(Agent, Agent.id == AgentVersion.agent_id)
        .join(Project, Project.id == Agent.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(AgentVersion.id == version_id, OrganizationMembership.user_id == user.id)
    )
    version = await session.scalar(statement)
    if version is None:
        raise ApiError("AGENT_VERSION_NOT_FOUND", "Agent version not found.", 404)
    return version


async def user_tool_version(session: AsyncSession, user: User, version_id: str) -> ToolVersion:
    statement = (
        select(ToolVersion)
        .join(Tool, Tool.id == ToolVersion.tool_id)
        .join(Project, Project.id == Tool.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(ToolVersion.id == version_id, OrganizationMembership.user_id == user.id)
    )
    version = await session.scalar(statement)
    if version is None:
        raise ApiError("TOOL_VERSION_NOT_FOUND", "Tool version not found.", 404)
    return version


async def user_workflow(session: AsyncSession, user: User, workflow_id: str) -> Workflow:
    statement = (
        select(Workflow)
        .join(Project, Project.id == Workflow.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(Workflow.id == workflow_id, OrganizationMembership.user_id == user.id)
    )
    workflow = await session.scalar(statement)
    if workflow is None:
        raise ApiError("WORKFLOW_NOT_FOUND", "Workflow not found.", 404)
    return workflow


async def user_workflow_version(
    session: AsyncSession, user: User, version_id: str
) -> WorkflowVersion:
    statement = (
        select(WorkflowVersion)
        .join(Workflow, Workflow.id == WorkflowVersion.workflow_id)
        .join(Project, Project.id == Workflow.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(WorkflowVersion.id == version_id, OrganizationMembership.user_id == user.id)
    )
    version = await session.scalar(statement)
    if version is None:
        raise ApiError("WORKFLOW_VERSION_NOT_FOUND", "Workflow version not found.", 404)
    return version


async def user_dataset(session: AsyncSession, user: User, dataset_id: str) -> Dataset:
    statement = (
        select(Dataset)
        .join(Project, Project.id == Dataset.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(Dataset.id == dataset_id, OrganizationMembership.user_id == user.id)
    )
    dataset = await session.scalar(statement)
    if dataset is None:
        raise ApiError("DATASET_NOT_FOUND", "Dataset not found.", 404)
    return dataset


async def user_approval(session: AsyncSession, user: User, approval_id: str) -> ApprovalRequest:
    statement = (
        select(ApprovalRequest)
        .join(Project, Project.id == ApprovalRequest.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(ApprovalRequest.id == approval_id, OrganizationMembership.user_id == user.id)
    )
    approval = await session.scalar(statement)
    if approval is None:
        raise ApiError("APPROVAL_NOT_FOUND", "Approval request not found.", 404)
    return approval


async def user_run(session: AsyncSession, user: User, run_id: str) -> Run:
    statement = (
        select(Run)
        .join(Project, Project.id == Run.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(Run.id == run_id, OrganizationMembership.user_id == user.id)
    )
    run = await session.scalar(statement)
    if run is None:
        raise ApiError("RUN_NOT_FOUND", "Run not found.", 404)
    return run


def validate_json_schema(schema: dict[str, Any], field: str) -> None:
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ApiError(
            "INVALID_JSON_SCHEMA",
            f"{field} is not a valid JSON Schema.",
            422,
            {"reason": exc.message},
        ) from exc


def agent_version_hash(version: AgentVersion) -> str:
    return canonical_hash(
        {
            "instructions": version.instructions,
            "model_provider": version.model_provider,
            "model_name": version.model_name,
            "model_parameters": version.model_parameters,
            "tool_version_ids": version.tool_version_ids,
            "memory_config": version.memory_config,
            "guardrail_config": version.guardrail_config,
            "max_steps": version.max_steps,
        }
    )


def tool_version_hash(version: ToolVersion) -> str:
    return canonical_hash(
        {
            "kind": version.kind,
            "input_schema": version.input_schema,
            "output_schema": version.output_schema,
            "execution_config": version.execution_config,
            "timeout_seconds": version.timeout_seconds,
            "retry_policy": version.retry_policy,
            "side_effect_level": version.side_effect_level,
        }
    )


def workflow_version_hash(version: WorkflowVersion) -> str:
    return canonical_hash(version.definition)


async def next_workflow_version_number(session: AsyncSession, workflow_id: str) -> int:
    latest = await session.scalar(
        select(func.coalesce(func.max(WorkflowVersion.version_number), 0)).where(
            WorkflowVersion.workflow_id == workflow_id
        )
    )
    return int(latest or 0) + 1


async def validate_workflow_dependencies(
    session: AsyncSession, workflow: Workflow, version: WorkflowVersion
) -> None:
    validate_definition(version.definition)
    for node in version.definition["nodes"]:
        node_type = node["type"]
        if node_type == "agent":
            found = await session.scalar(
                select(AgentVersion.id)
                .join(Agent, Agent.id == AgentVersion.agent_id)
                .where(
                    AgentVersion.id == node["agent_version_id"],
                    AgentVersion.status == VersionStatus.PUBLISHED.value,
                    Agent.project_id == workflow.project_id,
                )
            )
            if found is None:
                raise ApiError(
                    "INVALID_WORKFLOW",
                    "Workflow references an unpublished or cross-project agent version.",
                    422,
                    {"node_id": node["id"]},
                )
        elif node_type == "tool":
            found = await session.scalar(
                select(ToolVersion.id)
                .join(Tool, Tool.id == ToolVersion.tool_id)
                .where(
                    ToolVersion.id == node["tool_version_id"],
                    ToolVersion.status == VersionStatus.PUBLISHED.value,
                    Tool.project_id == workflow.project_id,
                )
            )
            if found is None:
                raise ApiError(
                    "INVALID_WORKFLOW",
                    "Workflow references an unpublished or cross-project tool version.",
                    422,
                    {"node_id": node["id"]},
                )


async def scoped_list(session: AsyncSession, statement: Select[tuple[Any]]) -> list[Any]:
    result = await session.scalars(statement)
    return list(result.all())


async def run_usage_records(session: AsyncSession, run_id: str) -> list[UsageRecord]:
    result = await session.scalars(
        select(UsageRecord).where(UsageRecord.run_id == run_id).order_by(UsageRecord.created_at)
    )
    return list(result.all())


async def latest_run_sequence(session: AsyncSession, run_id: str) -> int:
    latest = await session.scalar(
        select(func.coalesce(func.max(RunEvent.sequence_number), 0)).where(
            RunEvent.run_id == run_id
        )
    )
    return int(latest or 0)


async def run_events_after(
    session: AsyncSession,
    run_id: str,
    after_sequence: int,
    *,
    limit: int = 100,
) -> list[RunEvent]:
    result = await session.scalars(
        select(RunEvent)
        .where(RunEvent.run_id == run_id, RunEvent.sequence_number > after_sequence)
        .order_by(RunEvent.sequence_number.asc())
        .limit(limit)
    )
    return list(result.all())


async def send_run_events_after(
    websocket: WebSocket,
    session: AsyncSession,
    run_id: str,
    after_sequence: int,
    seen_event_ids: set[str],
) -> int:
    last_sequence = after_sequence
    while True:
        await session.rollback()
        events = await run_events_after(session, run_id, last_sequence)
        if not events:
            return last_sequence
        for event in events:
            last_sequence = max(last_sequence, event.sequence_number)
            if event.id in seen_event_ids:
                continue
            seen_event_ids.add(event.id)
            await websocket.send_json(
                {
                    "type": "event",
                    "event": RunEventRead.model_validate(event).model_dump(mode="json"),
                }
            )


def run_cost(run_id: str, records: list[UsageRecord]) -> RunCostRead:
    currency = records[0].currency if records else "USD"
    input_tokens = sum(record.input_tokens for record in records)
    output_tokens = sum(record.output_tokens for record in records)
    cached_input_tokens = sum(record.cached_input_tokens for record in records)
    reasoning_tokens = sum(record.reasoning_tokens for record in records)
    return RunCostRead(
        run_id=run_id,
        currency=currency,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
        reasoning_tokens=reasoning_tokens,
        total_tokens=input_tokens + output_tokens + cached_input_tokens + reasoning_tokens,
        total_cost=sum((record.calculated_cost for record in records), Decimal("0")),
        records=[UsageRecordRead.model_validate(record) for record in records],
    )


def trace_tree(
    steps: list[RunStep],
    events: list[RunEvent],
    usage_records: list[UsageRecord],
) -> list[TraceTreeNode]:
    events_by_step: dict[str | None, list[RunEventRead]] = defaultdict(list)
    for event in events:
        events_by_step[event.step_id].append(RunEventRead.model_validate(event))

    usage_by_step: dict[str | None, list[UsageRecordRead]] = defaultdict(list)
    for record in usage_records:
        usage_by_step[record.step_id].append(UsageRecordRead.model_validate(record))

    nodes = {
        step.id: TraceTreeNode(
            id=step.id,
            parent_step_id=step.parent_step_id,
            node_key=step.node_key,
            step_type=step.step_type,
            name=step.name,
            status=step.status,
            attempt=step.attempt,
            started_at=step.started_at,
            completed_at=step.completed_at,
            duration_ms=duration_ms(step.started_at, step.completed_at),
            input=step.input,
            output=step.output,
            error=step.error,
            events=events_by_step[step.id],
            usage=usage_by_step[step.id],
        )
        for step in steps
    }

    roots: list[TraceTreeNode] = []
    for step in steps:
        node = nodes[step.id]
        parent = nodes.get(step.parent_step_id or "")
        if parent is None:
            roots.append(node)
        else:
            parent.children.append(node)
    return roots


async def next_agent_version_number(session: AsyncSession, agent_id: str) -> int:
    latest = await session.scalar(
        select(func.coalesce(func.max(AgentVersion.version_number), 0)).where(
            AgentVersion.agent_id == agent_id
        )
    )
    return int(latest or 0) + 1


async def next_tool_version_number(session: AsyncSession, tool_id: str) -> int:
    latest = await session.scalar(
        select(func.coalesce(func.max(ToolVersion.version_number), 0)).where(
            ToolVersion.tool_id == tool_id
        )
    )
    return int(latest or 0) + 1


async def validate_agent_tools(session: AsyncSession, agent: Agent, version: AgentVersion) -> None:
    if not version.tool_version_ids:
        return
    result = await session.scalars(
        select(ToolVersion.id)
        .join(Tool, Tool.id == ToolVersion.tool_id)
        .where(
            ToolVersion.id.in_(version.tool_version_ids),
            ToolVersion.status == VersionStatus.PUBLISHED.value,
            Tool.project_id == agent.project_id,
        )
    )
    found = set(result.all())
    missing = sorted(set(version.tool_version_ids) - found)
    if missing:
        raise ApiError(
            "INVALID_AGENT_VERSION",
            "Agent references missing, unpublished, or cross-project tool versions.",
            422,
            {"tool_version_ids": missing},
        )


@router.post("/auth/register", response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def register(request: UserCreate, session: AsyncSession = Depends(get_session)) -> User:
    user = User(
        email=request.email.strip().lower(),
        password_hash=hash_password(request.password),
        display_name=request.display_name,
    )
    session.add(user)
    await commit(session)
    await session.refresh(user)
    return user


@router.post("/auth/login", response_model=TokenPair)
async def login(request: LoginRequest, session: AsyncSession = Depends(get_session)) -> TokenPair:
    user = await session.scalar(select(User).where(User.email == request.email.strip().lower()))
    if user is None or not verify_password(request.password, user.password_hash):
        raise ApiError("INVALID_CREDENTIALS", "Invalid email or password.", 401)
    return token_pair(user)


@router.post("/auth/refresh", response_model=TokenPair)
async def refresh(
    request: RefreshRequest, session: AsyncSession = Depends(get_session)
) -> TokenPair:
    data = decode_token(request.refresh_token, settings=get_settings(), token_type="refresh")
    user = await session.scalar(
        select(User).where(User.id == data["sub"], User.is_active.is_(True))
    )
    if user is None:
        raise ApiError("UNAUTHENTICATED", "Authentication required.", 401)
    return token_pair(user)


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout() -> Response:
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/auth/me", response_model=UserRead)
async def me(user: User = Depends(get_current_user)) -> User:
    return user


@router.post("/organizations", response_model=OrganizationRead, status_code=status.HTTP_201_CREATED)
async def create_organization(
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Organization:
    organization = Organization(name=request.name, slug=request.slug or slugify(request.name))
    session.add(organization)
    await session.flush()
    session.add(
        OrganizationMembership(
            organization_id=organization.id,
            user_id=user.id,
            role=MembershipRole.OWNER.value,
        )
    )
    await commit(session)
    await session.refresh(organization)
    return organization


@router.get("/organizations", response_model=list[OrganizationRead])
async def list_organizations(
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Organization]:
    return await scoped_list(
        session,
        select(Organization)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Organization.id,
        )
        .where(OrganizationMembership.user_id == user.id)
        .order_by(Organization.created_at.desc()),
    )


@router.get("/organizations/{organization_id}", response_model=OrganizationRead)
async def get_organization(
    organization_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Organization:
    return await member_organization(session, user, organization_id)


@router.post(
    "/organizations/{organization_id}/projects",
    response_model=ProjectRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_project(
    organization_id: str,
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Project:
    organization = await member_organization(session, user, organization_id)
    project = Project(
        organization_id=organization.id,
        name=request.name,
        slug=request.slug or slugify(request.name),
        description=request.description,
    )
    session.add(project)
    await commit(session)
    await session.refresh(project)
    return project


@router.get("/organizations/{organization_id}/projects", response_model=list[ProjectRead])
async def list_projects(
    organization_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Project]:
    await member_organization(session, user, organization_id)
    return await scoped_list(
        session,
        select(Project)
        .where(Project.organization_id == organization_id)
        .order_by(Project.created_at.desc()),
    )


@router.get("/projects/{project_id}", response_model=ProjectRead)
async def get_project(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Project:
    return await user_project(session, user, project_id)


@router.post(
    "/projects/{project_id}/agents", response_model=AgentRead, status_code=status.HTTP_201_CREATED
)
async def create_agent(
    project_id: str,
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Agent:
    project = await user_project(session, user, project_id)
    agent = Agent(
        organization_id=project.organization_id,
        project_id=project.id,
        name=request.name,
        slug=request.slug or slugify(request.name),
        description=request.description,
        created_by=user.id,
    )
    session.add(agent)
    await commit(session)
    await session.refresh(agent)
    return agent


@router.get("/projects/{project_id}/agents", response_model=list[AgentRead])
async def list_agents(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Agent]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(Agent)
        .where(Agent.project_id == project_id, Agent.archived_at.is_(None))
        .order_by(Agent.created_at.desc()),
    )


@router.get("/agents/{agent_id}", response_model=AgentRead)
async def get_agent(
    agent_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Agent:
    return await user_agent(session, user, agent_id)


@router.patch("/agents/{agent_id}", response_model=AgentRead)
async def update_agent(
    agent_id: str,
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Agent:
    agent = await user_agent(session, user, agent_id)
    agent.name = request.name
    agent.slug = request.slug or slugify(request.name)
    agent.description = request.description
    await commit(session)
    await session.refresh(agent)
    return agent


@router.delete("/agents/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Response:
    agent = await user_agent(session, user, agent_id)
    agent.archived_at = now_utc()
    await commit(session)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/agents/{agent_id}/versions",
    response_model=AgentVersionRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_agent_version(
    agent_id: str,
    request: AgentVersionCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> AgentVersion:
    agent = await user_agent(session, user, agent_id)
    version = AgentVersion(
        agent_id=agent.id,
        version_number=await next_agent_version_number(session, agent.id),
        instructions=request.instructions,
        model_provider=request.model_provider,
        model_name=request.model_name,
        model_parameters=request.model_parameters,
        tool_version_ids=request.tool_version_ids,
        memory_config=request.memory_config,
        guardrail_config=request.guardrail_config,
        max_steps=request.max_steps,
        created_by=user.id,
    )
    session.add(version)
    await commit(session)
    await session.refresh(version)
    return version


@router.get("/agents/{agent_id}/versions", response_model=list[AgentVersionRead])
async def list_agent_versions(
    agent_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[AgentVersion]:
    agent = await user_agent(session, user, agent_id)
    return await scoped_list(
        session,
        select(AgentVersion)
        .where(AgentVersion.agent_id == agent.id)
        .order_by(AgentVersion.version_number.desc()),
    )


@router.get("/agent-versions/{version_id}", response_model=AgentVersionRead)
async def get_agent_version(
    version_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> AgentVersion:
    return await user_agent_version(session, user, version_id)


@router.post("/agent-versions/{version_id}/publish", response_model=AgentVersionRead)
async def publish_agent_version(
    version_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> AgentVersion:
    version = await user_agent_version(session, user, version_id)
    if version.status == VersionStatus.PUBLISHED.value:
        return version
    agent = await session.scalar(select(Agent).where(Agent.id == version.agent_id))
    if agent is None:
        raise ApiError("AGENT_NOT_FOUND", "Agent not found.", 404)
    await validate_agent_tools(session, agent, version)
    version.status = VersionStatus.PUBLISHED.value
    version.configuration_hash = agent_version_hash(version)
    version.published_at = datetime.now(UTC)
    agent.latest_version_number = max(agent.latest_version_number, version.version_number)
    await commit(session)
    await session.refresh(version)
    return version


@router.post(
    "/agent-versions/{version_id}/runs",
    response_model=RunRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_agent_run(
    version_id: str,
    request: RunCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Run:
    version = await user_agent_version(session, user, version_id)
    if version.status != VersionStatus.PUBLISHED.value:
        raise ApiError("AGENT_VERSION_NOT_PUBLISHED", "Agent version must be published.", 422)

    agent = await session.scalar(select(Agent).where(Agent.id == version.agent_id))
    if agent is None:
        raise ApiError("AGENT_NOT_FOUND", "Agent not found.", 404)

    if request.idempotency_key:
        existing = await session.scalar(
            select(Run).where(
                Run.project_id == agent.project_id,
                Run.idempotency_key == request.idempotency_key,
            )
        )
        if existing is not None:
            settings = get_settings()
            if settings.inline_run_execution and not request.defer:
                existing = await execute_run(session, existing.id, settings)
            return existing

    run_id = new_id()
    run = Run(
        id=run_id,
        organization_id=agent.organization_id,
        project_id=agent.project_id,
        agent_version_id=version.id,
        workflow_version_id=None,
        parent_run_id=None,
        root_run_id=run_id,
        temporal_workflow_id=f"local-run-{run_id}",
        temporal_run_id=None,
        idempotency_key=request.idempotency_key,
        status=RunStatus.QUEUED.value,
        input=request.input,
        output=None,
        created_by=user.id,
    )
    session.add(run)
    await session.flush()
    await append_run_event(
        session,
        run=run,
        event_type="run.queued",
        actor_type=RunEventActor.API,
        actor_id=user.id,
        payload={
            "agent_version_id": version.id,
            "idempotency_key": request.idempotency_key,
            "deferred": request.defer,
        },
    )
    await commit(session)

    settings = get_settings()
    if settings.inline_run_execution and not request.defer:
        run = await execute_run(session, run.id, settings)
    return run


@router.get("/projects/{project_id}/runs", response_model=list[RunRead])
async def list_project_runs(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Run]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(Run).where(Run.project_id == project_id).order_by(Run.created_at.desc()),
    )


@router.get("/runs/{run_id}", response_model=RunRead)
async def get_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Run:
    return await user_run(session, user, run_id)


@router.post("/runs/{run_id}/resume", response_model=RunRead)
async def resume_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Run:
    run = await user_run(session, user, run_id)
    return await execute_run(session, run.id, get_settings())


@router.post("/runs/{run_id}/cancel", response_model=RunRead)
async def cancel_active_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Run:
    run = await user_run(session, user, run_id)
    return await cancel_run(session, run, user.id)


@router.get("/runs/{run_id}/steps", response_model=list[RunStepRead])
async def list_run_steps(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[RunStep]:
    await user_run(session, user, run_id)
    return await scoped_list(
        session,
        select(RunStep).where(RunStep.run_id == run_id).order_by(RunStep.created_at.asc()),
    )


@router.get("/runs/{run_id}/events", response_model=list[RunEventRead])
async def list_run_events(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[RunEvent]:
    await user_run(session, user, run_id)
    return await scoped_list(
        session,
        select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.sequence_number.asc()),
    )


@router.get("/runs/{run_id}/trace", response_model=RunTraceRead)
async def get_run_trace(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> RunTraceRead:
    run = await user_run(session, user, run_id)
    steps = await scoped_list(
        session,
        select(RunStep).where(RunStep.run_id == run_id).order_by(RunStep.created_at.asc()),
    )
    events = await scoped_list(
        session,
        select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.sequence_number.asc()),
    )
    usage_records = await run_usage_records(session, run_id)
    return RunTraceRead(
        run=RunRead.model_validate(run),
        steps=steps,
        events=events,
        tree=trace_tree(steps, events, usage_records),
        usage=usage_records,
        cost=run_cost(run_id, usage_records),
    )


@router.get("/runs/{run_id}/cost", response_model=RunCostRead)
async def get_run_cost(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> RunCostRead:
    await user_run(session, user, run_id)
    return run_cost(run_id, await run_usage_records(session, run_id))


@router.post("/runs/{run_id}/ws-ticket", response_model=WebSocketTicketRead)
async def create_run_ws_ticket(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> WebSocketTicketRead:
    run = await user_run(session, user, run_id)
    expires_in_seconds = 60
    return WebSocketTicketRead(
        ticket=create_token(
            subject=user.id,
            token_type="ws",
            settings=get_settings(),
            expires_delta=timedelta(seconds=expires_in_seconds),
            claims={"run_id": run.id, "project_id": run.project_id},
        ),
        expires_in_seconds=expires_in_seconds,
    )


@router.websocket("/ws/projects/{project_id}/runs/{run_id}")
async def run_websocket(
    websocket: WebSocket,
    project_id: str,
    run_id: str,
    session: AsyncSession = Depends(get_session),
) -> None:
    ticket = websocket.query_params.get("ticket")
    if ticket is None:
        await websocket.close(code=1008)
        return

    try:
        data = decode_token(ticket, settings=get_settings(), token_type="ws")
    except ApiError:
        await websocket.close(code=1008)
        return

    if data.get("run_id") != run_id or data.get("project_id") != project_id:
        await websocket.close(code=1008)
        return

    user = await session.scalar(
        select(User).where(User.id == data["sub"], User.is_active.is_(True))
    )
    if user is None:
        await websocket.close(code=1008)
        return

    try:
        run = await user_run(session, user, run_id)
    except ApiError:
        await websocket.close(code=1008)
        return
    if run.project_id != project_id:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    last_sequence = int(websocket.query_params.get("after_sequence") or 0)
    seen_event_ids: set[str] = set()
    await websocket.send_json(
        {
            "type": "snapshot",
            "run": RunRead.model_validate(run).model_dump(mode="json"),
            "latest_sequence": await latest_run_sequence(session, run_id),
        }
    )
    last_sequence = await send_run_events_after(
        websocket,
        session,
        run_id,
        last_sequence,
        seen_event_ids,
    )
    current_run = await session.get(Run, run_id, populate_existing=True)
    if current_run is None:
        await websocket.close(code=1008)
        return
    if current_run.status in {
        RunStatus.SUCCEEDED.value,
        RunStatus.FAILED.value,
        RunStatus.CANCELLED.value,
    } and last_sequence >= await latest_run_sequence(session, run_id):
        await websocket.send_json({"type": "complete", "latest_sequence": last_sequence})
        await websocket.close(code=1000)
        return

    settings = get_settings()
    async with open_run_event_subscription(settings.redis_url, run_channel(run_id)) as subscription:
        while True:
            await session.rollback()
            current_run = await session.get(Run, run_id, populate_existing=True)
            if current_run is None:
                await websocket.close(code=1008)
                return
            if current_run.status in {
                RunStatus.SUCCEEDED.value,
                RunStatus.FAILED.value,
                RunStatus.CANCELLED.value,
            } and last_sequence >= await latest_run_sequence(session, run_id):
                await websocket.send_json({"type": "complete", "latest_sequence": last_sequence})
                await websocket.close(code=1000)
                return

            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.01)
                if message.get("type") == "resume":
                    last_sequence = max(0, int(message.get("after_sequence") or 0))
                    seen_event_ids.clear()
                    last_sequence = await send_run_events_after(
                        websocket,
                        session,
                        run_id,
                        last_sequence,
                        seen_event_ids,
                    )
                elif message.get("type") == "ping":
                    await websocket.send_json({"type": "pong", "latest_sequence": last_sequence})
            except TimeoutError:
                live_event = await subscription.get_message(timeout=0.5)
                if live_event is not None:
                    event_id = live_event.get("event_id")
                    sequence_number = int(live_event.get("sequence_number") or 0)
                    if not isinstance(event_id, str) or event_id not in seen_event_ids:
                        if sequence_number > last_sequence:
                            last_sequence = await send_run_events_after(
                                websocket,
                                session,
                                run_id,
                                last_sequence,
                                seen_event_ids,
                            )
                else:
                    await websocket.send_json(
                        {"type": "heartbeat", "latest_sequence": last_sequence}
                    )
            except WebSocketDisconnect:
                return


@router.post(
    "/projects/{project_id}/tools", response_model=ToolRead, status_code=status.HTTP_201_CREATED
)
async def create_tool(
    project_id: str,
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Tool:
    project = await user_project(session, user, project_id)
    tool = Tool(
        organization_id=project.organization_id,
        project_id=project.id,
        name=request.name,
        slug=request.slug or slugify(request.name),
        description=request.description,
    )
    session.add(tool)
    await commit(session)
    await session.refresh(tool)
    return tool


@router.get("/projects/{project_id}/tools", response_model=list[ToolRead])
async def list_tools(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Tool]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(Tool).where(Tool.project_id == project_id).order_by(Tool.created_at.desc()),
    )


@router.get("/tools/{tool_id}", response_model=ToolRead)
async def get_tool(
    tool_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Tool:
    return await user_tool(session, user, tool_id)


@router.post(
    "/tools/{tool_id}/versions",
    response_model=ToolVersionRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_tool_version(
    tool_id: str,
    request: ToolVersionCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> ToolVersion:
    tool = await user_tool(session, user, tool_id)
    validate_json_schema(request.input_schema, "input_schema")
    validate_json_schema(request.output_schema, "output_schema")
    version = ToolVersion(
        tool_id=tool.id,
        version_number=await next_tool_version_number(session, tool.id),
        kind=request.kind.value,
        input_schema=request.input_schema,
        output_schema=request.output_schema,
        execution_config=request.execution_config,
        timeout_seconds=request.timeout_seconds,
        retry_policy=request.retry_policy,
        side_effect_level=request.side_effect_level.value,
    )
    session.add(version)
    await commit(session)
    await session.refresh(version)
    return version


@router.post("/tool-versions/{version_id}/publish", response_model=ToolVersionRead)
async def publish_tool_version(
    version_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> ToolVersion:
    version = await user_tool_version(session, user, version_id)
    if version.status == VersionStatus.PUBLISHED.value:
        return version
    version.status = VersionStatus.PUBLISHED.value
    version.configuration_hash = tool_version_hash(version)
    version.published_at = datetime.now(UTC)
    tool = await session.scalar(select(Tool).where(Tool.id == version.tool_id))
    if tool is not None:
        tool.latest_version_number = max(tool.latest_version_number, version.version_number)
    await commit(session)
    await session.refresh(version)
    return version


@router.post(
    "/projects/{project_id}/workflows",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflow(
    project_id: str,
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Workflow:
    project = await user_project(session, user, project_id)
    workflow = Workflow(
        organization_id=project.organization_id,
        project_id=project.id,
        name=request.name,
        slug=request.slug or slugify(request.name),
        description=request.description,
        created_by=user.id,
    )
    session.add(workflow)
    await commit(session)
    await session.refresh(workflow)
    return workflow


@router.get("/projects/{project_id}/workflows", response_model=list[WorkflowRead])
async def list_workflows(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Workflow]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(Workflow)
        .where(Workflow.project_id == project_id)
        .order_by(Workflow.created_at.desc()),
    )


@router.get("/workflows/{workflow_id}", response_model=WorkflowRead)
async def get_workflow(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Workflow:
    return await user_workflow(session, user, workflow_id)


@router.post(
    "/workflows/{workflow_id}/versions",
    response_model=WorkflowVersionRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflow_version(
    workflow_id: str,
    request: WorkflowVersionCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> WorkflowVersion:
    workflow = await user_workflow(session, user, workflow_id)
    validate_definition(request.definition)
    version = WorkflowVersion(
        workflow_id=workflow.id,
        version_number=await next_workflow_version_number(session, workflow.id),
        definition=request.definition,
        created_by=user.id,
    )
    session.add(version)
    await commit(session)
    await session.refresh(version)
    return version


@router.get("/workflows/{workflow_id}/versions", response_model=list[WorkflowVersionRead])
async def list_workflow_versions(
    workflow_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[WorkflowVersion]:
    workflow = await user_workflow(session, user, workflow_id)
    return await scoped_list(
        session,
        select(WorkflowVersion)
        .where(WorkflowVersion.workflow_id == workflow.id)
        .order_by(WorkflowVersion.version_number.desc()),
    )


@router.post("/workflow-versions/{version_id}/validate", response_model=WorkflowVersionRead)
async def validate_workflow_version(
    version_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> WorkflowVersion:
    version = await user_workflow_version(session, user, version_id)
    workflow = await session.get(Workflow, version.workflow_id)
    if workflow is None:
        raise ApiError("WORKFLOW_NOT_FOUND", "Workflow not found.", 404)
    await validate_workflow_dependencies(session, workflow, version)
    return version


@router.post("/workflow-versions/{version_id}/publish", response_model=WorkflowVersionRead)
async def publish_workflow_version(
    version_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> WorkflowVersion:
    version = await user_workflow_version(session, user, version_id)
    if version.status == VersionStatus.PUBLISHED.value:
        return version
    workflow = await session.get(Workflow, version.workflow_id)
    if workflow is None:
        raise ApiError("WORKFLOW_NOT_FOUND", "Workflow not found.", 404)
    await validate_workflow_dependencies(session, workflow, version)
    version.status = VersionStatus.PUBLISHED.value
    version.definition_hash = workflow_version_hash(version)
    version.published_at = now_utc()
    workflow.latest_version_number = max(workflow.latest_version_number, version.version_number)
    await commit(session)
    await session.refresh(version)
    return version


@router.post(
    "/workflow-versions/{version_id}/runs",
    response_model=RunRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_workflow_run(
    version_id: str,
    request: RunCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Run:
    version = await user_workflow_version(session, user, version_id)
    if version.status != VersionStatus.PUBLISHED.value:
        raise ApiError("WORKFLOW_VERSION_NOT_PUBLISHED", "Workflow version must be published.", 422)
    workflow = await session.get(Workflow, version.workflow_id)
    if workflow is None:
        raise ApiError("WORKFLOW_NOT_FOUND", "Workflow not found.", 404)
    if request.idempotency_key:
        existing = await session.scalar(
            select(Run).where(
                Run.project_id == workflow.project_id,
                Run.idempotency_key == request.idempotency_key,
            )
        )
        if existing is not None:
            if get_settings().inline_run_execution and not request.defer:
                existing = await execute_run(session, existing.id, get_settings())
            return existing

    run_id = new_id()
    run = Run(
        id=run_id,
        organization_id=workflow.organization_id,
        project_id=workflow.project_id,
        agent_version_id=None,
        workflow_version_id=version.id,
        parent_run_id=None,
        root_run_id=run_id,
        temporal_workflow_id=f"local-run-{run_id}",
        temporal_run_id=None,
        idempotency_key=request.idempotency_key,
        status=RunStatus.QUEUED.value,
        input=request.input,
        output=None,
        created_by=user.id,
    )
    session.add(run)
    await session.flush()
    await append_run_event(
        session,
        run=run,
        event_type="run.queued",
        actor_type=RunEventActor.API,
        actor_id=user.id,
        payload={
            "workflow_version_id": version.id,
            "idempotency_key": request.idempotency_key,
            "deferred": request.defer,
        },
    )
    await commit(session)
    if get_settings().inline_run_execution and not request.defer:
        run = await execute_run(session, run.id, get_settings())
    return run


@router.get("/projects/{project_id}/approvals", response_model=list[ApprovalRead])
async def list_approvals(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[ApprovalRequest]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(ApprovalRequest)
        .where(ApprovalRequest.project_id == project_id)
        .order_by(ApprovalRequest.created_at.desc()),
    )


async def decide_approval(
    approval_id: str,
    approved: bool,
    request: ApprovalDecision,
    session: AsyncSession,
    user: User,
) -> ApprovalRequest:
    approval = await user_approval(session, user, approval_id)
    if approval.status != "pending":
        raise ApiError("APPROVAL_ALREADY_DECIDED", "Approval request is no longer pending.", 409)
    if approval.expires_at is not None and approval.expires_at <= now_utc():
        approval.status = "expired"
        approval.decision = {"approved": False, "comment": "Approval timed out."}
        approval.decided_at = now_utc()
        await commit(session)
        raise ApiError("APPROVAL_EXPIRED", "Approval request has expired.", 409)

    run = await session.get(Run, approval.run_id)
    step = await session.get(RunStep, approval.step_id)
    if run is None or step is None:
        raise ApiError("APPROVAL_NOT_FOUND", "Approval request is incomplete.", 404)
    approval.status = "approved" if approved else "rejected"
    approval.decision = {"approved": approved, "comment": request.comment}
    approval.decided_by = user.id
    approval.decided_at = now_utc()
    await append_run_event(
        session,
        run=run,
        step=step,
        event_type="approval.approved" if approved else "approval.rejected",
        actor_type=RunEventActor.USER,
        actor_id=user.id,
        payload={"approval_id": approval.id, "comment": request.comment},
    )
    run.status = RunStatus.QUEUED.value
    await append_run_event(
        session,
        run=run,
        event_type="run.queued",
        actor_type=RunEventActor.USER,
        actor_id=user.id,
        payload={"reason": "approval_decided", "approval_id": approval.id},
    )
    await commit(session)
    if get_settings().inline_run_execution:
        await execute_run(session, run.id, get_settings())
    await session.refresh(approval)
    return approval


@router.post("/approvals/{approval_id}/approve", response_model=ApprovalRead)
async def approve(
    approval_id: str,
    request: ApprovalDecision,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> ApprovalRequest:
    return await decide_approval(approval_id, True, request, session, user)


@router.post("/approvals/{approval_id}/reject", response_model=ApprovalRead)
async def reject(
    approval_id: str,
    request: ApprovalDecision,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> ApprovalRequest:
    return await decide_approval(approval_id, False, request, session, user)


@router.post(
    "/projects/{project_id}/datasets",
    response_model=DatasetRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    project_id: str,
    request: NamedCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Dataset:
    project = await user_project(session, user, project_id)
    dataset = Dataset(
        organization_id=project.organization_id,
        project_id=project.id,
        name=request.name,
        slug=request.slug or slugify(request.name),
        description=request.description,
        created_by=user.id,
    )
    session.add(dataset)
    await commit(session)
    await session.refresh(dataset)
    return dataset


@router.get("/projects/{project_id}/datasets", response_model=list[DatasetRead])
async def list_datasets(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[Dataset]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(Dataset).where(Dataset.project_id == project_id).order_by(Dataset.created_at.desc()),
    )


@router.post(
    "/datasets/{dataset_id}/cases",
    response_model=DatasetCaseRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset_case(
    dataset_id: str,
    request: DatasetCaseCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> DatasetCase:
    dataset = await user_dataset(session, user, dataset_id)
    case = DatasetCase(
        dataset_id=dataset.id,
        input=request.input,
        expected_output=request.expected_output,
        case_metadata=request.metadata,
    )
    session.add(case)
    await commit(session)
    await session.refresh(case)
    return case


@router.get("/datasets/{dataset_id}/cases", response_model=list[DatasetCaseRead])
async def list_dataset_cases(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[DatasetCase]:
    dataset = await user_dataset(session, user, dataset_id)
    return await scoped_list(
        session,
        select(DatasetCase)
        .where(DatasetCase.dataset_id == dataset.id)
        .order_by(DatasetCase.created_at.asc()),
    )


async def execute_experiment(
    session: AsyncSession, experiment: Experiment, settings: Any
) -> Experiment:
    cases = await scoped_list(
        session,
        select(DatasetCase)
        .where(DatasetCase.dataset_id == experiment.dataset_id)
        .order_by(DatasetCase.created_at.asc()),
    )
    agent_version = (
        await session.get(AgentVersion, experiment.agent_version_id)
        if experiment.agent_version_id is not None
        else None
    )
    experiment.status = "running"
    experiment.started_at = now_utc()
    await session.commit()

    scores: list[Decimal] = []
    completed = 0
    for case in cases:
        run_id = new_id()
        run = Run(
            id=run_id,
            organization_id=experiment.organization_id,
            project_id=experiment.project_id,
            agent_version_id=experiment.agent_version_id,
            workflow_version_id=experiment.workflow_version_id,
            parent_run_id=None,
            root_run_id=run_id,
            temporal_workflow_id=f"experiment-{experiment.id}-{run_id}",
            temporal_run_id=None,
            idempotency_key=None,
            status=RunStatus.QUEUED.value,
            input=case.input,
            output=None,
            created_by=experiment.created_by,
        )
        evaluation_run = ExperimentRun(
            experiment_id=experiment.id,
            dataset_case_id=case.id,
            run_id=run_id,
            status="running",
        )
        session.add_all([run, evaluation_run])
        await session.flush()
        await append_run_event(
            session,
            run=run,
            event_type="run.queued",
            actor_type=RunEventActor.EVALUATOR,
            payload={"experiment_id": experiment.id, "dataset_case_id": case.id},
        )
        await session.commit()
        run = await execute_run(session, run.id, settings)
        evaluation_run.status = run.status
        evaluation_run.completed_at = now_utc()
        if run.status == RunStatus.SUCCEEDED.value:
            completed += 1
            output = run.output or {}
            deterministic, deterministic_details = deterministic_score(output, case.expected_output)
            if agent_version is not None:
                model_score, model_details = await model_judge_score(
                    output, case.expected_output, agent_version, settings
                )
                score = (deterministic + model_score) / Decimal("2")
            else:
                model_details = {"type": "not_available_for_workflow"}
                score = deterministic
            scores.append(score)
            evaluation_run.score = score
            evaluation_run.result = {
                "deterministic": deterministic_details,
                "model_based": model_details,
            }
            await append_run_event(
                session,
                run=run,
                event_type="evaluation.completed",
                actor_type=RunEventActor.EVALUATOR,
                payload={"experiment_id": experiment.id, "score": str(score)},
            )
        else:
            evaluation_run.result = {"error": run.error_message, "status": run.status}
        await session.commit()

    experiment.status = "succeeded" if completed == len(cases) else "completed_with_failures"
    experiment.completed_at = now_utc()
    experiment.summary = {
        "cases": len(cases),
        "completed": completed,
        "success_rate": float(Decimal(completed) / Decimal(len(cases))) if cases else 0.0,
        "average_score": float(sum(scores, Decimal("0")) / Decimal(len(scores))) if scores else 0.0,
    }
    await session.commit()
    await session.refresh(experiment)
    return experiment


@router.post(
    "/projects/{project_id}/experiments",
    response_model=ExperimentRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_experiment(
    project_id: str,
    request: ExperimentCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Experiment:
    project = await user_project(session, user, project_id)
    dataset = await user_dataset(session, user, request.dataset_id)
    if dataset.project_id != project.id:
        raise ApiError("DATASET_NOT_FOUND", "Dataset not found.", 404)
    if bool(request.agent_version_id) == bool(request.workflow_version_id):
        raise ApiError(
            "INVALID_EXPERIMENT",
            "Experiment requires exactly one agent or workflow version.",
            422,
        )
    if request.agent_version_id:
        version = await user_agent_version(session, user, request.agent_version_id)
        agent = await session.get(Agent, version.agent_id)
        if (
            agent is None
            or agent.project_id != project.id
            or version.status != VersionStatus.PUBLISHED.value
        ):
            raise ApiError(
                "INVALID_EXPERIMENT", "Agent version must be published in this project.", 422
            )
    if request.workflow_version_id:
        workflow_version = await user_workflow_version(session, user, request.workflow_version_id)
        workflow = await session.get(Workflow, workflow_version.workflow_id)
        if (
            workflow is None
            or workflow.project_id != project.id
            or workflow_version.status != VersionStatus.PUBLISHED.value
        ):
            raise ApiError(
                "INVALID_EXPERIMENT", "Workflow version must be published in this project.", 422
            )
    experiment = Experiment(
        organization_id=project.organization_id,
        project_id=project.id,
        dataset_id=dataset.id,
        agent_version_id=request.agent_version_id,
        workflow_version_id=request.workflow_version_id,
        created_by=user.id,
    )
    session.add(experiment)
    await commit(session)
    return await execute_experiment(session, experiment, get_settings())


@router.get("/experiments/{experiment_id}", response_model=ExperimentRead)
async def get_experiment(
    experiment_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Experiment:
    statement = (
        select(Experiment)
        .join(Project, Project.id == Experiment.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(Experiment.id == experiment_id, OrganizationMembership.user_id == user.id)
    )
    experiment = await session.scalar(statement)
    if experiment is None:
        raise ApiError("EXPERIMENT_NOT_FOUND", "Experiment not found.", 404)
    return experiment


@router.get("/experiments/{experiment_id}/runs", response_model=list[ExperimentRunRead])
async def list_experiment_runs(
    experiment_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[ExperimentRun]:
    await get_experiment(experiment_id, session, user)
    return await scoped_list(
        session,
        select(ExperimentRun)
        .where(ExperimentRun.experiment_id == experiment_id)
        .order_by(ExperimentRun.created_at.asc()),
    )


def new_project_api_key() -> tuple[str, str, str]:
    prefix = secrets.token_hex(4)
    secret = secrets.token_urlsafe(32)
    value = f"aops_{prefix}_{secret}"
    return prefix, value, hashlib.sha256(value.encode()).hexdigest()


async def authenticated_ingestion_key(session: AsyncSession, value: str | None) -> ApiKey:
    if value is None:
        raise ApiError("UNAUTHENTICATED", "Project API key required.", 401)
    parts = value.split("_", 2)
    if len(parts) != 3 or parts[0] != "aops":
        raise ApiError("UNAUTHENTICATED", "Project API key is invalid.", 401)
    api_key = await session.scalar(select(ApiKey).where(ApiKey.prefix == parts[1]))
    supplied_hash = hashlib.sha256(value.encode()).hexdigest()
    if (
        api_key is None
        or api_key.revoked_at is not None
        or not hmac.compare_digest(api_key.secret_hash, supplied_hash)
    ):
        raise ApiError("UNAUTHENTICATED", "Project API key is invalid.", 401)
    return api_key


@router.post(
    "/projects/{project_id}/api-keys",
    response_model=ApiKeyRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_api_key(
    project_id: str,
    request: ApiKeyCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> ApiKeyRead:
    project = await user_project(session, user, project_id)
    prefix, secret, secret_hash = new_project_api_key()
    api_key = ApiKey(
        organization_id=project.organization_id,
        project_id=project.id,
        name=request.name,
        prefix=prefix,
        secret_hash=secret_hash,
        created_by=user.id,
    )
    session.add(api_key)
    await commit(session)
    await session.refresh(api_key)
    return ApiKeyRead.model_validate(api_key).model_copy(update={"secret": secret})


@router.get("/projects/{project_id}/api-keys", response_model=list[ApiKeyRead])
async def list_api_keys(
    project_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> list[ApiKey]:
    await user_project(session, user, project_id)
    return await scoped_list(
        session,
        select(ApiKey).where(ApiKey.project_id == project_id).order_by(ApiKey.created_at.desc()),
    )


@router.delete("/api-keys/{api_key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_api_key(
    api_key_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> Response:
    statement = (
        select(ApiKey)
        .join(Project, Project.id == ApiKey.project_id)
        .join(
            OrganizationMembership,
            OrganizationMembership.organization_id == Project.organization_id,
        )
        .where(ApiKey.id == api_key_id, OrganizationMembership.user_id == user.id)
    )
    api_key = await session.scalar(statement)
    if api_key is None:
        raise ApiError("API_KEY_NOT_FOUND", "API key not found.", 404)
    api_key.revoked_at = now_utc()
    await commit(session)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def external_run_for_event(
    session: AsyncSession, api_key: ApiKey, event: ExternalTraceEvent
) -> Run:
    run = await session.get(Run, event.run_id)
    if run is not None:
        if run.project_id != api_key.project_id:
            raise ApiError("RUN_NOT_FOUND", "Run not found.", 404)
        return run
    run = Run(
        id=event.run_id,
        organization_id=api_key.organization_id,
        project_id=api_key.project_id,
        agent_version_id=None,
        workflow_version_id=None,
        parent_run_id=None,
        root_run_id=event.run_id,
        temporal_workflow_id=f"external-{event.run_id}",
        temporal_run_id=None,
        idempotency_key=None,
        status=RunStatus.RUNNING.value,
        input={"external": True},
        output=None,
        created_by=None,
    )
    session.add(run)
    await session.flush()
    return run


@router.post("/ingestion/events:batch", response_model=TraceBatchRead)
async def ingest_trace_events(
    request: TraceBatchCreate,
    x_agentops_api_key: str | None = Header(default=None, alias="X-AgentOps-Api-Key"),
    session: AsyncSession = Depends(get_session),
) -> TraceBatchRead:
    api_key = await authenticated_ingestion_key(session, x_agentops_api_key)
    accepted = 0
    duplicates: list[str] = []
    for event in request.events:
        if await session.get(RunEvent, event.event_id) is not None:
            duplicates.append(event.event_id)
            continue
        run = await external_run_for_event(session, api_key, event)
        await append_run_event(
            session,
            run=run,
            event_id=event.event_id,
            event_type=event.event_type,
            actor_type=RunEventActor.SYSTEM,
            payload={"external_sequence_number": event.sequence_number, "data": event.payload},
            trace_id=event.trace_id,
            span_id=event.span_id,
            timestamp=event.timestamp,
        )
        if run.agent_version_id is None and run.workflow_version_id is None:
            if event.event_type.endswith(".completed"):
                run.status = RunStatus.SUCCEEDED.value
                run.completed_at = event.timestamp
            elif event.event_type.endswith(".failed"):
                run.status = RunStatus.FAILED.value
                run.completed_at = event.timestamp
        accepted += 1
    api_key.last_used_at = now_utc()
    await commit(session)
    return TraceBatchRead(accepted=accepted, duplicate_event_ids=duplicates)
