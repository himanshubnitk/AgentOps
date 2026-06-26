from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

from agentops_domain.enums import MembershipRole, RunEventActor, RunStatus, VersionStatus
from agentops_domain.hashing import canonical_hash
from agentops_persistence.models import (
    Agent,
    AgentVersion,
    Organization,
    OrganizationMembership,
    Project,
    Run,
    RunEvent,
    RunStep,
    Tool,
    ToolVersion,
    User,
    new_id,
    now_utc,
)
from fastapi import APIRouter, Depends, Response, status
from jsonschema import Draft202012Validator, SchemaError
from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from agentops_api.config import get_settings
from agentops_api.dependencies import get_current_user, get_session
from agentops_api.errors import ApiError
from agentops_api.execution import append_run_event, cancel_run, execute_run
from agentops_api.schemas import (
    AgentRead,
    AgentVersionCreate,
    AgentVersionRead,
    LoginRequest,
    NamedCreate,
    OrganizationRead,
    ProjectRead,
    RefreshRequest,
    RunCreate,
    RunEventRead,
    RunRead,
    RunStepRead,
    RunTraceRead,
    TokenPair,
    ToolRead,
    ToolVersionCreate,
    ToolVersionRead,
    UserCreate,
    UserRead,
)
from agentops_api.security import create_token, decode_token, hash_password, verify_password

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


async def scoped_list(session: AsyncSession, statement: Select[tuple[Any]]) -> list[Any]:
    result = await session.scalars(statement)
    return list(result.all())


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
    return RunTraceRead(run=RunRead.model_validate(run), steps=steps, events=events)


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
