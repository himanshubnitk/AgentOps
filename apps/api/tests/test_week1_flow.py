from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from agentops_api.dependencies import get_session
from agentops_api.main import create_app
from agentops_persistence.models import Base
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async def override_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_session] = override_session
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        yield test_client

    await engine.dispose()


async def register_and_login(client: AsyncClient, email: str) -> str:
    password = "correct-horse"
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": email, "password": password},
    )
    assert response.status_code == 201, response.text

    response = await client.post(
        "/api/v1/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return str(response.json()["access_token"])


async def create_project(client: AsyncClient, token: str, suffix: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    response = await client.post(
        "/api/v1/organizations",
        json={"name": f"Org {suffix}", "slug": f"org-{suffix}"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    organization_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/organizations/{organization_id}/projects",
        json={"name": f"Project {suffix}", "slug": f"project-{suffix}"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


async def create_published_mock_search_tool(
    client: AsyncClient, headers: dict[str, str], project_id: str
) -> str:
    response = await client.post(
        f"/api/v1/projects/{project_id}/tools",
        json={"name": "Mock Search", "slug": "mock-search"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    tool_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/tools/{tool_id}/versions",
        json={
            "kind": "internal",
            "input_schema": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": False,
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "results": {"type": "array"},
                    "total": {"type": "integer"},
                },
                "required": ["query", "results", "total"],
                "additionalProperties": False,
            },
            "execution_config": {"name": "mock_search"},
            "side_effect_level": "read_only",
            "retry_policy": {"max_attempts": 2, "backoff_seconds": 0.01},
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    tool_version_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/tool-versions/{tool_version_id}/publish",
        headers=headers,
    )
    assert response.status_code == 200, response.text
    return str(tool_version_id)


async def create_published_research_agent(
    client: AsyncClient, headers: dict[str, str], project_id: str, tool_version_id: str
) -> str:
    response = await client.post(
        f"/api/v1/projects/{project_id}/agents",
        json={"name": "Research Agent", "slug": "research-agent"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    agent_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/agents/{agent_id}/versions",
        json={
            "instructions": "Research with citations and return structured output.",
            "model_provider": "mock",
            "model_name": "scripted",
            "tool_version_ids": [tool_version_id],
            "max_steps": 4,
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    agent_version_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/agent-versions/{agent_version_id}/publish",
        headers=headers,
    )
    assert response.status_code == 200, response.text
    return str(agent_version_id)


@pytest.mark.asyncio
async def test_week1_swagger_flow_and_tenant_isolation(client: AsyncClient) -> None:
    owner_token = await register_and_login(client, "owner@example.com")
    outsider_token = await register_and_login(client, "outsider@example.com")
    owner_headers = {"Authorization": f"Bearer {owner_token}"}
    outsider_headers = {"Authorization": f"Bearer {outsider_token}"}

    owner_project = await create_project(client, owner_token, "owner")
    await create_project(client, outsider_token, "outsider")

    response = await client.post(
        f"/api/v1/projects/{owner_project['id']}/tools",
        json={"name": "Web Search", "slug": "web-search"},
        headers=owner_headers,
    )
    assert response.status_code == 201, response.text
    tool_id = response.json()["id"]

    schema = {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
        "additionalProperties": False,
    }
    response = await client.post(
        f"/api/v1/tools/{tool_id}/versions",
        json={
            "kind": "http",
            "input_schema": schema,
            "output_schema": {"type": "object"},
            "execution_config": {"method": "GET", "url_template": "https://example.com/search"},
            "side_effect_level": "read_only",
        },
        headers=owner_headers,
    )
    assert response.status_code == 201, response.text
    tool_version_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/tool-versions/{tool_version_id}/publish",
        headers=owner_headers,
    )
    assert response.status_code == 200, response.text
    published_tool = response.json()
    assert published_tool["status"] == "published"
    assert published_tool["configuration_hash"]

    response = await client.post(
        f"/api/v1/projects/{owner_project['id']}/agents",
        json={"name": "Research Agent", "slug": "research-agent"},
        headers=owner_headers,
    )
    assert response.status_code == 201, response.text
    agent_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/agents/{agent_id}/versions",
        json={
            "instructions": "Research with citations.",
            "model_provider": "mock",
            "model_name": "scripted",
            "tool_version_ids": [tool_version_id],
        },
        headers=owner_headers,
    )
    assert response.status_code == 201, response.text
    agent_version_id = response.json()["id"]

    response = await client.post(
        f"/api/v1/agent-versions/{agent_version_id}/publish",
        headers=owner_headers,
    )
    assert response.status_code == 200, response.text
    published_agent = response.json()
    assert published_agent["status"] == "published"
    assert published_agent["configuration_hash"]

    response = await client.get(f"/api/v1/agents/{agent_id}", headers=outsider_headers)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "AGENT_NOT_FOUND"

    response = await client.get("/health/live")
    assert response.status_code == 200
    assert response.headers["x-request-id"]


@pytest.mark.asyncio
async def test_week2_single_agent_run_events_trace_cancel_and_tenancy(
    client: AsyncClient,
) -> None:
    owner_token = await register_and_login(client, "week2-owner@example.com")
    outsider_token = await register_and_login(client, "week2-outsider@example.com")
    owner_headers = {"Authorization": f"Bearer {owner_token}"}
    outsider_headers = {"Authorization": f"Bearer {outsider_token}"}

    owner_project = await create_project(client, owner_token, "week2-owner")
    await create_project(client, outsider_token, "week2-outsider")
    tool_version_id = await create_published_mock_search_tool(
        client, owner_headers, owner_project["id"]
    )
    agent_version_id = await create_published_research_agent(
        client, owner_headers, owner_project["id"], tool_version_id
    )

    response = await client.post(
        f"/api/v1/agent-versions/{agent_version_id}/runs",
        json={
            "input": {"query": "durable agent runs"},
            "idempotency_key": "research-run-1",
        },
        headers=owner_headers,
    )
    assert response.status_code == 202, response.text
    run = response.json()
    assert run["status"] == "succeeded"
    assert "durable agent runs" in run["output"]["answer"]
    run_id = run["id"]

    response = await client.get(f"/api/v1/runs/{run_id}/events", headers=owner_headers)
    assert response.status_code == 200, response.text
    events = response.json()
    assert [event["sequence_number"] for event in events] == list(range(1, len(events) + 1))
    event_types = [event["event_type"] for event in events]
    assert event_types[0] == "run.queued"
    assert "model.completed" in event_types
    assert "tool.completed" in event_types
    assert event_types[-1] == "run.succeeded"

    response = await client.get(f"/api/v1/runs/{run_id}/trace", headers=owner_headers)
    assert response.status_code == 200, response.text
    trace = response.json()
    assert trace["run"]["id"] == run_id
    assert {step["step_type"] for step in trace["steps"]} == {"model", "tool"}

    response = await client.get(f"/api/v1/runs/{run_id}", headers=outsider_headers)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "RUN_NOT_FOUND"

    response = await client.post(
        f"/api/v1/agent-versions/{agent_version_id}/runs",
        json={
            "input": {"query": "resumed run"},
            "idempotency_key": "research-run-resumed",
            "defer": True,
        },
        headers=owner_headers,
    )
    assert response.status_code == 202, response.text
    deferred_run = response.json()
    assert deferred_run["status"] == "queued"

    response = await client.post(
        f"/api/v1/runs/{deferred_run['id']}/resume",
        headers=owner_headers,
    )
    assert response.status_code == 200, response.text
    resumed_run = response.json()
    assert resumed_run["status"] == "succeeded"
    assert "resumed run" in resumed_run["output"]["answer"]

    response = await client.post(
        f"/api/v1/agent-versions/{agent_version_id}/runs",
        json={
            "input": {"query": "cancelled run"},
            "idempotency_key": "research-run-cancelled",
            "defer": True,
        },
        headers=owner_headers,
    )
    assert response.status_code == 202, response.text
    queued_run = response.json()
    assert queued_run["status"] == "queued"

    response = await client.post(
        f"/api/v1/runs/{queued_run['id']}/cancel",
        headers=owner_headers,
    )
    assert response.status_code == 200, response.text
    cancelled = response.json()
    assert cancelled["status"] == "cancelled"
    assert cancelled["error_code"] == "CANCELLED"

    response = await client.get(
        f"/api/v1/runs/{queued_run['id']}/events",
        headers=owner_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()[-1]["event_type"] == "run.cancelled"
