# AgentOps

AgentOps is a FastAPI backend for managing AI agents, their tools, versioned
configuration, and durable single-agent runs. It is built as a small control
plane with traceable execution state, so runs can be inspected through persisted
steps and ordered events instead of ad hoc logs.

## Features

- User registration, login, refresh tokens, and authenticated profile lookup.
- Organization, project, agent, and tool management.
- Draft and published versions for agents and tools.
- Configuration hashing for immutable published versions.
- Durable run records with project-scoped idempotency keys.
- Model and tool execution loop with step limits and typed failures.
- Internal tools for local demos plus HTTP tool execution with validation.
- Run status, step, event, and trace APIs for debugging.
- Local worker entry point for resuming queued or running work.

## Project Structure

```text
apps/api/src/agentops_api/        FastAPI app, routes, execution, providers
apps/api/tests/                   API and flow tests
packages/domain/src/              Shared enums and hashing helpers
packages/persistence/src/         SQLAlchemy session and models
packages/persistence/migrations/  Alembic migrations
alembic.ini                       Migration configuration
docker-compose.yml                Local Postgres, Redis, and Temporal stack
Makefile                          Common development commands
pyproject.toml                    Python package and tool configuration
```

## Quick Start

```bash
uv sync
cp .env.example .env
docker compose up -d postgres redis temporal temporal-ui
uv run alembic upgrade head
uv run uvicorn agentops_api.main:app --reload
```

Swagger is available at http://127.0.0.1:8000/api/v1/docs.

Temporal UI is available at http://127.0.0.1:8233. The executable run path uses
the database-backed local worker for now; the schema already stores workflow IDs
so a Temporal adapter can be added behind the same API later.

## Worker Mode

By default, `AGENTOPS_INLINE_RUN_EXECUTION=true` executes a run inside the API
request after the run is persisted. To test deferred execution:

```bash
AGENTOPS_INLINE_RUN_EXECUTION=false uv run uvicorn agentops_api.main:app --reload
uv run agentops-worker
```

or:

```bash
make run-worker
```

## Demo Flow

1. Register and log in.
2. Create an organization and a project.
3. Create an internal `mock_search` tool version and publish it.
4. Create a `mock` provider agent version using that tool and publish it.
5. Start a run:

```json
{
  "input": {"query": "durable agent runs"},
  "idempotency_key": "research-run-1"
}
```

6. Inspect `GET /api/v1/runs/{run_id}/events`.
7. Inspect `GET /api/v1/runs/{run_id}/trace`.
8. Start another run with `"defer": true`, then resume it with
   `agentops-worker` or `POST /api/v1/runs/{run_id}/resume`.

## Commands

```bash
make setup
make infra-up
make migrate
make dev-api
make run-worker
make test
make lint
```

The direct checks are:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy apps packages
```
