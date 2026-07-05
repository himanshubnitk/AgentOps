.PHONY: setup infra-up dev-api run-worker run-relay test lint migrate migration compose-down

setup:
	uv sync

infra-up:
	docker compose up -d postgres redis temporal temporal-ui

dev-api:
	uv run uvicorn agentops_api.main:app --reload

run-worker:
	uv run agentops-worker

run-relay:
	uv run agentops-relay

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy apps packages

migrate:
	uv run alembic upgrade head

migration:
	uv run alembic revision --autogenerate -m "$(name)"

compose-down:
	docker compose down
