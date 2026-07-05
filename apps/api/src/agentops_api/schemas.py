from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from agentops_domain.enums import ToolKind, ToolSideEffectLevel
from pydantic import BaseModel, ConfigDict, Field, field_validator

from agentops_api.observability import redact_payload


class UserCreate(BaseModel):
    email: str
    password: str = Field(min_length=8)
    display_name: str | None = None


class LoginRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    email: str
    display_name: str | None


class NamedCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    slug: str | None = Field(default=None, max_length=180)
    description: str | None = None


class OrganizationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    slug: str
    created_at: datetime


class ProjectRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    organization_id: str
    name: str
    slug: str
    description: str | None
    created_at: datetime


class AgentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    organization_id: str
    project_id: str
    name: str
    slug: str
    description: str | None
    latest_version_number: int
    created_at: datetime


class AgentVersionCreate(BaseModel):
    instructions: str = Field(min_length=1)
    model_provider: str = Field(min_length=1, max_length=80)
    model_name: str = Field(min_length=1, max_length=160)
    model_parameters: dict[str, Any] = Field(default_factory=dict)
    tool_version_ids: list[str] = Field(default_factory=list)
    memory_config: dict[str, Any] = Field(default_factory=dict)
    guardrail_config: dict[str, Any] = Field(default_factory=dict)
    max_steps: int = Field(default=12, ge=1, le=100)


class AgentVersionRead(AgentVersionCreate):
    model_config = ConfigDict(from_attributes=True)

    id: str
    agent_id: str
    version_number: int
    status: str
    configuration_hash: str | None
    created_at: datetime
    published_at: datetime | None


class ToolRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    organization_id: str
    project_id: str
    name: str
    slug: str
    description: str | None
    latest_version_number: int
    created_at: datetime


class ToolVersionCreate(BaseModel):
    kind: ToolKind
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    execution_config: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=15, ge=1, le=300)
    retry_policy: dict[str, Any] = Field(default_factory=dict)
    side_effect_level: ToolSideEffectLevel = ToolSideEffectLevel.READ_ONLY


class ToolVersionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    tool_id: str
    version_number: int
    kind: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    execution_config: dict[str, Any]
    timeout_seconds: int
    retry_policy: dict[str, Any]
    side_effect_level: str
    status: str
    configuration_hash: str | None
    created_at: datetime
    published_at: datetime | None


class RunCreate(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=200)
    defer: bool = False


class RunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    organization_id: str
    project_id: str
    agent_version_id: str | None
    workflow_version_id: str | None
    parent_run_id: str | None
    root_run_id: str
    temporal_workflow_id: str
    temporal_run_id: str | None
    idempotency_key: str | None
    status: str
    input: dict[str, Any]
    output: dict[str, Any] | None
    error_code: str | None
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_by: str | None
    created_at: datetime
    updated_at: datetime

    @field_validator("input", "output", mode="before")
    @classmethod
    def redact_json(cls, value: Any) -> Any:
        return redact_payload(value)


class RunStepRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    run_id: str
    node_key: str
    parent_step_id: str | None
    step_type: str
    name: str
    status: str
    attempt: int
    input: dict[str, Any]
    output: dict[str, Any] | None
    error: dict[str, Any] | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime

    @field_validator("input", "output", "error", mode="before")
    @classmethod
    def redact_json(cls, value: Any) -> Any:
        return redact_payload(value)


class RunEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    run_id: str
    step_id: str | None
    sequence_number: int
    event_type: str
    timestamp: datetime
    actor_type: str
    actor_id: str | None
    payload: dict[str, Any]
    trace_id: str | None
    span_id: str | None
    created_at: datetime

    @field_validator("payload", mode="before")
    @classmethod
    def redact_payload_field(cls, value: Any) -> Any:
        return redact_payload(value)


class UsageRecordRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    run_id: str
    step_id: str | None
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    reasoning_tokens: int
    input_unit_price: Decimal
    output_unit_price: Decimal
    currency: str
    calculated_cost: Decimal
    pricing_version: str
    created_at: datetime


class RunCostRead(BaseModel):
    run_id: str
    currency: str
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    reasoning_tokens: int
    total_tokens: int
    total_cost: Decimal
    records: list[UsageRecordRead]


class TraceTreeNode(BaseModel):
    id: str
    parent_step_id: str | None
    node_key: str
    step_type: str
    name: str
    status: str
    attempt: int
    started_at: datetime | None
    completed_at: datetime | None
    duration_ms: int | None
    input: dict[str, Any]
    output: dict[str, Any] | None
    error: dict[str, Any] | None
    events: list[RunEventRead]
    usage: list[UsageRecordRead]
    children: list[TraceTreeNode] = Field(default_factory=list)

    @field_validator("input", "output", "error", mode="before")
    @classmethod
    def redact_json(cls, value: Any) -> Any:
        return redact_payload(value)


class RunTraceRead(BaseModel):
    run: RunRead
    steps: list[RunStepRead]
    events: list[RunEventRead]
    tree: list[TraceTreeNode] = Field(default_factory=list)
    usage: list[UsageRecordRead] = Field(default_factory=list)
    cost: RunCostRead | None = None


class WebSocketTicketRead(BaseModel):
    ticket: str
    expires_in_seconds: int
