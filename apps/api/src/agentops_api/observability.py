from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from agentops_persistence.models import AgentVersion, Run, RunStep, UsageRecord, new_id

from agentops_api.providers import Usage

SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "cookie",
    "credentials",
    "credential",
    "idempotency_key",
    "password",
    "refresh_token",
    "secret",
    "token",
}


def redact_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if key.lower() in SENSITIVE_KEYS else redact_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_payload(item) for item in value]
    if isinstance(value, str) and len(value) > 2000:
        return f"{value[:2000]}...[truncated]"
    return value


def duration_ms(started_at: datetime | None, completed_at: datetime | None) -> int | None:
    if started_at is None or completed_at is None:
        return None
    return max(0, int((completed_at - started_at).total_seconds() * 1000))


def usage_record_for(
    *,
    run: Run,
    step: RunStep,
    agent_version: AgentVersion,
    usage: Usage,
) -> UsageRecord:
    pricing = agent_version.model_parameters.get("pricing", {})
    pricing = pricing if isinstance(pricing, dict) else {}
    input_price = decimal_from(pricing.get("input_unit_price"))
    output_price = decimal_from(pricing.get("output_unit_price"))
    cost = (Decimal(usage.input_tokens) * input_price) + (
        Decimal(usage.output_tokens) * output_price
    )
    return UsageRecord(
        id=new_id(),
        run_id=run.id,
        step_id=step.id,
        provider=agent_version.model_provider,
        model=agent_version.model_name,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        reasoning_tokens=usage.reasoning_tokens,
        input_unit_price=input_price,
        output_unit_price=output_price,
        currency=str(pricing.get("currency") or "USD")[:3].upper(),
        calculated_cost=cost,
        pricing_version=str(pricing.get("version") or "unpriced"),
    )


def decimal_from(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:  # noqa: BLE001
        return Decimal("0")
