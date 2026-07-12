from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from agentops_persistence.models import AgentVersion

from agentops_api.config import Settings
from agentops_api.providers import provider_for


def deterministic_score(
    output: dict[str, Any], expected: dict[str, Any]
) -> tuple[Decimal, dict[str, Any]]:
    rendered = json.dumps(output, sort_keys=True).lower()
    required = expected.get("must_include", [])
    if isinstance(required, list) and required:
        matches = [str(value).lower() for value in required if str(value).lower() in rendered]
        score = Decimal(len(matches)) / Decimal(len(required))
        return score, {"type": "keyword_coverage", "matched": matches, "required": required}
    score = Decimal("1") if output == expected else Decimal("0")
    return score, {"type": "exact_match", "expected": expected}


async def model_judge_score(
    output: dict[str, Any],
    expected: dict[str, Any],
    agent_version: AgentVersion,
    settings: Settings,
) -> tuple[Decimal, dict[str, Any]]:
    prompt = {
        "task": "Score the candidate output against the expected criteria from 0 to 1.",
        "expected": expected,
        "candidate": output,
        "response_format": {"score": "number from 0 to 1", "reason": "short string"},
    }
    result = await provider_for(agent_version, settings).invoke(
        agent_version=agent_version,
        run_input={"query": json.dumps(prompt, sort_keys=True)},
        tools=[],
        tool_results=[],
        step_index=1,
    )
    try:
        judged = json.loads(result.message)
        score = Decimal(str(judged["score"]))
        if not Decimal("0") <= score <= Decimal("1"):
            raise ValueError("score outside range")
        return score, {
            "type": "model_based",
            "provider": agent_version.model_provider,
            "model": agent_version.model_name,
            "reason": str(judged.get("reason") or ""),
        }
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        score, details = deterministic_score(output, expected)
        return score, {
            "type": "model_based_fallback",
            "provider": agent_version.model_provider,
            "model": agent_version.model_name,
            "fallback": details,
        }
