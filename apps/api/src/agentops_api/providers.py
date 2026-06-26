from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx
from agentops_persistence.models import AgentVersion

from agentops_api.config import Settings
from agentops_api.errors import ApiError


@dataclass(frozen=True)
class ToolSpec:
    version_id: str
    name: str
    slug: str
    description: str | None
    input_schema: dict[str, Any]
    side_effect_level: str


@dataclass(frozen=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cached_input_tokens": self.cached_input_tokens,
            "reasoning_tokens": self.reasoning_tokens,
        }


@dataclass(frozen=True)
class ToolCall:
    tool_version_id: str
    name: str
    arguments: dict[str, Any]
    call_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "tool_version_id": self.tool_version_id,
            "name": self.name,
            "arguments": self.arguments,
            "call_id": self.call_id,
        }


@dataclass(frozen=True)
class ModelResult:
    message: str
    final_output: dict[str, Any] | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    raw_response: dict[str, Any] = field(default_factory=dict)


class ModelProvider(Protocol):
    async def invoke(
        self,
        *,
        agent_version: AgentVersion,
        run_input: dict[str, Any],
        tools: list[ToolSpec],
        tool_results: list[dict[str, Any]],
        step_index: int,
    ) -> ModelResult:
        """Invoke the model provider and return normalized output."""


class MockModelProvider:
    async def invoke(
        self,
        *,
        agent_version: AgentVersion,
        run_input: dict[str, Any],
        tools: list[ToolSpec],
        tool_results: list[dict[str, Any]],
        step_index: int,
    ) -> ModelResult:
        if tools and not tool_results:
            query = str(run_input.get("query") or run_input.get("topic") or run_input)
            tool = tools[0]
            return ModelResult(
                message=f"Calling {tool.name} for research context.",
                tool_calls=[
                    ToolCall(
                        tool_version_id=tool.version_id,
                        name=tool.name,
                        arguments={"query": query},
                        call_id=f"mock-tool-{step_index}",
                    )
                ],
                usage=Usage(input_tokens=24, output_tokens=12),
                raw_response={"provider": "mock", "mode": "tool_call"},
            )

        answer = build_mock_answer(run_input, tool_results)
        return ModelResult(
            message=answer,
            final_output={
                "answer": answer,
                "tool_results": tool_results,
                "model": agent_version.model_name,
            },
            usage=Usage(input_tokens=32, output_tokens=max(8, len(answer.split()))),
            raw_response={"provider": "mock", "mode": "final"},
        )


class OpenAICompatibleProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def invoke(
        self,
        *,
        agent_version: AgentVersion,
        run_input: dict[str, Any],
        tools: list[ToolSpec],
        tool_results: list[dict[str, Any]],
        step_index: int,
    ) -> ModelResult:
        parameters = agent_version.model_parameters
        api_key = str(parameters.get("api_key") or self.settings.openai_api_key or "")
        if not api_key:
            raise ApiError(
                "PROVIDER_CONFIGURATION_ERROR",
                "OpenAI-compatible provider requires an API key.",
                422,
            )

        base_url = str(parameters.get("base_url") or "https://api.openai.com/v1").rstrip("/")
        request_payload = {
            "model": agent_version.model_name,
            "messages": build_messages(agent_version.instructions, run_input, tool_results),
            "temperature": parameters.get("temperature", 0),
        }
        if tools:
            request_payload["tools"] = [openai_tool_schema(tool) for tool in tools]
            request_payload["tool_choice"] = "auto"

        timeout = float(parameters.get("timeout_seconds") or self.settings.provider_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json=request_payload,
            )

        if response.status_code >= 400:
            raise ApiError(
                "PROVIDER_REQUEST_FAILED",
                "Model provider returned an error.",
                502,
                {"status_code": response.status_code, "body": response.text[:500]},
            )

        data = response.json()
        message = extract_choice_message(data)
        usage = normalize_usage(data.get("usage", {}))
        tool_calls = parse_tool_calls(message, tools, step_index)
        content = str(message.get("content") or "")
        if tool_calls:
            return ModelResult(
                message=content or "Provider requested tool execution.",
                tool_calls=tool_calls,
                usage=usage,
                raw_response=data,
            )
        return ModelResult(
            message=content,
            final_output={"answer": content, "tool_results": tool_results},
            usage=usage,
            raw_response=data,
        )


def provider_for(agent_version: AgentVersion, settings: Settings) -> ModelProvider:
    provider_name = agent_version.model_provider.lower().strip()
    if provider_name == "mock":
        return MockModelProvider()
    if provider_name in {"openai", "openai-compatible"}:
        return OpenAICompatibleProvider(settings)
    raise ApiError(
        "UNSUPPORTED_PROVIDER",
        "Unsupported model provider.",
        422,
        {"model_provider": agent_version.model_provider},
    )


def build_mock_answer(run_input: dict[str, Any], tool_results: list[dict[str, Any]]) -> str:
    query = str(run_input.get("query") or run_input.get("topic") or "the request")
    if not tool_results:
        return f"Mock research answer for {query}."
    first_output = tool_results[0].get("output", {})
    results = first_output.get("results") if isinstance(first_output, dict) else None
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            snippet = first.get("snippet") or first.get("title") or "mock evidence"
            return f"Mock research answer for {query}: {snippet}"
    return f"Mock research answer for {query} using {len(tool_results)} tool result(s)."


def build_messages(
    instructions: str,
    run_input: dict[str, Any],
    tool_results: list[dict[str, Any]],
) -> list[dict[str, str]]:
    content = json.dumps({"input": run_input, "tool_results": tool_results}, sort_keys=True)
    return [
        {"role": "system", "content": instructions},
        {"role": "user", "content": content},
    ]


def normalize_usage(raw_usage: Any) -> Usage:
    usage = raw_usage if isinstance(raw_usage, dict) else {}
    return Usage(
        input_tokens=int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0),
        output_tokens=int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
        cached_input_tokens=int(usage.get("cached_input_tokens") or 0),
        reasoning_tokens=int(usage.get("reasoning_tokens") or 0),
    )


def openai_tool_schema(tool: ToolSpec) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": normalize_tool_name(tool),
            "description": tool.description or tool.name,
            "parameters": tool.input_schema,
        },
    }


def normalize_tool_name(tool: ToolSpec) -> str:
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", tool.slug).strip("_")
    return value or f"tool_{tool.version_id.replace('-', '_')}"


def extract_choice_message(data: dict[str, Any]) -> dict[str, Any]:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ApiError(
            "PROVIDER_RESPONSE_INVALID", "Provider response did not contain choices.", 502
        )
    first = choices[0]
    if not isinstance(first, dict) or not isinstance(first.get("message"), dict):
        raise ApiError("PROVIDER_RESPONSE_INVALID", "Provider response message was invalid.", 502)
    return dict(first["message"])


def parse_tool_calls(
    message: dict[str, Any], tools: list[ToolSpec], step_index: int
) -> list[ToolCall]:
    raw_calls = message.get("tool_calls")
    if not isinstance(raw_calls, list):
        return []

    tool_by_name = {normalize_tool_name(tool): tool for tool in tools}
    calls: list[ToolCall] = []
    for index, raw_call in enumerate(raw_calls, start=1):
        if not isinstance(raw_call, dict):
            continue
        function = raw_call.get("function")
        if not isinstance(function, dict):
            continue
        function_name = str(function.get("name") or "")
        tool = tool_by_name.get(function_name)
        if tool is None:
            raise ApiError(
                "PROVIDER_TOOL_NOT_ALLOWED",
                "Provider requested a tool that is not attached to this agent version.",
                422,
                {"tool_name": function_name},
            )
        arguments = parse_arguments(function.get("arguments"))
        calls.append(
            ToolCall(
                tool_version_id=tool.version_id,
                name=tool.name,
                arguments=arguments,
                call_id=str(raw_call.get("id") or f"tool-{step_index}-{index}"),
            )
        )
    return calls


def parse_arguments(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return {}
