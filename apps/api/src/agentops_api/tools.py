from __future__ import annotations

import asyncio
import ipaddress
import json
from dataclasses import dataclass
from string import Formatter
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx
from agentops_domain.enums import ToolKind, ToolSideEffectLevel
from agentops_persistence.models import ToolVersion
from jsonschema import Draft202012Validator, ValidationError

from agentops_api.errors import ApiError


@dataclass(frozen=True)
class ToolBinding:
    version_id: str
    tool_id: str
    name: str
    slug: str
    description: str | None
    kind: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    execution_config: dict[str, Any]
    timeout_seconds: int
    retry_policy: dict[str, Any]
    side_effect_level: str

    @classmethod
    def from_version(
        cls,
        *,
        version: ToolVersion,
        name: str,
        slug: str,
        description: str | None,
    ) -> ToolBinding:
        return cls(
            version_id=version.id,
            tool_id=version.tool_id,
            name=name,
            slug=slug,
            description=description,
            kind=version.kind,
            input_schema=version.input_schema,
            output_schema=version.output_schema,
            execution_config=version.execution_config,
            timeout_seconds=version.timeout_seconds,
            retry_policy=version.retry_policy,
            side_effect_level=version.side_effect_level,
        )


class ToolExecutor:
    async def execute(self, binding: ToolBinding, arguments: dict[str, Any]) -> dict[str, Any]:
        validate_payload(binding.input_schema, arguments, "tool input")
        output = await self._execute_with_retries(binding, arguments)
        validate_payload(binding.output_schema, output, "tool output")
        return output

    async def _execute_with_retries(
        self, binding: ToolBinding, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        attempts = retry_attempts(binding)
        last_error: ApiError | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await self._execute_once(binding, arguments)
            except ApiError as exc:
                last_error = exc
                if attempt == attempts:
                    raise
                await asyncio.sleep(retry_backoff_seconds(binding, attempt))
        if last_error is not None:
            raise last_error
        raise ApiError("TOOL_EXECUTION_ERROR", "Tool execution failed.", 502)

    async def _execute_once(
        self, binding: ToolBinding, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        if binding.kind == ToolKind.INTERNAL.value:
            return execute_internal_tool(binding, arguments)
        if binding.kind == ToolKind.HTTP.value:
            return await execute_http_tool(binding, arguments)
        raise ApiError(
            "TOOL_KIND_UNSUPPORTED",
            "Tool kind is not executable in the single-agent runtime.",
            422,
            {"kind": binding.kind},
        )


def validate_payload(schema: dict[str, Any], payload: dict[str, Any], label: str) -> None:
    try:
        Draft202012Validator(schema).validate(payload)
    except ValidationError as exc:
        raise ApiError(
            "TOOL_VALIDATION_ERROR",
            f"{label} failed JSON Schema validation.",
            422,
            {"reason": exc.message, "path": list(exc.path)},
        ) from exc


def retry_attempts(binding: ToolBinding) -> int:
    if binding.side_effect_level == ToolSideEffectLevel.NON_IDEMPOTENT_WRITE.value:
        return 1
    configured = binding.retry_policy.get("max_attempts", 1)
    try:
        attempts = int(configured)
    except (TypeError, ValueError):
        attempts = 1
    return max(1, min(attempts, 5))


def retry_backoff_seconds(binding: ToolBinding, attempt: int) -> float:
    configured = binding.retry_policy.get("backoff_seconds", 0.1)
    try:
        base = float(configured)
    except (TypeError, ValueError):
        base = 0.1
    return min(base * attempt, 5.0)


def execute_internal_tool(binding: ToolBinding, arguments: dict[str, Any]) -> dict[str, Any]:
    tool_name = str(binding.execution_config.get("name") or binding.slug).replace("-", "_")
    if tool_name == "mock_search":
        query = str(arguments.get("query") or arguments.get("q") or "")
        return {
            "query": query,
            "results": [
                {
                    "title": f"Mock result for {query}",
                    "url": "https://example.com/mock-search",
                    "snippet": f"Deterministic evidence about {query}.",
                }
            ],
            "total": 1,
        }
    if tool_name == "echo":
        return {"echo": arguments}
    if tool_name == "json_transform":
        return {"result": arguments}
    raise ApiError(
        "INTERNAL_TOOL_NOT_FOUND",
        "Internal tool is not registered in the local executor.",
        422,
        {"tool": tool_name},
    )


async def execute_http_tool(binding: ToolBinding, arguments: dict[str, Any]) -> dict[str, Any]:
    method = str(binding.execution_config.get("method") or "GET").upper()
    if method not in {"GET", "POST"}:
        raise ApiError(
            "HTTP_TOOL_METHOD_UNSUPPORTED",
            "HTTP tools currently support GET and POST.",
            422,
            {"method": method},
        )

    url_template = binding.execution_config.get("url_template")
    if not isinstance(url_template, str) or not url_template:
        raise ApiError("HTTP_TOOL_CONFIG_INVALID", "HTTP tool requires a url_template.", 422)

    url = render_url_template(url_template, arguments)
    validate_http_target(url, binding.execution_config)
    timeout = httpx.Timeout(float(binding.timeout_seconds))
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        if method == "GET":
            response = await client.get(url, params=arguments)
        else:
            response = await client.post(url, json=arguments)

    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        body = response.json()
    else:
        body = {"body": response.text[:2000]}

    payload = body if isinstance(body, dict) else {"data": body}
    payload["status_code"] = response.status_code
    if response.status_code >= 400:
        raise ApiError(
            "HTTP_TOOL_REQUEST_FAILED",
            "HTTP tool returned an error response.",
            502,
            {"status_code": response.status_code, "body": payload},
        )
    return payload


def render_url_template(template: str, arguments: dict[str, Any]) -> str:
    values = {
        name: quote_plus(str(arguments.get(name, "")))
        for _, name, _, _ in Formatter().parse(template)
        if name
    }
    try:
        return template.format(**values)
    except KeyError as exc:
        raise ApiError(
            "HTTP_TOOL_CONFIG_INVALID",
            "HTTP tool url_template referenced a missing argument.",
            422,
            {"argument": str(exc)},
        ) from exc


def validate_http_target(url: str, execution_config: dict[str, Any]) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ApiError("HTTP_TOOL_TARGET_BLOCKED", "HTTP tool URL must be http or https.", 422)

    hostname = parsed.hostname.lower()
    allowed_hosts = execution_config.get("allowed_hosts")
    if isinstance(allowed_hosts, list) and allowed_hosts:
        normalized_hosts = {str(host).lower() for host in allowed_hosts}
        if hostname not in normalized_hosts:
            raise ApiError(
                "HTTP_TOOL_TARGET_BLOCKED",
                "HTTP tool target is not in allowed_hosts.",
                422,
                {"hostname": hostname},
            )

    if hostname in {"localhost", "localhost.localdomain"} or hostname.endswith(".localhost"):
        raise ApiError("HTTP_TOOL_TARGET_BLOCKED", "HTTP tool cannot target localhost.", 422)

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return

    if (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_unspecified
        or address.is_multicast
    ):
        raise ApiError(
            "HTTP_TOOL_TARGET_BLOCKED",
            "HTTP tool cannot target private or reserved IP addresses.",
            422,
            {"hostname": hostname},
        )


def tool_output_preview(output: dict[str, Any]) -> dict[str, Any]:
    serialized = json.dumps(output, sort_keys=True, default=str)
    if len(serialized) <= 1000:
        return output
    return {"preview": serialized[:1000], "truncated": True}
