from __future__ import annotations

import copy
import json
import re
from collections import defaultdict, deque
from typing import Any

from agentops_api.errors import ApiError

NODE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,99}$")
EXPRESSION = re.compile(r"^\$\{(.+)}$")
REFERENCE = re.compile(
    r"^(?:workflow\.input(?:\.([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*))?|"
    r"nodes\.([A-Za-z][A-Za-z0-9_-]{0,99})\.output"
    r"(?:\.([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*))?)$"
)
COMPARISON = re.compile(r"^(.+?)\s*(==|!=)\s*(true|false|null|-?\d+(?:\.\d+)?|\"[^\"]*\")$")
SUPPORTED_NODE_TYPES = {"agent", "tool", "approval"}


def validate_definition(definition: dict[str, Any]) -> None:
    nodes = definition.get("nodes")
    edges = definition.get("edges")
    if not isinstance(nodes, list) or not nodes:
        raise ApiError("INVALID_WORKFLOW", "Workflow requires at least one node.", 422)
    if not isinstance(edges, list):
        raise ApiError("INVALID_WORKFLOW", "Workflow edges must be a list.", 422)

    node_ids: list[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            raise ApiError("INVALID_WORKFLOW", "Workflow nodes must be objects.", 422)
        node_id = node.get("id")
        node_type = node.get("type")
        if not isinstance(node_id, str) or not NODE_ID.fullmatch(node_id):
            raise ApiError("INVALID_WORKFLOW", "Workflow node IDs are invalid.", 422)
        if node_type not in SUPPORTED_NODE_TYPES:
            raise ApiError(
                "INVALID_WORKFLOW",
                "Workflow node type is unsupported.",
                422,
                {"node_id": node_id, "type": node_type},
            )
        if node_type == "agent" and not isinstance(node.get("agent_version_id"), str):
            raise ApiError("INVALID_WORKFLOW", "Agent nodes require agent_version_id.", 422)
        if node_type == "tool" and not isinstance(node.get("tool_version_id"), str):
            raise ApiError("INVALID_WORKFLOW", "Tool nodes require tool_version_id.", 422)
        node_ids.append(node_id)

    if len(node_ids) != len(set(node_ids)):
        raise ApiError("INVALID_WORKFLOW", "Workflow contains duplicate node IDs.", 422)

    node_set = set(node_ids)
    outgoing: dict[str, list[str]] = defaultdict(list)
    incoming: dict[str, int] = {node_id: 0 for node_id in node_ids}
    for edge in edges:
        if not isinstance(edge, dict):
            raise ApiError("INVALID_WORKFLOW", "Workflow edges must be objects.", 422)
        source, target = edge.get("from"), edge.get("to")
        if not isinstance(source, str) or not isinstance(target, str):
            raise ApiError("INVALID_WORKFLOW", "Workflow edges require from and to.", 422)
        if source not in node_set or target not in node_set:
            raise ApiError(
                "INVALID_WORKFLOW", "Workflow edge references a missing node.", 422, {"edge": edge}
            )
        outgoing[source].append(target)
        incoming[target] += 1
        condition = edge.get("condition")
        if condition is not None:
            _validate_condition(condition, node_set)

    starts = [node_id for node_id, count in incoming.items() if count == 0]
    if len(starts) != 1:
        raise ApiError(
            "INVALID_WORKFLOW",
            "Workflow must have exactly one start node.",
            422,
            {"start_nodes": starts},
        )
    if not any(not outgoing[node_id] for node_id in node_ids):
        raise ApiError("INVALID_WORKFLOW", "Workflow requires a terminal node.", 422)

    visited: set[str] = set()
    queue = deque(starts)
    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        queue.extend(outgoing[node_id])
    if visited != node_set:
        raise ApiError(
            "INVALID_WORKFLOW",
            "Workflow contains nodes unreachable from its start node.",
            422,
            {"node_ids": sorted(node_set - visited)},
        )

    if len(topological_layers(definition)) == 0:
        raise ApiError("INVALID_WORKFLOW", "Workflow contains a cycle.", 422)

    for node in nodes:
        _validate_expressions(node.get("input", {}), node_set)


def topological_layers(definition: dict[str, Any]) -> list[list[str]]:
    nodes = [str(node["id"]) for node in definition["nodes"]]
    outgoing: dict[str, list[str]] = defaultdict(list)
    incoming = {node_id: 0 for node_id in nodes}
    for edge in definition.get("edges", []):
        source, target = str(edge["from"]), str(edge["to"])
        outgoing[source].append(target)
        incoming[target] += 1

    layers: list[list[str]] = []
    ready = sorted(node_id for node_id, count in incoming.items() if count == 0)
    processed = 0
    while ready:
        layer = ready
        layers.append(layer)
        ready = []
        for node_id in layer:
            processed += 1
            for child in outgoing[node_id]:
                incoming[child] -= 1
                if incoming[child] == 0:
                    ready.append(child)
        ready.sort()
    return layers if processed == len(nodes) else []


def incoming_edges(definition: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    edges: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in definition.get("edges", []):
        edges[str(edge["to"])].append(edge)
    return edges


def resolve_value(value: Any, workflow_input: dict[str, Any], outputs: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {key: resolve_value(item, workflow_input, outputs) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_value(item, workflow_input, outputs) for item in value]
    if not isinstance(value, str):
        return value
    match = EXPRESSION.fullmatch(value)
    if match:
        return copy.deepcopy(_resolve_reference(match.group(1), workflow_input, outputs))

    def replace(match: re.Match[str]) -> str:
        return str(_resolve_reference(match.group(1), workflow_input, outputs))

    return re.sub(r"\$\{([^}]+)}", replace, value)


def edge_allows(
    edge: dict[str, Any], workflow_input: dict[str, Any], outputs: dict[str, Any]
) -> bool:
    condition = edge.get("condition")
    if condition is None:
        return True
    expression = _expression_text(condition)
    comparison = COMPARISON.fullmatch(expression)
    if comparison:
        left, operator, raw_right = comparison.groups()
        actual = _resolve_reference(left.strip(), workflow_input, outputs)
        expected = json.loads(raw_right)
        return bool(actual == expected) if operator == "==" else bool(actual != expected)
    return bool(_resolve_reference(expression, workflow_input, outputs))


def _validate_expressions(value: Any, node_ids: set[str]) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _validate_expressions(item, node_ids)
    elif isinstance(value, list):
        for item in value:
            _validate_expressions(item, node_ids)
    elif isinstance(value, str):
        for expression in re.findall(r"\$\{([^}]+)}", value):
            _validate_reference(expression, node_ids)


def _validate_condition(condition: Any, node_ids: set[str]) -> None:
    expression = _expression_text(condition)
    comparison = COMPARISON.fullmatch(expression)
    _validate_reference((comparison.group(1) if comparison else expression).strip(), node_ids)


def _expression_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ApiError("INVALID_WORKFLOW", "Workflow expressions must be strings.", 422)
    match = EXPRESSION.fullmatch(value)
    if not match:
        raise ApiError("INVALID_WORKFLOW", "Workflow expressions must use ${...}.", 422)
    return match.group(1)


def _validate_reference(expression: str, node_ids: set[str]) -> None:
    match = REFERENCE.fullmatch(expression)
    if not match:
        raise ApiError(
            "INVALID_WORKFLOW",
            "Workflow expression is unsupported.",
            422,
            {"expression": expression},
        )
    node_id = match.group(2)
    if node_id is not None and node_id not in node_ids:
        raise ApiError(
            "INVALID_WORKFLOW",
            "Workflow expression references a missing node.",
            422,
            {"node_id": node_id},
        )


def _resolve_reference(
    expression: str, workflow_input: dict[str, Any], outputs: dict[str, Any]
) -> Any:
    match = REFERENCE.fullmatch(expression)
    if match is None:
        raise ApiError("INVALID_WORKFLOW", "Workflow expression is unsupported.", 422)
    workflow_path, node_id, output_path = match.groups()
    if node_id is None:
        return _read_path(workflow_input, workflow_path)
    return _read_path(outputs.get(node_id), output_path)


def _read_path(value: Any, path: str | None) -> Any:
    current = value
    if path is None:
        return current
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
