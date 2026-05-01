"""Synchronous Deepagents adapter for the shared agent tools registry."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from claude_agent_sdk import SdkMcpTool

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools import build_agent_tool_registry

_T = TypeVar("_T")
logger = logging.getLogger(__name__)

_LEGACY_ARG_ALIASES: dict[str, dict[str, str]] = {
    "search_messages": {"query_text": "query"},
    "semantic_search": {"query_text": "query"},
}


def _run_sync(tool_name: str, operation: Callable[[], Awaitable[_T]]) -> _T:
    """Run an async operation synchronously (must be called outside event loop)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(operation())
    raise RuntimeError(f"Deepagents tool '{tool_name}' cannot run inside an active event loop")


def _schema_items(input_schema: object) -> list[tuple[str, object]]:
    if not isinstance(input_schema, dict):
        return []
    properties = input_schema.get("properties")
    if isinstance(properties, dict):
        return [(name, annotation) for name, annotation in properties.items() if isinstance(name, str)]
    return [(name, annotation) for name, annotation in input_schema.items() if isinstance(name, str)]


def _annotation_for_signature(annotation: object) -> object:
    if isinstance(annotation, dict):
        return Any
    return annotation


def _make_signature(input_schema: object) -> inspect.Signature:
    params: list[inspect.Parameter] = []
    for name, annotation in _schema_items(input_schema):
        if not name.isidentifier():
            continue
        params.append(
            inspect.Parameter(
                name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=None,
                annotation=_annotation_for_signature(annotation),
            )
        )
    return inspect.Signature(params, return_annotation=str)


def _mcp_result_to_text(result: object) -> str:
    if not isinstance(result, dict) or "content" not in result:
        return str(result)

    parts: list[str] = []
    for item in result.get("content") or []:
        if isinstance(item, dict):
            if item.get("type") == "text":
                parts.append(str(item.get("text", "")))
            continue
        if getattr(item, "type", None) == "text":
            parts.append(str(getattr(item, "text", "")))
    return "\n".join(part for part in parts if part) if parts else str(result)


def _arguments_from_call(tool: SdkMcpTool, args: tuple[object, ...], kwargs: dict[str, object]) -> dict[str, object]:
    schema_names = [name for name, _annotation in _schema_items(tool.input_schema)]
    if len(args) > len(schema_names):
        raise TypeError(f"{tool.name} expected at most {len(schema_names)} positional arguments")

    tool_args = dict(kwargs)
    for index, value in enumerate(args):
        tool_args.setdefault(schema_names[index], value)

    for legacy_name, canonical_name in _LEGACY_ARG_ALIASES.get(tool.name, {}).items():
        if canonical_name not in tool_args and legacy_name in tool_args:
            tool_args[canonical_name] = tool_args[legacy_name]
    return tool_args


def _adapt_sdk_tool(tool: SdkMcpTool, runtime_context: AgentRuntimeContext) -> Callable[..., str]:
    """Adapt an async registry tool to the sync callable shape expected by Deepagents."""

    def sync_tool(*args, **kwargs) -> str:
        try:
            tool_args = _arguments_from_call(tool, args, kwargs)
            result = runtime_context.run_sync(tool.name, lambda: tool.handler(tool_args))
            return _mcp_result_to_text(result)
        except Exception as exc:
            logger.warning("Deepagents tool %s failed: %s", tool.name, exc, exc_info=True)
            return f"Ошибка выполнения {tool.name}: {exc}"

    sync_tool.__name__ = tool.name
    sync_tool.__qualname__ = tool.name
    sync_tool.__doc__ = tool.description
    sync_tool.__module__ = __name__
    sync_tool.__signature__ = _make_signature(tool.input_schema)  # type: ignore[attr-defined]
    sync_tool.__annotations__ = {
        name: _annotation_for_signature(annotation)
        for name, annotation in _schema_items(tool.input_schema)
        if name.isidentifier()
    }
    sync_tool.__annotations__["return"] = str
    return sync_tool


def build_deepagents_tools(
    db,
    client_pool=None,
    config=None,
    runtime_context: AgentRuntimeContext | None = None,
) -> list[Callable[..., str]]:
    """Build Deepagents tools from the authoritative shared agent tools registry."""
    if runtime_context is None:
        runtime_context = AgentRuntimeContext.build(
            db=db,
            config=config,
            client_pool=client_pool,
        )
    else:
        config = config if config is not None else runtime_context.config
        client_pool = client_pool if client_pool is not None else runtime_context.client_pool

    registry_tools = build_agent_tool_registry(
        db,
        client_pool=client_pool,
        scheduler_manager=runtime_context.scheduler_manager,
        config=config,
        runtime_context=runtime_context,
    )
    return [_adapt_sdk_tool(tool, runtime_context) for tool in registry_tools]
