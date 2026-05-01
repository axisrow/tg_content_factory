"""Synchronous Deepagents adapter for the shared agent tools registry."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any

from claude_agent_sdk import SdkMcpTool

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools import build_agent_tool_registry

logger = logging.getLogger(__name__)

_LEGACY_ARG_ALIASES: dict[str, dict[str, str]] = {
    "search_messages": {"query_text": "query"},
    "semantic_search": {"query_text": "query"},
}

_REQUIRED_ARGS_BY_TOOL: dict[str, frozenset[str]] = {
    "search_messages": frozenset({"query"}),
    "semantic_search": frozenset({"query"}),
    "search_telegram": frozenset({"query"}),
    "search_my_chats": frozenset({"query"}),
    "search_in_channel": frozenset({"channel_id", "query"}),
    "search_hybrid": frozenset({"query"}),
    "add_channel": frozenset({"identifier"}),
    "delete_channel": frozenset({"pk"}),
    "toggle_channel": frozenset({"pk"}),
    "import_channels": frozenset({"text"}),
    "create_tag": frozenset({"name"}),
    "delete_tag": frozenset({"name"}),
    "set_channel_tags": frozenset({"pk"}),
    "collect_channel": frozenset({"pk"}),
    "collect_channel_stats": frozenset({"pk"}),
    "get_pipeline_detail": frozenset({"pipeline_id"}),
    "get_refinement_steps": frozenset({"pipeline_id"}),
    "add_pipeline": frozenset({"name", "prompt_template", "source_channel_ids", "target_refs"}),
    "edit_pipeline": frozenset({"pipeline_id"}),
    "toggle_pipeline": frozenset({"pipeline_id"}),
    "delete_pipeline": frozenset({"pipeline_id"}),
    "run_pipeline": frozenset({"pipeline_id"}),
    "list_pipeline_runs": frozenset({"pipeline_id"}),
    "get_pipeline_run": frozenset({"run_id"}),
    "publish_pipeline_run": frozenset({"run_id"}),
    "set_refinement_steps": frozenset({"pipeline_id", "steps_json"}),
    "export_pipeline_json": frozenset({"pipeline_id"}),
    "import_pipeline_json": frozenset({"json_text"}),
    "create_pipeline_from_template": frozenset({"template_id", "name"}),
    "ai_edit_pipeline": frozenset({"pipeline_id", "instruction"}),
    "view_moderation_run": frozenset({"run_id"}),
    "approve_run": frozenset({"run_id"}),
    "reject_run": frozenset({"run_id"}),
    "bulk_approve_runs": frozenset({"run_ids"}),
    "bulk_reject_runs": frozenset({"run_ids"}),
    "get_search_query": frozenset({"sq_id"}),
    "add_search_query": frozenset({"query"}),
    "edit_search_query": frozenset({"sq_id"}),
    "delete_search_query": frozenset({"sq_id"}),
    "toggle_search_query": frozenset({"sq_id"}),
    "run_search_query": frozenset({"sq_id"}),
    "get_search_query_stats": frozenset({"sq_id"}),
    "toggle_account": frozenset({"account_id"}),
    "delete_account": frozenset({"account_id"}),
    "toggle_channel_filter": frozenset({"pk"}),
    "hard_delete_channels": frozenset({"pks"}),
    "toggle_scheduler_job": frozenset({"job_id"}),
    "set_scheduler_interval": frozenset({"job_id", "minutes"}),
    "cancel_scheduler_task": frozenset({"task_id"}),
    "send_photos_now": frozenset({"target", "file_paths"}),
    "schedule_photos": frozenset({"target", "file_paths", "schedule_at"}),
    "cancel_photo_item": frozenset({"item_id"}),
    "toggle_auto_upload": frozenset({"job_id"}),
    "delete_auto_upload": frozenset({"job_id"}),
    "create_photo_batch": frozenset({"target", "file_paths"}),
    "create_auto_upload": frozenset({"target", "folder_path"}),
    "update_auto_upload": frozenset({"job_id"}),
    "leave_dialogs": frozenset({"dialog_ids"}),
    "create_telegram_channel": frozenset({"title"}),
    "get_forum_topics": frozenset({"channel_id"}),
    "resolve_entity": frozenset({"identifier"}),
    "send_message": frozenset({"recipient", "text"}),
    "edit_message": frozenset({"chat_id", "message_id", "text"}),
    "delete_message": frozenset({"chat_id", "message_ids"}),
    "forward_messages": frozenset({"from_chat", "to_chat", "message_ids"}),
    "pin_message": frozenset({"chat_id", "message_id"}),
    "unpin_message": frozenset({"chat_id"}),
    "download_media": frozenset({"chat_id", "message_id"}),
    "get_participants": frozenset({"chat_id"}),
    "edit_admin": frozenset({"chat_id", "user_id"}),
    "edit_permissions": frozenset({"chat_id", "user_id"}),
    "kick_participant": frozenset({"chat_id", "user_id"}),
    "get_broadcast_stats": frozenset({"chat_id"}),
    "archive_chat": frozenset({"chat_id"}),
    "unarchive_chat": frozenset({"chat_id"}),
    "mark_read": frozenset({"chat_id"}),
    "read_messages": frozenset({"chat_id"}),
    "generate_image": frozenset({"prompt"}),
    "list_image_models": frozenset({"provider"}),
    "delete_agent_thread": frozenset({"thread_id"}),
    "rename_agent_thread": frozenset({"thread_id", "title"}),
    "get_thread_messages": frozenset({"thread_id"}),
}


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


def _required_schema_args(tool_name: str, input_schema: object) -> frozenset[str]:
    if isinstance(input_schema, dict):
        required = input_schema.get("required")
        if isinstance(required, list):
            return frozenset(name for name in required if isinstance(name, str))
    return _REQUIRED_ARGS_BY_TOOL.get(tool_name, frozenset())


def _make_signature(tool_name: str, input_schema: object) -> inspect.Signature:
    params: list[inspect.Parameter] = []
    required = _required_schema_args(tool_name, input_schema)
    for name, annotation in _schema_items(input_schema):
        if not name.isidentifier():
            continue
        params.append(
            inspect.Parameter(
                name,
                inspect.Parameter.KEYWORD_ONLY,
                default=inspect.Parameter.empty if name in required else None,
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
    sync_tool.__signature__ = _make_signature(tool.name, tool.input_schema)  # type: ignore[attr-defined]
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
