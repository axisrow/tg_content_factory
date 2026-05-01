"""Agent tools package — MCP tools covering all system operations.

Usage::

    from src.agent.tools import make_mcp_server
    server = make_mcp_server(db, client_pool=pool)
"""

from __future__ import annotations

import functools

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools._registry import AgentToolContext, _text_response  # noqa: F401


def _wrap_with_session_gate(tool: SdkMcpTool) -> SdkMcpTool:
    """Wrap a SdkMcpTool with a session-level permission check.

    If the tool is disabled in DB settings for all phones (db_permissions[tool_name] == False)
    and a PermissionGate is active, shows an interactive dialog instead of blocking silently.
    Phone-level checks are handled separately inside require_phone_permission().
    """
    tool_name = tool.name
    original_handler = tool.handler

    @functools.wraps(original_handler)
    async def wrapped_handler(*args, **kwargs):
        from src.agent.permission_gate import get_gate, get_request_context

        gate = get_gate()
        if gate is not None:
            ctx = get_request_context()
            if ctx is not None and not ctx.db_permissions.get(tool_name, True):
                result = await gate.check(tool_name, kwargs.get("phone", ""))
                if result is not None:
                    return result
        return await original_handler(*args, **kwargs)

    return SdkMcpTool(
        name=tool.name,
        description=tool.description,
        input_schema=tool.input_schema,
        handler=wrapped_handler,
        annotations=tool.annotations,
    )


def build_agent_tool_registry(
    db,
    client_pool=None,
    scheduler_manager=None,
    config=None,
    runtime_context: AgentRuntimeContext | None = None,
    *,
    wrap_session_gate: bool = True,
) -> list[SdkMcpTool]:
    """Build the authoritative agent tools registry shared by all backends."""
    from src.services.embedding_service import EmbeddingService

    embedding_service = EmbeddingService(db, config=config)

    # Import all tool modules
    from src.agent.tools import (
        accounts,
        agent_threads,
        analytics,
        channels,
        collection,
        dialogs,
        filters,
        images,
        messaging,
        moderation,
        notifications,
        photo_loader,
        pipelines,
        scheduler,
        search,
        search_queries,
        settings,
    )

    # Extra context passed alongside the standard (db, client_pool, embedding_service)
    runtime_context = runtime_context or AgentRuntimeContext.build(
        db=db,
        config=config,
        client_pool=client_pool,
        scheduler_manager=scheduler_manager,
    )
    tool_context = AgentToolContext.build(
        db=db,
        config=config,
        client_pool=client_pool,
        scheduler_manager=scheduler_manager,
        embedding_service=embedding_service,
        runtime_context=runtime_context,
    )
    extras = {
        "scheduler_manager": scheduler_manager,
        "config": config,
        "runtime_context": runtime_context,
        "tool_context": tool_context,
    }

    all_tools = []
    for module in [
        search,
        channels,
        collection,
        pipelines,
        moderation,
        search_queries,
        accounts,
        filters,
        analytics,
        scheduler,
        notifications,
        photo_loader,
        dialogs,
        messaging,
        images,
        settings,
        agent_threads,
    ]:
        for tool_obj in module.register(db, client_pool, embedding_service, **extras):
            all_tools.append(_wrap_with_session_gate(tool_obj) if wrap_session_gate else tool_obj)

    return all_tools


def make_mcp_server(db, client_pool=None, scheduler_manager=None, config=None):
    """Create an in-process MCP server with all agent tools.

    Args:
        db: Database instance for all DB operations.
        client_pool: Optional ClientPool for Telegram operations.
            If None (CLI mode), pool-dependent tools return an error message.
        scheduler_manager: Optional live SchedulerManager instance.
            If None, scheduler tools return an error message.
    """
    all_tools = build_agent_tool_registry(
        db,
        client_pool=client_pool,
        scheduler_manager=scheduler_manager,
        config=config,
    )

    return create_sdk_mcp_server(
        name="telegram_db",
        tools=all_tools,
    )


# Tool names safe for unattended pipeline execution (read-only subset).
_PIPELINE_SAFE_TOOLS: frozenset[str] = frozenset({
    "search_messages",
    "semantic_search",
    "search_telegram",
    "search_my_chats",
    "search_in_channel",
    "search_hybrid",
    "list_channels",
    "get_channel_stats",
    "collect_channel_stats",
    "collect_all_stats",
    "list_pipelines",
    "get_pipeline_detail",
    "list_pipeline_runs",
    "get_pipeline_run",
    "get_pipeline_queue",
    "get_refinement_steps",
    "export_pipeline_json",
    "list_pipeline_templates",
    "list_pending_moderation",
    "view_moderation_run",
    "get_top_messages",
    "get_content_type_stats",
    "get_hourly_activity",
    "get_analytics_summary",
    "get_daily_stats",
    "get_pipeline_stats",
    "get_trending_topics",
    "get_trending_channels",
    "get_message_velocity",
    "get_peak_hours",
    "get_calendar",
    "list_tags",
    "list_search_queries",
    "get_search_query_stats",
})


def _adapt_sdk_tool(sdk_tool: SdkMcpTool):
    """Adapt an SdkMcpTool for direct pipeline invocation.

    SdkMcpTool.handler(args: dict) → dict (MCP response format).
    AgentLoopHandler calls fn(**kwargs) → str.

    This wrapper translates between the two calling conventions.
    """

    async def wrapper(**kwargs):
        result = await sdk_tool.handler(kwargs)
        if isinstance(result, dict) and "content" in result:
            parts = []
            for item in result["content"]:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(item.get("text", ""))
            return "\n".join(parts) if parts else str(result)
        return str(result)

    wrapper.__doc__ = sdk_tool.description
    return wrapper


def build_agent_tools_dict(
    db,
    client_pool=None,
    search_engine=None,
    config=None,
    scheduler_manager=None,
    runtime_context: AgentRuntimeContext | None = None,
):
    """Build a dict {tool_name: async_callable} for AgentLoopHandler in pipeline context.

    Only read-only tools are included — destructive operations require interactive
    confirmation which is unavailable in automated pipeline execution.

    Args:
        db: Database instance.
        client_pool: Optional ClientPool for Telegram operations.
        search_engine: Optional SearchEngine for search tools.
        config: Optional config dict.
    """
    runtime_context = runtime_context or AgentRuntimeContext.build(
        db=db,
        config=config,
        client_pool=client_pool,
        scheduler_manager=scheduler_manager,
    )
    tools_dict: dict[str, object] = {}

    for tool_obj in build_agent_tool_registry(
        db,
        client_pool=client_pool,
        scheduler_manager=scheduler_manager,
        config=config,
        runtime_context=runtime_context,
        wrap_session_gate=False,
    ):
        if tool_obj.name in _PIPELINE_SAFE_TOOLS:
            tools_dict[tool_obj.name] = _adapt_sdk_tool(tool_obj)

    return tools_dict
