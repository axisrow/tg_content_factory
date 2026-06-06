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

    Non-phone-bound tools may ask PermissionGate before execution. Phone-bound
    tools skip this wrapper so the prompt happens after phone resolution.
    """
    tool_name = tool.name
    original_handler = tool.handler

    @functools.wraps(original_handler)
    async def wrapped_handler(*args, **kwargs):
        from src.agent.permission_gate import get_gate, get_request_context
        from src.agent.tools.permissions import PHONE_BINDED_TOOLS, ToolAccessState

        if tool_name in PHONE_BINDED_TOOLS:
            return await original_handler(*args, **kwargs)
        gate = get_gate()
        if gate is not None:
            ctx = get_request_context()
            state = None
            if ctx is not None and ctx.tool_access_policy is not None:
                state = ctx.tool_access_policy.get(tool_name, ToolAccessState.DENIED)
            elif ctx is not None and ctx.db_permissions is not None:
                state = ToolAccessState.ALLOWED if ctx.db_permissions.get(tool_name, True) else ToolAccessState.DENIED
            if state == ToolAccessState.REQUESTABLE:
                result = await gate.check(tool_name, kwargs.get("phone", ""))
                if result is not None:
                    return result
            if state == ToolAccessState.DENIED:
                return _text_response(f"❌ Доступ к '{tool_name}' запрещён настройками агента.")
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


def make_mcp_server(db, client_pool=None, scheduler_manager=None, config=None, access_policy=None):
    """Create an in-process MCP server with all agent tools.

    Args:
        db: Database instance for all DB operations.
        client_pool: Optional ClientPool for Telegram operations.
            If None (CLI mode), pool-dependent tools return an error message.
        scheduler_manager: Optional live SchedulerManager instance.
            If None, scheduler tools return an error message.
        access_policy: Optional tri-state tool ACL (``{bare_name: ToolAccessState}``,
            from :func:`load_tool_access_policy`). When provided, tools not visible
            under it are NOT registered — this is the only ACL enforcement available
            to the out-of-process ``mcp-server`` path, where the call-time session
            gate (a ContextVar) cannot reach across the process boundary. The
            in-process backends leave this ``None`` because they filter via the
            SDK ``allowed_tools`` allow-list instead. Filtering uses
            ``gate_active=False`` (unattended): DENIED and REQUESTABLE tools are
            both hidden, since a headless subprocess cannot prompt for a grant.
    """
    all_tools = build_agent_tool_registry(
        db,
        client_pool=client_pool,
        scheduler_manager=scheduler_manager,
        config=config,
    )

    if access_policy is not None:
        from src.agent.tools.permissions import is_tool_visible_for_llm

        all_tools = [
            tool_obj
            for tool_obj in all_tools
            if is_tool_visible_for_llm(tool_obj.name, access_policy, gate_active=False)
        ]

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
    "list_dialogs_for_import",
    "list_search_queries",
    "get_search_query_stats",
    "get_pipeline_dry_run_count",
    "get_trending_emojis",
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
