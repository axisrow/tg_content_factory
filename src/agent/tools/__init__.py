"""Agent tools package — MCP tools covering all system operations.

Usage::

    from src.agent.tools import make_mcp_server
    server = make_mcp_server(db, client_pool=pool)
"""

from __future__ import annotations

import functools

from claude_agent_sdk import SdkMcpTool, create_sdk_mcp_server

from src.agent.tools._registry import _text_response  # noqa: F401


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


def make_mcp_server(db, client_pool=None, scheduler_manager=None, config=None):
    """Create an in-process MCP server with all agent tools.

    Args:
        db: Database instance for all DB operations.
        client_pool: Optional ClientPool for Telegram operations.
            If None (CLI mode), pool-dependent tools return an error message.
        scheduler_manager: Optional live SchedulerManager instance.
            If None, scheduler tools return an error message.
    """
    from src.services.embedding_service import EmbeddingService

    embedding_service = EmbeddingService(db, config=config)

    # Import all tool modules
    from src.agent.tools import (
        accounts,
        agent_threads,
        analytics,
        channels,
        collection,
        filters,
        images,
        messaging,
        moderation,
        my_telegram,
        notifications,
        photo_loader,
        pipelines,
        scheduler,
        search,
        search_queries,
        settings,
    )

    # Extra context passed alongside the standard (db, client_pool, embedding_service)
    extras = {"scheduler_manager": scheduler_manager, "config": config}

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
        my_telegram,
        messaging,
        images,
        settings,
        agent_threads,
    ]:
        for tool_obj in module.register(db, client_pool, embedding_service, **extras):
            all_tools.append(_wrap_with_session_gate(tool_obj))

    return create_sdk_mcp_server(
        name="telegram_db",
        tools=all_tools,
    )
