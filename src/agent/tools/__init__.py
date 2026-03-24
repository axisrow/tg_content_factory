"""Agent tools package — MCP tools covering all system operations.

Usage::

    from src.agent.tools import make_mcp_server
    server = make_mcp_server(db, client_pool=pool)
"""

from __future__ import annotations

from claude_agent_sdk import create_sdk_mcp_server

from src.agent.tools._registry import _text_response  # noqa: F401


def make_mcp_server(db, client_pool=None, scheduler_manager=None):
    """Create an in-process MCP server with all agent tools.

    Args:
        db: Database instance for all DB operations.
        client_pool: Optional ClientPool for Telegram operations.
            If None (CLI mode), pool-dependent tools return an error message.
        scheduler_manager: Optional live SchedulerManager instance.
            If None, scheduler tools return an error message.
    """
    from src.services.embedding_service import EmbeddingService

    embedding_service = EmbeddingService(db)

    # Import all tool modules
    from src.agent.tools import (
        accounts,
        agent_threads,
        analytics,
        channels,
        collection,
        filters,
        images,
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
    extras = {"scheduler_manager": scheduler_manager}

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
        images,
        settings,
        agent_threads,
    ]:
        all_tools.extend(module.register(db, client_pool, embedding_service, **extras))

    return create_sdk_mcp_server(
        name="telegram_db",
        tools=all_tools,
    )
