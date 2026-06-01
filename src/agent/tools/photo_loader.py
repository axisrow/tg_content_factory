"""Agent tools for photo upload and scheduling."""

from __future__ import annotations

from src.agent.tools._registry import get_tool_context
from src.agent.tools.photo_loader_read import (
    register_auto_read_tools,
    register_batch_read_tools,
    register_dialog_tools,
)
from src.agent.tools.photo_loader_write import (
    register_auto_write_tools,
    register_batch_write_tools,
    register_send_tools,
)


def register(db, client_pool, embedding_service, **kwargs):
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []
    tools.extend(register_batch_read_tools(db, client_pool))
    tools.extend(register_send_tools(db, ctx, client_pool))
    tools.extend(register_auto_read_tools(db, client_pool))
    auto_write_tools = register_auto_write_tools(db, ctx, client_pool)
    tools.extend(auto_write_tools[:2])
    tools.extend(register_batch_write_tools(db, ctx, client_pool))
    tools.extend(auto_write_tools[2:])
    tools.extend(register_dialog_tools(db, ctx, client_pool))
    return tools
