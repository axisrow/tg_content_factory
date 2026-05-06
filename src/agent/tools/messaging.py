"""Agent tools for Telegram messaging.

This module is intentionally an order-preserving entrypoint. Tool handler
groups live in sibling modules so write, media, moderation, and read/state
logic can evolve independently without changing the public registration order.
"""

from __future__ import annotations

from src.agent.tools._registry import get_tool_context
from src.agent.tools.messaging_admin import register_admin_moderation_tools
from src.agent.tools.messaging_chat_state import register_chat_state_read_tools
from src.agent.tools.messaging_media import register_pin_media_tools
from src.agent.tools.messaging_write import register_message_write_tools


def register(db, client_pool, embedding_service, **kwargs):
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []
    tools.extend(register_message_write_tools(ctx, client_pool))
    tools.extend(register_pin_media_tools(ctx, client_pool))
    tools.extend(register_admin_moderation_tools(ctx, client_pool))
    tools.extend(register_chat_state_read_tools(db, ctx, client_pool))
    return tools
