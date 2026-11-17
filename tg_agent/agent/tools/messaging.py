"""Agent tools for Telegram messaging.

This module is intentionally an order-preserving entrypoint. Tool handler
groups live in sibling modules so write, media, moderation, and read/state
logic can evolve independently without changing the public registration order.
"""

from __future__ import annotations

from tg_agent.agent.tools._categories import ToolCategory, ToolMeta
from tg_agent.agent.tools._registry import get_tool_context
from tg_agent.agent.tools.messaging_admin import register_admin_moderation_tools
from tg_agent.agent.tools.messaging_chat_state import register_chat_state_read_tools
from tg_agent.agent.tools.messaging_media import register_pin_media_tools
from tg_agent.agent.tools.messaging_write import register_message_write_tools
from tg_agent.agent.tools.telegram_queue import register_queue_status_tools

# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Сообщения", {
        "send_message": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="send"),
        "send_reaction": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "send_reactions": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "forward_messages": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="send"),
        "edit_message": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="send"),
        "edit_message_media": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="send"),
        "delete_message": ToolMeta(ToolCategory.DELETE, phone_bound=True, flood_category="send"),
        "pin_message": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="send"),
        "unpin_message": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="send"),
        "download_media": ToolMeta(ToolCategory.READ, phone_bound=True),
        "read_messages": ToolMeta(ToolCategory.READ, phone_bound=True, flood_category="history"),
        "get_telegram_queue_status": ToolMeta(ToolCategory.READ, phone_bound=True),
        "cancel_telegram_command": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "clear_pending_telegram_commands": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "translate_message": ToolMeta(ToolCategory.WRITE),
    }),
    ("Управление чатом", {
        "get_participants": ToolMeta(ToolCategory.READ, phone_bound=True),
        "edit_admin": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="admin_action"),
        "edit_permissions": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="admin_action"),
        "kick_participant": ToolMeta(ToolCategory.DELETE, phone_bound=True, flood_category="admin_action"),
        "get_broadcast_stats": ToolMeta(ToolCategory.READ, phone_bound=True),
        "archive_chat": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="admin_action"),
        "unarchive_chat": ToolMeta(ToolCategory.WRITE, phone_bound=True, flood_category="admin_action"),
        "mark_read": ToolMeta(ToolCategory.WRITE, phone_bound=True),
    }),
]

def register(db, client_pool, embedding_service, **kwargs):
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []
    tools.extend(register_message_write_tools(ctx, client_pool))
    tools.extend(register_pin_media_tools(ctx, client_pool))
    tools.extend(register_admin_moderation_tools(ctx, client_pool))
    tools.extend(register_chat_state_read_tools(db, ctx, client_pool))
    tools.extend(register_queue_status_tools(db, ctx))
    return tools
