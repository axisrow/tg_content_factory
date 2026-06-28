"""Agent tools for photo upload and scheduling."""

from __future__ import annotations

from src.agent.tools._categories import ToolCategory, ToolMeta
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

# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Фото", {
        "list_photo_batches": ToolMeta(ToolCategory.READ),
        "list_photo_items": ToolMeta(ToolCategory.READ),
        "send_photos_now": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "schedule_photos": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "cancel_photo_item": ToolMeta(ToolCategory.WRITE),
        "list_auto_uploads": ToolMeta(ToolCategory.READ),
        "toggle_auto_upload": ToolMeta(ToolCategory.WRITE),
        "delete_auto_upload": ToolMeta(ToolCategory.DELETE),
        "create_photo_batch": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "publish_photo_batch": ToolMeta(ToolCategory.WRITE),
        "run_photo_due": ToolMeta(ToolCategory.WRITE),
        "create_auto_upload": ToolMeta(ToolCategory.WRITE, phone_bound=True),
        "update_auto_upload": ToolMeta(ToolCategory.WRITE),
        "list_photo_dialogs": ToolMeta(ToolCategory.READ, phone_bound=True),
        "refresh_photo_dialogs": ToolMeta(ToolCategory.WRITE, phone_bound=True),
    }),
]

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
