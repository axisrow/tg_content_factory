"""Agent tool permission registry — classifies tools into read/write/delete categories."""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from enum import Enum

logger = logging.getLogger(__name__)

TOOL_PERMISSIONS_SETTING = "agent_tool_permissions"
MCP_PREFIX = "mcp__telegram_db__"


class ToolCategory(str, Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"


# ---------------------------------------------------------------------------
# Authoritative tool → category mapping.  Every tool in _ALLOWED_TOOLS must
# appear here.  The bare name (without the MCP prefix) is used as the key.
# ---------------------------------------------------------------------------

TOOL_CATEGORIES: dict[str, ToolCategory] = {
    # Search
    "search_messages": ToolCategory.READ,
    "semantic_search": ToolCategory.READ,
    "index_messages": ToolCategory.WRITE,
    # Channels
    "list_channels": ToolCategory.READ,
    "get_channel_stats": ToolCategory.READ,
    "add_channel": ToolCategory.WRITE,
    "delete_channel": ToolCategory.DELETE,
    "toggle_channel": ToolCategory.WRITE,
    "import_channels": ToolCategory.WRITE,
    "refresh_channel_types": ToolCategory.WRITE,
    # Collection
    "collect_channel": ToolCategory.WRITE,
    "collect_all_channels": ToolCategory.WRITE,
    "collect_channel_stats": ToolCategory.READ,
    "collect_all_stats": ToolCategory.READ,
    # Pipelines
    "list_pipelines": ToolCategory.READ,
    "get_pipeline_detail": ToolCategory.READ,
    "add_pipeline": ToolCategory.WRITE,
    "edit_pipeline": ToolCategory.WRITE,
    "toggle_pipeline": ToolCategory.WRITE,
    "delete_pipeline": ToolCategory.DELETE,
    "run_pipeline": ToolCategory.WRITE,
    "generate_draft": ToolCategory.WRITE,
    "list_pipeline_runs": ToolCategory.READ,
    "get_pipeline_run": ToolCategory.READ,
    "publish_pipeline_run": ToolCategory.WRITE,
    "get_pipeline_queue": ToolCategory.READ,
    # Moderation
    "list_pending_moderation": ToolCategory.READ,
    "view_moderation_run": ToolCategory.READ,
    "approve_run": ToolCategory.WRITE,
    "reject_run": ToolCategory.WRITE,
    "bulk_approve_runs": ToolCategory.WRITE,
    "bulk_reject_runs": ToolCategory.WRITE,
    # Search Queries
    "list_search_queries": ToolCategory.READ,
    "get_search_query": ToolCategory.READ,
    "add_search_query": ToolCategory.WRITE,
    "edit_search_query": ToolCategory.WRITE,
    "delete_search_query": ToolCategory.DELETE,
    "toggle_search_query": ToolCategory.WRITE,
    "run_search_query": ToolCategory.WRITE,
    # Accounts
    "list_accounts": ToolCategory.READ,
    "toggle_account": ToolCategory.WRITE,
    "delete_account": ToolCategory.DELETE,
    "get_flood_status": ToolCategory.READ,
    # Filters
    "analyze_filters": ToolCategory.READ,
    "apply_filters": ToolCategory.WRITE,
    "reset_filters": ToolCategory.WRITE,
    "toggle_channel_filter": ToolCategory.WRITE,
    "purge_filtered_channels": ToolCategory.DELETE,
    "hard_delete_channels": ToolCategory.DELETE,
    # Analytics
    "get_analytics_summary": ToolCategory.READ,
    "get_pipeline_stats": ToolCategory.READ,
    "get_daily_stats": ToolCategory.READ,
    "get_trending_topics": ToolCategory.READ,
    "get_trending_channels": ToolCategory.READ,
    "get_message_velocity": ToolCategory.READ,
    "get_peak_hours": ToolCategory.READ,
    "get_calendar": ToolCategory.READ,
    # Scheduler
    "get_scheduler_status": ToolCategory.READ,
    "start_scheduler": ToolCategory.WRITE,
    "stop_scheduler": ToolCategory.WRITE,
    "trigger_collection": ToolCategory.WRITE,
    "toggle_scheduler_job": ToolCategory.WRITE,
    # Notifications
    "get_notification_status": ToolCategory.READ,
    "setup_notification_bot": ToolCategory.WRITE,
    "delete_notification_bot": ToolCategory.DELETE,
    "test_notification": ToolCategory.WRITE,
    # Photo Loader
    "list_photo_batches": ToolCategory.READ,
    "list_photo_items": ToolCategory.READ,
    "send_photos_now": ToolCategory.WRITE,
    "schedule_photos": ToolCategory.WRITE,
    "cancel_photo_item": ToolCategory.WRITE,
    "list_auto_uploads": ToolCategory.READ,
    "toggle_auto_upload": ToolCategory.WRITE,
    "delete_auto_upload": ToolCategory.DELETE,
    # My Telegram
    "list_dialogs": ToolCategory.READ,
    "refresh_dialogs": ToolCategory.WRITE,
    "leave_dialogs": ToolCategory.DELETE,
    "create_telegram_channel": ToolCategory.WRITE,
    "get_forum_topics": ToolCategory.READ,
    "clear_dialog_cache": ToolCategory.WRITE,
    # Images
    "generate_image": ToolCategory.WRITE,
    "list_image_models": ToolCategory.READ,
    "list_image_providers": ToolCategory.READ,
    # Settings
    "get_settings": ToolCategory.READ,
    "save_scheduler_settings": ToolCategory.WRITE,
    "save_agent_settings": ToolCategory.WRITE,
    "save_filter_settings": ToolCategory.WRITE,
    "get_system_info": ToolCategory.READ,
    # Agent Threads
    "list_agent_threads": ToolCategory.READ,
    "create_agent_thread": ToolCategory.WRITE,
    "delete_agent_thread": ToolCategory.DELETE,
    "rename_agent_thread": ToolCategory.WRITE,
    "get_thread_messages": ToolCategory.READ,
}

# ---------------------------------------------------------------------------
# Module groups — ordered dict mapping display name → list of bare tool names.
# Order matches the registration order in tools/__init__.py.
# ---------------------------------------------------------------------------

MODULE_GROUPS: OrderedDict[str, list[str]] = OrderedDict([
    ("Поиск", [
        "search_messages", "semantic_search", "index_messages",
    ]),
    ("Каналы", [
        "list_channels", "get_channel_stats", "add_channel", "delete_channel",
        "toggle_channel", "import_channels", "refresh_channel_types",
    ]),
    ("Сбор", [
        "collect_channel", "collect_all_channels", "collect_channel_stats", "collect_all_stats",
    ]),
    ("Пайплайны", [
        "list_pipelines", "get_pipeline_detail", "add_pipeline", "edit_pipeline",
        "toggle_pipeline", "delete_pipeline", "run_pipeline", "generate_draft",
        "list_pipeline_runs", "get_pipeline_run", "publish_pipeline_run", "get_pipeline_queue",
    ]),
    ("Модерация", [
        "list_pending_moderation", "view_moderation_run", "approve_run", "reject_run",
        "bulk_approve_runs", "bulk_reject_runs",
    ]),
    ("Поисковые запросы", [
        "list_search_queries", "get_search_query", "add_search_query", "edit_search_query",
        "delete_search_query", "toggle_search_query", "run_search_query",
    ]),
    ("Аккаунты", [
        "list_accounts", "toggle_account", "delete_account", "get_flood_status",
    ]),
    ("Фильтры", [
        "analyze_filters", "apply_filters", "reset_filters", "toggle_channel_filter",
        "purge_filtered_channels", "hard_delete_channels",
    ]),
    ("Аналитика", [
        "get_analytics_summary", "get_pipeline_stats", "get_daily_stats",
        "get_trending_topics", "get_trending_channels", "get_message_velocity",
        "get_peak_hours", "get_calendar",
    ]),
    ("Планировщик", [
        "get_scheduler_status", "start_scheduler", "stop_scheduler",
        "trigger_collection", "toggle_scheduler_job",
    ]),
    ("Уведомления", [
        "get_notification_status", "setup_notification_bot", "delete_notification_bot",
        "test_notification",
    ]),
    ("Фото", [
        "list_photo_batches", "list_photo_items", "send_photos_now", "schedule_photos",
        "cancel_photo_item", "list_auto_uploads", "toggle_auto_upload", "delete_auto_upload",
    ]),
    ("Мой Telegram", [
        "list_dialogs", "refresh_dialogs", "leave_dialogs", "create_telegram_channel",
        "get_forum_topics", "clear_dialog_cache",
    ]),
    ("Изображения", [
        "list_image_models", "list_image_providers", "generate_image",
    ]),
    ("Настройки", [
        "get_settings", "save_scheduler_settings", "save_agent_settings",
        "save_filter_settings", "get_system_info",
    ]),
    ("Треды агента", [
        "list_agent_threads", "create_agent_thread", "delete_agent_thread",
        "rename_agent_thread", "get_thread_messages",
    ]),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_permissions() -> dict[str, bool]:
    """Default permissions: read=True, write/delete=False."""
    return {
        name: (cat == ToolCategory.READ)
        for name, cat in TOOL_CATEGORIES.items()
    }


async def load_tool_permissions(db) -> dict[str, bool]:
    """Load per-tool permissions from DB.  Missing setting → read-only defaults."""
    defaults = _default_permissions()
    raw = await db.get_setting(TOOL_PERMISSIONS_SETTING)
    if not raw:
        return defaults
    try:
        saved: dict = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupted tool permissions setting, using defaults")
        return defaults
    # Merge: saved values take precedence, new tools get default for their category
    return {name: saved.get(name, defaults[name]) for name in TOOL_CATEGORIES}


async def save_tool_permissions(db, permissions: dict[str, bool]) -> None:
    """Persist per-tool permissions as JSON."""
    await db.set_setting(TOOL_PERMISSIONS_SETTING, json.dumps(permissions, ensure_ascii=False))


def filter_allowed_tools(all_tools: list[str], permissions: dict[str, bool]) -> list[str]:
    """Filter MCP-prefixed tool names by permissions.

    Tools not present in TOOL_CATEGORIES pass through unchanged (future-proof).
    """
    result = []
    for prefixed_name in all_tools:
        bare = prefixed_name.removeprefix(MCP_PREFIX)
        if permissions.get(bare, True):
            result.append(prefixed_name)
    return result


def build_template_context(permissions: dict[str, bool]) -> dict:
    """Build template context dicts for the permissions UI.

    Returns dict with keys:
        tool_permission_categories: {category_value: [{name, module, enabled}, ...]}
        tool_permission_modules: [{name, display_name, tools: [{name, category, enabled}]}]
    """
    # By category
    categories: dict[str, list[dict]] = {"read": [], "write": [], "delete": []}
    # Reverse lookup: tool → module display name
    tool_to_module: dict[str, str] = {}
    for mod_name, tool_names in MODULE_GROUPS.items():
        for t in tool_names:
            tool_to_module[t] = mod_name

    for tool_name, cat in TOOL_CATEGORIES.items():
        entry = {
            "name": tool_name,
            "module": tool_to_module.get(tool_name, ""),
            "enabled": permissions.get(tool_name, True),
        }
        categories[cat.value].append(entry)

    # By module
    modules = []
    for mod_name, tool_names in MODULE_GROUPS.items():
        tools = []
        for t in tool_names:
            cat = TOOL_CATEGORIES.get(t, ToolCategory.READ)
            tools.append({
                "name": t,
                "category": cat.value,
                "enabled": permissions.get(t, True),
            })
        modules.append({"name": mod_name, "display_name": mod_name, "tools": tools})

    return {
        "tool_permission_categories": categories,
        "tool_permission_modules": modules,
    }
