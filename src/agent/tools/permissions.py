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
# Authoritative tool → category mapping.  get_all_allowed_tools() derives the
# MCP-prefixed allow-list from this dict.  Bare name is used as the key.
# ---------------------------------------------------------------------------

TOOL_CATEGORIES: dict[str, ToolCategory] = {
    # Search
    "search_messages": ToolCategory.READ,
    "semantic_search": ToolCategory.READ,
    "index_messages": ToolCategory.WRITE,
    "search_telegram": ToolCategory.READ,
    "search_my_chats": ToolCategory.READ,
    "search_in_channel": ToolCategory.READ,
    "search_hybrid": ToolCategory.READ,
    # Channels
    "list_channels": ToolCategory.READ,
    "get_channel_stats": ToolCategory.READ,
    "add_channel": ToolCategory.WRITE,
    "delete_channel": ToolCategory.DELETE,
    "toggle_channel": ToolCategory.WRITE,
    "import_channels": ToolCategory.WRITE,
    "refresh_channel_types": ToolCategory.WRITE,
    "refresh_channel_meta": ToolCategory.WRITE,
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
    # Moderation
    "list_pending_moderation": ToolCategory.READ,
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
    "get_search_query_stats": ToolCategory.READ,
    # Accounts
    "list_accounts": ToolCategory.READ,
    "toggle_account": ToolCategory.WRITE,
    "delete_account": ToolCategory.DELETE,
    "get_flood_status": ToolCategory.READ,
    "clear_flood_status": ToolCategory.WRITE,
    "get_account_info": ToolCategory.READ,
    # Filters
    "analyze_filters": ToolCategory.READ,
    "apply_filters": ToolCategory.WRITE,
    "reset_filters": ToolCategory.WRITE,
    "toggle_channel_filter": ToolCategory.WRITE,
    "purge_filtered_channels": ToolCategory.DELETE,
    "hard_delete_channels": ToolCategory.DELETE,
    "precheck_filters": ToolCategory.WRITE,
    # Analytics
    "get_analytics_summary": ToolCategory.READ,
    "get_pipeline_stats": ToolCategory.READ,
    "get_daily_stats": ToolCategory.READ,
    "get_trending_topics": ToolCategory.READ,
    "get_trending_channels": ToolCategory.READ,
    "get_message_velocity": ToolCategory.READ,
    "get_peak_hours": ToolCategory.READ,
    "get_calendar": ToolCategory.READ,
    "get_top_messages": ToolCategory.READ,
    "get_content_type_stats": ToolCategory.READ,
    "get_hourly_activity": ToolCategory.READ,
    # Scheduler
    "get_scheduler_status": ToolCategory.READ,
    "start_scheduler": ToolCategory.WRITE,
    "stop_scheduler": ToolCategory.WRITE,
    "trigger_collection": ToolCategory.WRITE,
    "toggle_scheduler_job": ToolCategory.WRITE,
    "set_scheduler_interval": ToolCategory.WRITE,
    "cancel_scheduler_task": ToolCategory.WRITE,
    "clear_pending_tasks": ToolCategory.WRITE,
    # Notifications
    "get_notification_status": ToolCategory.READ,
    "setup_notification_bot": ToolCategory.WRITE,
    "delete_notification_bot": ToolCategory.DELETE,
    "test_notification": ToolCategory.WRITE,
    "notification_dry_run": ToolCategory.READ,
    # Photo Loader
    "list_photo_batches": ToolCategory.READ,
    "list_photo_items": ToolCategory.READ,
    "send_photos_now": ToolCategory.WRITE,
    "schedule_photos": ToolCategory.WRITE,
    "cancel_photo_item": ToolCategory.WRITE,
    "list_auto_uploads": ToolCategory.READ,
    "toggle_auto_upload": ToolCategory.WRITE,
    "delete_auto_upload": ToolCategory.DELETE,
    "create_photo_batch": ToolCategory.WRITE,
    "run_photo_due": ToolCategory.WRITE,
    "create_auto_upload": ToolCategory.WRITE,
    "update_auto_upload": ToolCategory.WRITE,
    "list_photo_dialogs": ToolCategory.READ,
    "refresh_photo_dialogs": ToolCategory.WRITE,
    # My Telegram
    "search_my_telegram": ToolCategory.READ,
    "refresh_dialogs": ToolCategory.WRITE,
    "leave_dialogs": ToolCategory.DELETE,
    "create_telegram_channel": ToolCategory.WRITE,
    "get_forum_topics": ToolCategory.READ,
    "clear_dialog_cache": ToolCategory.WRITE,
    "get_cache_status": ToolCategory.READ,
    # Messaging
    "send_message": ToolCategory.WRITE,
    "forward_messages": ToolCategory.WRITE,
    "edit_message": ToolCategory.WRITE,
    "delete_message": ToolCategory.DELETE,
    "pin_message": ToolCategory.WRITE,
    "unpin_message": ToolCategory.WRITE,
    "download_media": ToolCategory.READ,
    "get_participants": ToolCategory.READ,
    "edit_admin": ToolCategory.WRITE,
    "edit_permissions": ToolCategory.WRITE,
    "kick_participant": ToolCategory.DELETE,
    "get_broadcast_stats": ToolCategory.READ,
    "archive_chat": ToolCategory.WRITE,
    "unarchive_chat": ToolCategory.WRITE,
    "mark_read": ToolCategory.WRITE,
    "read_messages": ToolCategory.READ,
    # Images
    "generate_image": ToolCategory.WRITE,
    "list_image_models": ToolCategory.READ,
    "list_image_providers": ToolCategory.READ,
    "list_generated_images": ToolCategory.READ,
    # Settings
    "get_settings": ToolCategory.READ,
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
        "search_telegram", "search_my_chats", "search_in_channel", "search_hybrid",
    ]),
    ("Каналы", [
        "list_channels", "get_channel_stats", "add_channel", "delete_channel",
        "toggle_channel", "import_channels", "refresh_channel_types", "refresh_channel_meta",
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
        "get_search_query_stats",
    ]),
    ("Аккаунты", [
        "list_accounts", "toggle_account", "delete_account", "get_flood_status",
        "clear_flood_status", "get_account_info",
    ]),
    ("Фильтры", [
        "analyze_filters", "apply_filters", "reset_filters", "toggle_channel_filter",
        "purge_filtered_channels", "hard_delete_channels", "precheck_filters",
    ]),
    ("Аналитика", [
        "get_analytics_summary", "get_pipeline_stats", "get_daily_stats",
        "get_trending_topics", "get_trending_channels", "get_message_velocity",
        "get_peak_hours", "get_calendar",
        "get_top_messages", "get_content_type_stats", "get_hourly_activity",
    ]),
    ("Планировщик", [
        "get_scheduler_status", "start_scheduler", "stop_scheduler",
        "trigger_collection", "toggle_scheduler_job",
        "set_scheduler_interval", "cancel_scheduler_task", "clear_pending_tasks",
    ]),
    ("Уведомления", [
        "get_notification_status", "setup_notification_bot", "delete_notification_bot",
        "test_notification", "notification_dry_run",
    ]),
    ("Фото", [
        "list_photo_batches", "list_photo_items", "send_photos_now", "schedule_photos",
        "cancel_photo_item", "list_auto_uploads", "toggle_auto_upload", "delete_auto_upload",
        "create_photo_batch", "run_photo_due", "create_auto_upload", "update_auto_upload",
        "list_photo_dialogs", "refresh_photo_dialogs",
    ]),
    ("Мой Telegram", [
        "search_my_telegram", "refresh_dialogs", "leave_dialogs", "create_telegram_channel",
        "get_forum_topics", "clear_dialog_cache", "get_cache_status",
    ]),
    ("Сообщения", [
        "send_message", "forward_messages", "edit_message", "delete_message",
        "pin_message", "unpin_message", "download_media", "read_messages",
    ]),
    ("Управление чатом", [
        "get_participants", "edit_admin", "edit_permissions", "kick_participant",
        "get_broadcast_stats", "archive_chat", "unarchive_chat", "mark_read",
    ]),
    ("Изображения", [
        "list_image_models", "list_image_providers", "generate_image", "list_generated_images",
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
    """Default permissions: all tools enabled."""
    return {name: True for name in TOOL_CATEGORIES}


def _is_per_phone_format(saved: dict) -> bool:
    """Detect whether saved dict is per-phone (values are dicts) or flat (values are bools)."""
    if not saved:
        return False
    first_value = next(iter(saved.values()))
    return isinstance(first_value, dict)


async def _load_raw_permissions(db) -> dict:
    """Load raw JSON from DB setting."""
    raw = await db.get_setting(TOOL_PERMISSIONS_SETTING)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupted tool permissions setting, using defaults")
        return {}


async def load_tool_permissions(db, phone: str | None = None) -> dict[str, bool]:
    """Load per-tool permissions from DB for a specific phone.

    If *phone* is ``None``, loads permissions for the primary account.
    Supports both legacy flat format and per-phone format.
    Missing setting → all-enabled defaults.
    """
    defaults = _default_permissions()
    saved = await _load_raw_permissions(db)
    if not saved:
        logger.debug("Tool permissions: no DB setting, using defaults (all enabled)")
        return defaults

    is_per_phone = _is_per_phone_format(saved)
    logger.debug(
        "Tool permissions raw: per_phone=%s, top_keys=%s",
        is_per_phone, list(saved.keys())[:5],
    )

    if is_per_phone:
        phone_used = phone
        if phone and phone in saved:
            phone_perms = saved[phone]
        else:
            if phone is None:
                accounts = await db.get_accounts()
                if accounts:
                    primary = next((a for a in accounts if a.is_primary), accounts[0])
                    phone_used = primary.phone
                    phone_perms = saved.get(primary.phone, {})
                else:
                    phone_perms = {}
            else:
                phone_perms = {}
        if not phone_perms:
            logger.debug("Tool permissions: phone=%s not in saved, using defaults", phone_used)
            return defaults
        result = {name: phone_perms.get(name, defaults[name]) for name in TOOL_CATEGORIES}
    else:
        phone_used = "(flat/legacy)"
        result = {name: saved.get(name, defaults[name]) for name in TOOL_CATEGORIES}

    enabled = sum(1 for v in result.values() if v)
    disabled = sum(1 for v in result.values() if not v)
    logger.debug("Tool permissions for %s: %d enabled, %d disabled", phone_used, enabled, disabled)
    return result


async def load_tool_permissions_all_phones(db, accounts) -> dict[str, dict[str, bool]]:
    """Load permissions for every account phone.  Returns ``{phone: {tool: bool}}``."""
    defaults = _default_permissions()
    saved = await _load_raw_permissions(db)

    result = {}
    for acc in accounts:
        if _is_per_phone_format(saved) and acc.phone in saved:
            phone_perms = saved[acc.phone]
            result[acc.phone] = {name: phone_perms.get(name, defaults[name]) for name in TOOL_CATEGORIES}
        elif not _is_per_phone_format(saved) and saved:
            # Legacy flat → apply to all phones
            result[acc.phone] = {name: saved.get(name, defaults[name]) for name in TOOL_CATEGORIES}
        else:
            result[acc.phone] = dict(defaults)
    return result


async def save_tool_permissions(db, permissions: dict[str, bool], phone: str | None = None) -> None:
    """Persist per-tool permissions as JSON.

    If *phone* is given, saves under the per-phone key without touching other phones.
    If *phone* is ``None``, saves in legacy flat format (backward compat).
    """
    if phone is None:
        await db.set_setting(TOOL_PERMISSIONS_SETTING, json.dumps(permissions, ensure_ascii=False))
        return

    saved = await _load_raw_permissions(db)
    if saved and not _is_per_phone_format(saved):
        # Migrate legacy flat → per-phone: existing flat becomes the phone's entry
        saved = {}
    # Seed unsaved accounts with all-enabled defaults so they are not implicitly denied
    defaults = _default_permissions()
    try:
        accounts = await db.get_accounts()
        for acc in accounts:
            if acc.phone not in saved:
                saved[acc.phone] = dict(defaults)
    except Exception:
        pass  # DB error — proceed with what we have
    saved[phone] = permissions
    await db.set_setting(TOOL_PERMISSIONS_SETTING, json.dumps(saved, ensure_ascii=False))


async def load_tool_permissions_union(db) -> dict[str, bool]:
    """Union across all phones — True if ANY phone allows the tool.

    Used by agent backends so that tools visible to at least one account
    remain available for the session.  Per-phone handler gates do fine-grained blocking.
    """
    defaults = _default_permissions()
    saved = await _load_raw_permissions(db)
    if not saved:
        return defaults

    if not _is_per_phone_format(saved):
        return {name: saved.get(name, defaults[name]) for name in TOOL_CATEGORIES}

    phone_dicts = [v for v in saved.values() if isinstance(v, dict)]
    if not phone_dicts:
        return defaults
    return {name: any(pd.get(name, defaults[name]) for pd in phone_dicts) for name in TOOL_CATEGORIES}


def get_all_allowed_tools() -> list[str]:
    """Build the full list of MCP-prefixed tool names from TOOL_CATEGORIES."""
    return [f"{MCP_PREFIX}{name}" for name in TOOL_CATEGORIES]


def filter_allowed_tools(all_tools: list[str], permissions: dict[str, bool]) -> list[str]:
    """Filter MCP-prefixed tool names by permissions.

    Unknown tools (not in permissions dict) are denied by default.
    """
    result = []
    for prefixed_name in all_tools:
        bare = prefixed_name.removeprefix(MCP_PREFIX)
        if permissions.get(bare, False):
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
            "enabled": permissions.get(tool_name, False),
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
                "enabled": permissions.get(t, False),
            })
        modules.append({"name": mod_name, "display_name": mod_name, "tools": tools})

    return {
        "tool_permission_categories": categories,
        "tool_permission_modules": modules,
    }
