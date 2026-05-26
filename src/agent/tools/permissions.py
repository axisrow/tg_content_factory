"""Agent tool permission registry — classifies tools into read/write/delete categories."""

from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from enum import Enum
from inspect import isawaitable

logger = logging.getLogger(__name__)

_PERMISSIONS_CACHE_TTL = 60.0  # seconds
_permissions_cache: tuple[dict[str, bool], float] | None = None
_access_policy_cache: tuple[dict[str, "ToolAccessState"], float] | None = None

TOOL_PERMISSIONS_SETTING = "agent_tool_permissions"
MCP_PREFIX = "mcp__telegram_db__"
BUILTIN_TOOLS = ["WebSearch", "WebFetch"]

# Tools that bind to a specific Telegram account and route through
# require_phone_permission (directly or via prepare_telegram_tool). For these
# tools tri-state access policy treats explicit grants/denies separately from
# missing ACL entries. Missing phone-bound entries are requestable in
# interactive agent sessions and blocked in unattended contexts.
#
# Kept in sync with the static scan in
# `tests/test_tool_permissions.py::test_every_phone_binded_tool_is_registered_in_tool_categories`.
PHONE_BINDED_TOOLS: frozenset[str] = frozenset({
    "archive_chat",
    "clear_dialog_cache",
    "create_auto_upload",
    "create_photo_batch",
    "create_telegram_channel",
    "delete_message",
    "download_media",
    "edit_admin",
    "edit_message",
    "edit_permissions",
    "forward_messages",
    "get_broadcast_stats",
    "get_participants",
    "kick_participant",
    "leave_dialogs",
    "list_photo_dialogs",
    "mark_read",
    "pin_message",
    "read_messages",
    "refresh_dialogs",
    "refresh_photo_dialogs",
    "resolve_entity",
    "schedule_photos",
    "search_dialogs",
    "send_message",
    "send_photos_now",
    "send_reaction",
    "unarchive_chat",
    "unpin_message",
})


async def _load_account_records(db) -> list[object]:
    for getter_name in ("get_account_summaries", "get_accounts"):
        getter = getattr(db, getter_name, None)
        if not callable(getter):
            continue
        result = getter()
        if isawaitable(result):
            result = await result
        if isinstance(result, (list, tuple)):
            return list(result)
    return []


class ToolCategory(str, Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"


class ToolAccessState(str, Enum):
    ALLOWED = "allowed"
    REQUESTABLE = "requestable"
    DENIED = "denied"


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
    "list_tags": ToolCategory.READ,
    "create_tag": ToolCategory.WRITE,
    "delete_tag": ToolCategory.DELETE,
    "set_channel_tags": ToolCategory.WRITE,
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
    "get_refinement_steps": ToolCategory.READ,
    "set_refinement_steps": ToolCategory.WRITE,
    "export_pipeline_json": ToolCategory.READ,
    "import_pipeline_json": ToolCategory.WRITE,
    "list_pipeline_templates": ToolCategory.READ,
    "create_pipeline_from_template": ToolCategory.WRITE,
    "ai_edit_pipeline": ToolCategory.WRITE,
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
    # Dialogs
    "search_dialogs": ToolCategory.READ,
    "refresh_dialogs": ToolCategory.WRITE,
    "leave_dialogs": ToolCategory.DELETE,
    "create_telegram_channel": ToolCategory.WRITE,
    "get_forum_topics": ToolCategory.READ,
    "clear_dialog_cache": ToolCategory.WRITE,
    "get_cache_status": ToolCategory.READ,
    "resolve_entity": ToolCategory.READ,
    # Messaging
    "send_message": ToolCategory.WRITE,
    "send_reaction": ToolCategory.WRITE,
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
    # Built-in Claude tools (no MCP prefix)
    "WebSearch": ToolCategory.READ,
    "WebFetch": ToolCategory.READ,
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
        "list_tags", "create_tag", "delete_tag", "set_channel_tags",
    ]),
    ("Сбор", [
        "collect_channel", "collect_all_channels", "collect_channel_stats", "collect_all_stats",
    ]),
    ("Пайплайны", [
        "list_pipelines", "get_pipeline_detail", "add_pipeline", "edit_pipeline",
        "toggle_pipeline", "delete_pipeline", "run_pipeline", "generate_draft",
        "list_pipeline_runs", "get_pipeline_run", "publish_pipeline_run",
        "get_pipeline_queue", "get_refinement_steps", "set_refinement_steps",
        "export_pipeline_json", "import_pipeline_json", "list_pipeline_templates",
        "create_pipeline_from_template", "ai_edit_pipeline",
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
    ("Диалоги", [
        "search_dialogs", "refresh_dialogs", "leave_dialogs", "create_telegram_channel",
        "get_forum_topics", "clear_dialog_cache", "get_cache_status", "resolve_entity",
    ]),
    ("Сообщения", [
        "send_message", "send_reaction", "forward_messages", "edit_message", "delete_message",
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
    ("Веб-поиск", ["WebSearch", "WebFetch"]),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_permissions(*, for_missing_in_saved: bool = False) -> dict[str, bool]:
    """Default permissions.

    Without flags: all tools enabled — used for clean installs (no saved ACL)
    and when seeding new accounts that have never had explicit permissions.

    With ``for_missing_in_saved=True``: READ tools default enabled, WRITE/DELETE
    default disabled, AND phone-bound tools (regardless of category) default
    disabled.  Used to fill in tools missing from a saved ACL so:

    * newly introduced WRITE/DELETE actions cannot bypass an existing
      restrictive configuration (Codex round 4);
    * phone-bound READ actions (read_messages, download_media,
      get_participants, resolve_entity, …) cannot be granted by default
      when an admin opens and saves an existing per-phone tab — they all
      touch the live Telegram client and must require explicit per-phone
      opt-in (Codex round 10).

    Non-phone-bound DB-only READ tools (list_channels, search_messages,
    analytics, settings) keep the permissive True default so opening
    settings does not lock admins out of read-only DB work they already had.
    """
    if for_missing_in_saved:
        return {
            name: (cat == ToolCategory.READ and name not in PHONE_BINDED_TOOLS)
            for name, cat in TOOL_CATEGORIES.items()
        }
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


async def _load_raw_permissions_strict(db) -> tuple[dict, bool]:
    """Load raw permissions and preserve corruption as a fail-closed signal."""
    raw = await db.get_setting(TOOL_PERMISSIONS_SETTING)
    if not raw:
        return {}, False
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupted tool permissions setting, using fail-closed policy")
        return {}, True
    if not isinstance(data, dict):
        logger.warning("Tool permissions setting is not an object, using fail-closed policy")
        return {}, True
    return data, False


def _state_from_saved_value(value: object) -> ToolAccessState:
    if value is True:
        return ToolAccessState.ALLOWED
    if value is False:
        return ToolAccessState.DENIED
    return ToolAccessState.REQUESTABLE


def _merge_access_states(states: list[ToolAccessState]) -> ToolAccessState:
    if ToolAccessState.ALLOWED in states:
        return ToolAccessState.ALLOWED
    if ToolAccessState.REQUESTABLE in states:
        return ToolAccessState.REQUESTABLE
    return ToolAccessState.DENIED


async def load_tool_access_policy(db, *, use_cache: bool = False) -> dict[str, ToolAccessState]:
    """Return aggregate tool access states for model visibility decisions.

    ``ALLOWED`` means at least one configured scope explicitly allows the tool.
    ``REQUESTABLE`` means the tool is not explicitly allowed or denied and may
    ask through PermissionGate in an interactive request.
    ``DENIED`` means every configured scope explicitly denies the tool, or the
    stored ACL is malformed.
    """
    global _access_policy_cache  # noqa: PLW0603
    if use_cache and _access_policy_cache is not None:
        cached_value, expires = _access_policy_cache
        if time.monotonic() < expires:
            return cached_value

    saved, malformed = await _load_raw_permissions_strict(db)
    if malformed:
        result = {name: ToolAccessState.DENIED for name in TOOL_CATEGORIES}
    elif not saved:
        result = {name: ToolAccessState.ALLOWED for name in TOOL_CATEGORIES}
    elif not _is_per_phone_format(saved):
        result = {
            name: _state_from_saved_value(saved.get(name))
            for name in TOOL_CATEGORIES
        }
    else:
        phone_dicts = [v for v in saved.values() if isinstance(v, dict)]
        if not phone_dicts:
            result = {name: ToolAccessState.REQUESTABLE for name in TOOL_CATEGORIES}
        else:
            result = {
                name: _merge_access_states([
                    _state_from_saved_value(phone_perms.get(name))
                    for phone_perms in phone_dicts
                ])
                for name in TOOL_CATEGORIES
            }

    _access_policy_cache = (result, time.monotonic() + _PERMISSIONS_CACHE_TTL)
    return result


async def get_tool_access_state(db, tool_name: str, *, phone: str = "") -> ToolAccessState:
    """Return access state for one tool, optionally scoped to a phone."""
    if tool_name not in TOOL_CATEGORIES:
        return ToolAccessState.DENIED
    try:
        saved, malformed = await _load_raw_permissions_strict(db)
    except Exception:
        logger.warning("Failed to load agent tool permissions; blocking '%s'", tool_name, exc_info=True)
        return ToolAccessState.DENIED
    if malformed:
        return ToolAccessState.DENIED
    if not saved:
        return ToolAccessState.ALLOWED
    if not _is_per_phone_format(saved):
        return _state_from_saved_value(saved.get(tool_name))
    if not phone:
        policy = await load_tool_access_policy(db)
        return policy.get(tool_name, ToolAccessState.DENIED)
    phone_perms = saved.get(phone)
    if not isinstance(phone_perms, dict):
        return ToolAccessState.REQUESTABLE
    return _state_from_saved_value(phone_perms.get(tool_name))


async def get_explicit_allowed_phones(db, tool_name: str) -> list[str]:
    """Return phones with an explicit True grant for the tool."""
    try:
        saved, malformed = await _load_raw_permissions_strict(db)
    except Exception:
        logger.warning("Failed to load agent tool permissions; cannot list allowed phones", exc_info=True)
        return []
    if malformed or not saved or not _is_per_phone_format(saved):
        return []
    return sorted(
        phone
        for phone, phone_perms in saved.items()
        if isinstance(phone_perms, dict) and phone_perms.get(tool_name) is True
    )


async def load_tool_permissions(db, phone: str | None = None) -> dict[str, bool]:
    """Load per-tool permissions from DB for a specific phone.

    If *phone* is ``None``, loads permissions for the primary account.
    Supports both legacy flat format and per-phone format.
    Missing setting → all-enabled defaults.
    """
    defaults = _default_permissions()
    missing_defaults = _default_permissions(for_missing_in_saved=True)
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
        phone_known = False
        if phone and phone in saved:
            phone_perms = saved[phone]
            phone_known = True
        else:
            if phone is None:
                accounts = await _load_account_records(db)
                if accounts:
                    primary = next((a for a in accounts if a.is_primary), accounts[0])
                    phone_used = primary.phone
                    if primary.phone in saved:
                        phone_perms = saved[primary.phone]
                        phone_known = True
                    else:
                        phone_perms = {}
                else:
                    phone_perms = {}
            else:
                phone_perms = {}
        if not phone_known:
            # Per-phone ACL exists but this phone is absent — fully fail-closed.
            # Matches require_phone_permission's deny-for-absent semantics and
            # avoids exposing live Telegram READ tools through an unauthorized
            # account.
            logger.debug(
                "Tool permissions: phone=%s not in saved per-phone ACL, denying everything",
                phone_used,
            )
            return {name: False for name in TOOL_CATEGORIES}
        result = {name: phone_perms.get(name, missing_defaults[name]) for name in TOOL_CATEGORIES}
    else:
        phone_used = "(flat/legacy)"
        result = {name: saved.get(name, missing_defaults[name]) for name in TOOL_CATEGORIES}

    enabled = sum(1 for v in result.values() if v)
    disabled = sum(1 for v in result.values() if not v)
    logger.debug("Tool permissions for %s: %d enabled, %d disabled", phone_used, enabled, disabled)
    return result


async def load_tool_permissions_all_phones(db, accounts) -> dict[str, dict[str, bool]]:
    """Load permissions for every account phone.  Returns ``{phone: {tool: bool}}``."""
    defaults = _default_permissions()
    missing_defaults = _default_permissions(for_missing_in_saved=True)
    # All-False template for accounts absent from a per-phone ACL.  Matches the
    # behaviour of require_phone_permission (deny for absent phones) so the
    # settings UI never shows pre-checked grants for an account the admin has
    # not explicitly authorized.
    all_denied = {name: False for name in TOOL_CATEGORIES}
    saved = await _load_raw_permissions(db)

    result = {}
    saved_is_per_phone = _is_per_phone_format(saved)
    for acc in accounts:
        if saved_is_per_phone and acc.phone in saved:
            phone_perms = saved[acc.phone]
            result[acc.phone] = {name: phone_perms.get(name, missing_defaults[name]) for name in TOOL_CATEGORIES}
        elif saved_is_per_phone:
            # Per-phone ACL exists but this account is absent — render the
            # settings UI fully fail-closed.  Admin must explicitly opt-in
            # any tool per account; otherwise saving this tab would silently
            # grant pre-checked READ defaults.
            result[acc.phone] = dict(all_denied)
        elif saved:
            # Legacy flat → apply to all phones
            result[acc.phone] = {name: saved.get(name, missing_defaults[name]) for name in TOOL_CATEGORIES}
        else:
            result[acc.phone] = dict(defaults)
    return result


async def save_tool_permissions(db, permissions: dict[str, bool], phone: str | None = None) -> None:
    """Persist per-tool permissions as JSON.

    If *phone* is given, saves under the per-phone key without touching other phones.
    If *phone* is ``None``, saves in legacy flat format (backward compat).

    Phones absent from the saved ACL are NOT materialized when another phone is
    saved.  Materializing them would re-open the absent-phone leak through the
    save path: a previously-denied phone would become present in saved with the
    seeded READ defaults, which then short-circuits require_phone_permission's
    absent-phone deny.  Admin must explicitly open each account's tab in the
    settings UI and save it to grant any access.
    """
    global _permissions_cache, _access_policy_cache  # noqa: PLW0603
    _permissions_cache = None  # invalidate cache on save
    _access_policy_cache = None

    if phone is None:
        await db.set_setting(TOOL_PERMISSIONS_SETTING, json.dumps(permissions, ensure_ascii=False))
        return

    saved = await _load_raw_permissions(db)
    if saved and not _is_per_phone_format(saved):
        # Discard the legacy flat ACL on the first per-phone save: every
        # phone starts fail-closed (load_tool_permissions returns all-False
        # for absent phones), so an admin upgrading from flat to per-phone
        # must explicitly grant each phone via the Settings UI. The old
        # flat overrides are NOT carried over to the current phone — that
        # would silently retain broader access than the admin saw on screen.
        saved = {}
    saved[phone] = permissions
    await db.set_setting(TOOL_PERMISSIONS_SETTING, json.dumps(saved, ensure_ascii=False))


async def load_tool_permissions_union(db, *, use_cache: bool = False) -> dict[str, bool]:
    """Compatibility boolean view: True only for explicitly allowed tools.

    New code should use ``load_tool_access_policy`` plus ``visible_tools_for_llm``
    so requestable tools can remain visible in interactive agent sessions.
    """
    global _permissions_cache  # noqa: PLW0603
    if use_cache and _permissions_cache is not None:
        cached_value, expires = _permissions_cache
        if time.monotonic() < expires:
            return cached_value

    policy = await load_tool_access_policy(db, use_cache=False)
    result = {name: state == ToolAccessState.ALLOWED for name, state in policy.items()}
    _permissions_cache = (result, time.monotonic() + _PERMISSIONS_CACHE_TTL)
    return result


_all_allowed_tools_cache: list[str] | None = None


def get_all_allowed_tools() -> list[str]:
    """Build the full list of tool names from TOOL_CATEGORIES.

    MCP tools get the prefix; built-in tools use bare names.
    Result is computed once and cached (TOOL_CATEGORIES is static).
    """
    global _all_allowed_tools_cache  # noqa: PLW0603
    if _all_allowed_tools_cache is not None:
        return _all_allowed_tools_cache
    result = []
    for name in TOOL_CATEGORIES:
        if name in BUILTIN_TOOLS:
            result.append(name)
        else:
            result.append(f"{MCP_PREFIX}{name}")
    _all_allowed_tools_cache = result
    return result


def _bare_tool_name(tool_name: str) -> str:
    return tool_name.removeprefix(MCP_PREFIX) if tool_name.startswith(MCP_PREFIX) else tool_name


def is_tool_visible_for_llm(
    tool_name: str,
    access_policy: dict[str, ToolAccessState],
    *,
    gate_active: bool,
) -> bool:
    """Return whether a tool should be advertised to the model."""
    state = access_policy.get(_bare_tool_name(tool_name), ToolAccessState.DENIED)
    if state == ToolAccessState.ALLOWED:
        return True
    if state == ToolAccessState.REQUESTABLE:
        return gate_active
    return False


def visible_tools_for_llm(
    all_tools: list[str],
    access_policy: dict[str, ToolAccessState],
    *,
    gate_active: bool,
) -> list[str]:
    """Filter tool names for model visibility using tri-state access policy."""
    return [
        tool_name
        for tool_name in all_tools
        if is_tool_visible_for_llm(tool_name, access_policy, gate_active=gate_active)
    ]


def filter_allowed_tools(all_tools: list[str], permissions: dict[str, bool]) -> list[str]:
    """Filter tool names by permissions.

    Handles both MCP-prefixed and bare built-in tool names.
    Unknown tools (not in permissions dict) are denied by default.
    """
    result = []
    for tool_name in all_tools:
        bare = tool_name.removeprefix(MCP_PREFIX) if tool_name.startswith(MCP_PREFIX) else tool_name
        if permissions.get(bare, False):
            result.append(tool_name)
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
