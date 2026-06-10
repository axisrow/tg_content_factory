"""Agent tool permission registry — classifies tools into read/write/delete categories.

Since #245 the authoritative metadata lives NEXT TO the tool definitions: each
registered tool module declares a module-level ``TOOL_GROUPS`` list of
``(display_group_name, {tool_name: ToolMeta})`` pairs. This module derives the
legacy ``TOOL_CATEGORIES`` / ``MODULE_GROUPS`` / ``PHONE_BINDED_TOOLS``
attributes from those declarations lazily (PEP 562 module ``__getattr__``), so
all existing importers keep working unchanged while adding a tool now means
editing exactly one file.
"""

from __future__ import annotations

import importlib
import json
import logging
import time
from collections import OrderedDict
from inspect import isawaitable

from src.agent.tools._categories import (
    TOOL_MODULE_ORDER,
    ToolAccessState,
    ToolCategory,
    ToolMeta,
)

__all__ = [  # noqa: F822 — TOOL_CATEGORIES & co. are served by module __getattr__
    "TOOL_CATEGORIES",
    "MODULE_GROUPS",
    "PHONE_BINDED_TOOLS",
    "ToolAccessState",
    "ToolCategory",
    "ToolMeta",
]

logger = logging.getLogger(__name__)

_PERMISSIONS_CACHE_TTL = 60.0  # seconds
_permissions_cache: tuple[dict[str, bool], float] | None = None
_access_policy_cache: tuple[dict[str, ToolAccessState], float] | None = None

TOOL_PERMISSIONS_SETTING = "agent_tool_permissions"
MCP_PREFIX = "mcp__telegram_db__"
BUILTIN_TOOLS = ["WebSearch", "WebFetch"]
_BUILTIN_GROUP_NAME = "Веб-поиск"


# ---------------------------------------------------------------------------
# Lazy derivation from per-module TOOL_GROUPS declarations.
# ---------------------------------------------------------------------------

_metadata_cache: tuple[dict[str, ToolCategory], OrderedDict[str, list[str]], frozenset[str]] | None = None


def _build_metadata() -> tuple[dict[str, ToolCategory], OrderedDict[str, list[str]], frozenset[str]]:
    global _metadata_cache  # noqa: PLW0603
    if _metadata_cache is not None:
        return _metadata_cache

    categories: dict[str, ToolCategory] = {}
    groups: OrderedDict[str, list[str]] = OrderedDict()
    phone_bound: set[str] = set()

    for module_name in TOOL_MODULE_ORDER:
        module = importlib.import_module(f"src.agent.tools.{module_name}")
        tool_groups = getattr(module, "TOOL_GROUPS", None)
        if not tool_groups:
            raise RuntimeError(
                f"src.agent.tools.{module_name} does not declare TOOL_GROUPS — "
                "every registered tool module must classify its tools (#245)"
            )
        for group_name, tools in tool_groups:
            bucket = groups.setdefault(group_name, [])
            for tool_name, meta in tools.items():
                if tool_name in categories:
                    raise RuntimeError(
                        f"Tool '{tool_name}' classified twice "
                        f"(second time in {module_name}.TOOL_GROUPS)"
                    )
                categories[tool_name] = meta.category
                bucket.append(tool_name)
                if meta.phone_bound:
                    phone_bound.add(tool_name)

    for builtin in BUILTIN_TOOLS:
        categories[builtin] = ToolCategory.READ
    groups[_BUILTIN_GROUP_NAME] = list(BUILTIN_TOOLS)

    _metadata_cache = (categories, groups, frozenset(phone_bound))
    return _metadata_cache


def _tool_categories() -> dict[str, ToolCategory]:
    return _build_metadata()[0]


def _module_groups() -> OrderedDict[str, list[str]]:
    return _build_metadata()[1]


def _phone_binded_tools() -> frozenset[str]:
    return _build_metadata()[2]


def __getattr__(name: str):
    # PEP 562: keep the historical module attributes working for all existing
    # importers while building them lazily (tool modules pull the agent SDK —
    # CLI paths that import permissions for constants must not pay for that
    # until the mappings are actually needed).
    if name == "TOOL_CATEGORIES":
        return _tool_categories()
    if name == "MODULE_GROUPS":
        return _module_groups()
    if name == "PHONE_BINDED_TOOLS":
        return _phone_binded_tools()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
            name: (cat == ToolCategory.READ and name not in _phone_binded_tools())
            for name, cat in _tool_categories().items()
        }
    return {name: True for name in _tool_categories()}


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
        result = {name: ToolAccessState.DENIED for name in _tool_categories()}
    elif not saved:
        result = {name: ToolAccessState.ALLOWED for name in _tool_categories()}
    elif not _is_per_phone_format(saved):
        result = {
            name: _state_from_saved_value(saved.get(name))
            for name in _tool_categories()
        }
    else:
        phone_dicts = [v for v in saved.values() if isinstance(v, dict)]
        if not phone_dicts:
            result = {name: ToolAccessState.REQUESTABLE for name in _tool_categories()}
        else:
            result = {
                name: _merge_access_states([
                    _state_from_saved_value(phone_perms.get(name))
                    for phone_perms in phone_dicts
                ])
                for name in _tool_categories()
            }

    _access_policy_cache = (result, time.monotonic() + _PERMISSIONS_CACHE_TTL)
    return result


async def get_tool_access_state(db, tool_name: str, *, phone: str = "") -> ToolAccessState:
    """Return access state for one tool, optionally scoped to a phone."""
    if tool_name not in _tool_categories():
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
            return {name: False for name in _tool_categories()}
        result = {name: phone_perms.get(name, missing_defaults[name]) for name in _tool_categories()}
    else:
        phone_used = "(flat/legacy)"
        result = {name: saved.get(name, missing_defaults[name]) for name in _tool_categories()}

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
    all_denied = {name: False for name in _tool_categories()}
    saved = await _load_raw_permissions(db)

    result = {}
    saved_is_per_phone = _is_per_phone_format(saved)
    for acc in accounts:
        if saved_is_per_phone and acc.phone in saved:
            phone_perms = saved[acc.phone]
            result[acc.phone] = {name: phone_perms.get(name, missing_defaults[name]) for name in _tool_categories()}
        elif saved_is_per_phone:
            # Per-phone ACL exists but this account is absent — render the
            # settings UI fully fail-closed.  Admin must explicitly opt-in
            # any tool per account; otherwise saving this tab would silently
            # grant pre-checked READ defaults.
            result[acc.phone] = dict(all_denied)
        elif saved:
            # Legacy flat → apply to all phones
            result[acc.phone] = {name: saved.get(name, missing_defaults[name]) for name in _tool_categories()}
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
    """Build the full list of tool names from _tool_categories().

    MCP tools get the prefix; built-in tools use bare names.
    Result is computed once and cached (_tool_categories() is static).
    """
    global _all_allowed_tools_cache  # noqa: PLW0603
    if _all_allowed_tools_cache is not None:
        return _all_allowed_tools_cache
    result = []
    for name in _tool_categories():
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
    for mod_name, tool_names in _module_groups().items():
        for t in tool_names:
            tool_to_module[t] = mod_name

    for tool_name, cat in _tool_categories().items():
        entry = {
            "name": tool_name,
            "module": tool_to_module.get(tool_name, ""),
            "enabled": permissions.get(tool_name, False),
        }
        categories[cat.value].append(entry)

    # By module
    modules = []
    for mod_name, tool_names in _module_groups().items():
        tools = []
        for t in tool_names:
            cat = _tool_categories().get(t, ToolCategory.READ)
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
