"""Shared tool-permission metadata primitives (#245).

Lives in a tiny dependency-free module so tool modules can declare their
``TOOL_GROUPS`` metadata without importing permissions.py (which would be a
cycle: permissions derives its mappings FROM the tool modules) and without
pulling the heavy SDK imports the tool modules themselves need.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ToolCategory(str, Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"


# Tools whose result payload IS a secret (e.g. a Telegram StringSession granting
# full account access, #828). Generic backend loggers must redact these results
# instead of previewing them — the tool body itself never logs, but the framework
# around it (ReAct fallback, streaming summaries) must not leak the value either.
# Kept here in the dependency-free module so loggers can consult it without
# importing the heavy tool/SDK modules.
SECRET_RESULT_TOOLS: frozenset[str] = frozenset({"export_session"})

# Valid flood categories for the agent tool floodgate boundary (#1851) — the
# sliding-window buckets ``TelegramRateLimitGate`` configures by default (the
# pool gate is built without ``category_limits`` overrides beyond history, so
# these are exactly the buckets a tool-level peek can consult). "resolve" and
# "reaction" are deliberately absent: the package registers them as no-op
# categories without buckets, so mapping a tool there would silently disable
# its pacing.
FLOOD_TOOL_CATEGORIES: frozenset[str] = frozenset({
    "send",
    "admin_action",
    "channel_lifecycle",
    "history",
    "dialogs",
    "dialog_sweep",
    "dialogs_page",
    "default",
})


class ToolAccessState(str, Enum):
    ALLOWED = "allowed"
    REQUESTABLE = "requestable"
    DENIED = "denied"


@dataclass(frozen=True)
class ToolMeta:
    """Per-tool permission metadata declared next to the tool's module.

    ``phone_bound`` marks tools that bind to a specific Telegram account and
    route through ``require_phone_permission`` / ``prepare_telegram_tool`` —
    the tri-state access policy treats them fail-closed for absent ACL phones.

    ``flood_category`` names the agent tool floodgate boundary bucket the
    tool's Telegram work is paced in (#1851); empty string (the default)
    means the permissive ``default`` bucket. Validated at declaration so a
    typo fails at import, not as a silent no-op at dispatch.
    """

    category: ToolCategory
    phone_bound: bool = False
    flood_category: str = ""

    def __post_init__(self) -> None:
        if self.flood_category and self.flood_category not in FLOOD_TOOL_CATEGORIES:
            raise ValueError(
                f"flood_category {self.flood_category!r} is not one of "
                f"{sorted(FLOOD_TOOL_CATEGORIES)}"
            )


# Registration order — single source of truth consumed by BOTH
# build_agent_tool_registry (tools/__init__.py) and the permissions
# derivation, so the settings-UI group order always matches registration.
TOOL_MODULE_ORDER: tuple[str, ...] = (
    "search",
    "channels",
    "collection",
    "pipelines",
    "moderation",
    "search_queries",
    "accounts",
    "filters",
    "analytics",
    "scheduler",
    "notifications",
    "photo_loader",
    "dialogs",
    "messaging",
    "dm",
    "images",
    "export",
    "settings",
)
