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
    """

    category: ToolCategory
    phone_bound: bool = False


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
    "images",
    "settings",
    "agent_threads",
)
