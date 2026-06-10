"""Auto-derivation invariants for agent tool permissions (#245).

TOOL_CATEGORIES / MODULE_GROUPS / PHONE_BINDED_TOOLS are no longer hand-edited
dicts in permissions.py — they are derived from module-level ``TOOL_GROUPS``
declarations that live next to the tool definitions. These tests pin:

1. the migration produced byte-identical mappings (golden snapshot; the
   snapshot file is a #245 migration guard — when a NEW tool is added later,
   regenerate it together with the TOOL_GROUPS entry);
2. every ``@tool("name", …)`` declaration in src/agent/tools is classified,
   and no stale classification survives a tool's removal;
3. group membership has no duplicates and matches the categories mapping.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from src.agent.tools._categories import TOOL_MODULE_ORDER
from src.agent.tools.permissions import (
    BUILTIN_TOOLS,
    MODULE_GROUPS,
    PHONE_BINDED_TOOLS,
    TOOL_CATEGORIES,
)

_SNAPSHOT_PATH = Path(__file__).parent / "data" / "tool_permissions_snapshot.json"
_TOOLS_ROOT = Path(__file__).resolve().parent.parent / "src" / "agent" / "tools"

_TOOL_DECL_RE = re.compile(r'@tool\(\s*\n?\s*"([a-zA-Z_]+)"')


def _scan_declared_tool_names() -> set[str]:
    found: set[str] = set()
    for path in _TOOLS_ROOT.rglob("*.py"):
        for match in _TOOL_DECL_RE.finditer(path.read_text(encoding="utf-8")):
            found.add(match.group(1))
    return found


def test_derived_mappings_match_golden_snapshot():
    """Migration guard for #245: the derived dicts must be identical to the
    snapshot of the last reviewed state. When adding a new tool, update its
    module's TOOL_GROUPS and regenerate this snapshot in the same commit."""
    snap = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    assert {k: v.value for k, v in TOOL_CATEGORIES.items()} == snap["tool_categories"]
    assert {k: list(v) for k, v in MODULE_GROUPS.items()} == snap["module_groups"]
    assert sorted(PHONE_BINDED_TOOLS) == snap["phone_binded_tools"]


def test_module_groups_preserve_registration_order():
    snap = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    assert list(MODULE_GROUPS.keys()) == list(snap["module_groups"].keys())


def test_every_declared_tool_is_classified():
    """Every @tool("name", …) in src/agent/tools must appear in the derived
    TOOL_CATEGORIES — otherwise the fail-closed ACL silently locks it out."""
    declared = _scan_declared_tool_names()
    missing = declared - set(TOOL_CATEGORIES)
    assert not missing, (
        f"Tools declared via @tool() but missing from TOOL_GROUPS metadata: "
        f"{sorted(missing)}. Add them to the TOOL_GROUPS dict of their module."
    )


def test_no_stale_classifications():
    """Every classified tool (except builtins) must still be declared somewhere
    — stale metadata would advertise a tool the registry no longer builds."""
    declared = _scan_declared_tool_names()
    stale = set(TOOL_CATEGORIES) - declared - set(BUILTIN_TOOLS)
    assert not stale, (
        f"TOOL_GROUPS metadata lists tools with no @tool() declaration left: "
        f"{sorted(stale)}. Remove them from their module's TOOL_GROUPS."
    )


def test_group_membership_matches_categories_exactly():
    seen: list[str] = []
    for tools in MODULE_GROUPS.values():
        seen.extend(tools)
    assert len(seen) == len(set(seen)), "a tool appears in more than one group"
    assert set(seen) == set(TOOL_CATEGORIES), (
        "MODULE_GROUPS membership and TOOL_CATEGORIES keys diverged"
    )


def test_tool_module_order_matches_registry_loop():
    """The registry loop in tools/__init__.py must consume the same order
    constant the permissions derivation uses — a hardcoded duplicate list
    would silently desync group ordering in the settings UI."""
    init_src = (_TOOLS_ROOT / "__init__.py").read_text(encoding="utf-8")
    assert "TOOL_MODULE_ORDER" in init_src
    # Every module named in the order constant must exist as a file.
    for module_name in TOOL_MODULE_ORDER:
        assert (_TOOLS_ROOT / f"{module_name}.py").exists(), module_name


def test_lazy_attributes_are_importable_directly():
    """PEP 562 module __getattr__ must serve the legacy attribute names."""
    import src.agent.tools.permissions as permissions

    assert permissions.TOOL_CATEGORIES
    assert permissions.MODULE_GROUPS
    assert permissions.PHONE_BINDED_TOOLS
    assert "search_messages" in permissions.TOOL_CATEGORIES
