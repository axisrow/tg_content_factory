"""Tests for src/agent/tools/permissions.py — tool permission system."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.agent.tools.permissions import (
    BUILTIN_TOOLS,
    MCP_PREFIX,
    MODULE_GROUPS,
    TOOL_CATEGORIES,
    ToolCategory,
    _default_permissions,
    _is_per_phone_format,
    build_template_context,
    filter_allowed_tools,
    get_all_allowed_tools,
    load_tool_permissions,
    load_tool_permissions_all_phones,
    load_tool_permissions_union,
    save_tool_permissions,
)

# ── ToolCategory ──────────────────────────────────────────────────────────────


def test_tool_category_values():
    assert ToolCategory.READ.value == "read"
    assert ToolCategory.WRITE.value == "write"
    assert ToolCategory.DELETE.value == "delete"


# ── TOOL_CATEGORIES completeness ─────────────────────────────────────────────


def test_all_tools_have_category():
    """Every tool in MODULE_GROUPS must be in TOOL_CATEGORIES."""
    all_module_tools = set()
    for tools in MODULE_GROUPS.values():
        all_module_tools.update(tools)
    for tool in all_module_tools:
        assert tool in TOOL_CATEGORIES, f"Tool {tool!r} in MODULE_GROUPS but not in TOOL_CATEGORIES"


def test_all_categories_are_valid():
    for name, cat in TOOL_CATEGORIES.items():
        assert isinstance(cat, ToolCategory), f"{name}: expected ToolCategory, got {type(cat)}"


def test_module_groups_cover_all_tools():
    """All TOOL_CATEGORIES entries should appear in MODULE_GROUPS."""
    grouped = set()
    for tools in MODULE_GROUPS.values():
        grouped.update(tools)
    for tool in TOOL_CATEGORIES:
        assert tool in grouped, f"Tool {tool!r} in TOOL_CATEGORIES but not in any MODULE_GROUPS"


def test_pipeline_safe_tools_are_read_only():
    """All tools in _PIPELINE_SAFE_TOOLS that exist in TOOL_CATEGORIES should be READ category."""
    from src.agent.tools import _PIPELINE_SAFE_TOOLS

    for tool_name in _PIPELINE_SAFE_TOOLS:
        cat = TOOL_CATEGORIES.get(tool_name)
        if cat is not None:
            assert cat == ToolCategory.READ, (
                f"_PIPELINE_SAFE_TOOLS contains {tool_name!r} with category {cat}, expected READ"
            )


# ── _default_permissions ─────────────────────────────────────────────────────


def test_default_permissions_all_true():
    perms = _default_permissions()
    assert len(perms) == len(TOOL_CATEGORIES)
    assert all(perms.values())


# ── _is_per_phone_format ─────────────────────────────────────────────────────


class TestIsPerPhoneFormat:
    def test_empty_dict(self):
        assert _is_per_phone_format({}) is False

    def test_flat_format(self):
        assert _is_per_phone_format({"search_messages": True, "send_message": False}) is False

    def test_per_phone_format(self):
        perms = {"+7900": {"search_messages": True}, "+7800": {"search_messages": False}}
        assert _is_per_phone_format(perms) is True

    def test_none_is_false(self):
        assert _is_per_phone_format(None) is False


# ── load_tool_permissions ─────────────────────────────────────────────────────


class TestLoadToolPermissions:
    @pytest.mark.asyncio
    async def test_no_setting_returns_all_enabled(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        perms = await load_tool_permissions(db)
        assert all(perms.values())
        assert len(perms) == len(TOOL_CATEGORIES)

    @pytest.mark.asyncio
    async def test_flat_format(self):
        db = MagicMock()
        raw = json.dumps({"search_messages": False, "send_message": True})
        db.get_setting = AsyncMock(return_value=raw)
        perms = await load_tool_permissions(db)
        assert perms["search_messages"] is False
        assert perms["send_message"] is True
        # Non-specified tools default to True
        assert perms["list_channels"] is True

    @pytest.mark.asyncio
    async def test_per_phone_format_with_matching_phone(self):
        db = MagicMock()
        raw = json.dumps({"+7900": {"search_messages": False, "list_channels": False}})
        db.get_setting = AsyncMock(return_value=raw)
        perms = await load_tool_permissions(db, phone="+7900")
        assert perms["search_messages"] is False
        assert perms["list_channels"] is False
        # Non-specified tools for this phone default to True
        assert perms["send_message"] is True

    @pytest.mark.asyncio
    async def test_per_phone_phone_not_in_saved(self):
        db = MagicMock()
        raw = json.dumps({"+7900": {"search_messages": False}})
        db.get_setting = AsyncMock(return_value=raw)
        perms = await load_tool_permissions(db, phone="+7800")
        # Phone not in saved → defaults (all enabled)
        assert all(perms.values())

    @pytest.mark.asyncio
    async def test_per_phone_none_defaults_to_primary(self):
        primary = SimpleNamespace(phone="+111", is_primary=True)
        db = MagicMock()
        raw = json.dumps({"+111": {"search_messages": False}})
        db.get_setting = AsyncMock(return_value=raw)
        db.get_accounts = AsyncMock(return_value=[primary])
        perms = await load_tool_permissions(db, phone=None)
        assert perms["search_messages"] is False

    @pytest.mark.asyncio
    async def test_corrupted_json_returns_defaults(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value="not valid json{{{")
        perms = await load_tool_permissions(db)
        assert all(perms.values())


# ── load_tool_permissions_all_phones ──────────────────────────────────────────


class TestLoadToolPermissionsAllPhones:
    @pytest.mark.asyncio
    async def test_no_setting_returns_defaults(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        acc1 = SimpleNamespace(phone="+7900")
        result = await load_tool_permissions_all_phones(db, [acc1])
        assert "+7900" in result
        assert all(result["+7900"].values())

    @pytest.mark.asyncio
    async def test_flat_applies_to_all_phones(self):
        db = MagicMock()
        raw = json.dumps({"search_messages": False})
        db.get_setting = AsyncMock(return_value=raw)
        acc1 = SimpleNamespace(phone="+7900")
        acc2 = SimpleNamespace(phone="+7800")
        result = await load_tool_permissions_all_phones(db, [acc1, acc2])
        assert result["+7900"]["search_messages"] is False
        assert result["+7800"]["search_messages"] is False

    @pytest.mark.asyncio
    async def test_per_phone_isolation(self):
        db = MagicMock()
        raw = json.dumps({
            "+7900": {"search_messages": False},
            "+7800": {"search_messages": True},
        })
        db.get_setting = AsyncMock(return_value=raw)
        acc1 = SimpleNamespace(phone="+7900")
        acc2 = SimpleNamespace(phone="+7800")
        result = await load_tool_permissions_all_phones(db, [acc1, acc2])
        assert result["+7900"]["search_messages"] is False
        assert result["+7800"]["search_messages"] is True

    @pytest.mark.asyncio
    async def test_phone_not_in_saved_gets_defaults(self):
        db = MagicMock()
        raw = json.dumps({"+7900": {"search_messages": False}})
        db.get_setting = AsyncMock(return_value=raw)
        acc1 = SimpleNamespace(phone="+7900")
        acc2 = SimpleNamespace(phone="+7800")
        result = await load_tool_permissions_all_phones(db, [acc1, acc2])
        assert result["+7800"]["search_messages"] is True


# ── save_tool_permissions ─────────────────────────────────────────────────────


class TestSaveToolPermissions:
    @pytest.mark.asyncio
    async def test_flat_save(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        db.set_setting = AsyncMock()
        perms = {"search_messages": False}
        await save_tool_permissions(db, perms)
        db.set_setting.assert_awaited_once()
        saved_json = db.set_setting.call_args[0][1]
        assert json.loads(saved_json) == perms

    @pytest.mark.asyncio
    async def test_per_phone_save(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        db.set_setting = AsyncMock()
        db.get_accounts = AsyncMock(return_value=[SimpleNamespace(phone="+7900")])
        perms = {"search_messages": False}
        await save_tool_permissions(db, perms, phone="+7900")
        db.set_setting.assert_awaited_once()
        saved_json = db.set_setting.call_args[0][1]
        parsed = json.loads(saved_json)
        assert "+7900" in parsed
        assert parsed["+7900"]["search_messages"] is False

    @pytest.mark.asyncio
    async def test_per_phone_migrates_legacy_flat(self):
        """Saving per-phone when existing is flat should migrate to per-phone."""
        db = MagicMock()
        existing_flat = json.dumps({"search_messages": False})
        db.get_setting = AsyncMock(return_value=existing_flat)
        db.set_setting = AsyncMock()
        db.get_accounts = AsyncMock(return_value=[SimpleNamespace(phone="+7900")])
        perms = {"search_messages": True}
        await save_tool_permissions(db, perms, phone="+7900")
        saved_json = db.set_setting.call_args[0][1]
        parsed = json.loads(saved_json)
        # Should be per-phone format now
        assert isinstance(parsed["+7900"], dict)
        assert parsed["+7900"]["search_messages"] is True

    @pytest.mark.asyncio
    async def test_per_phone_does_not_touch_other_phones(self):
        db = MagicMock()
        existing = json.dumps({
            "+7900": {"search_messages": False},
            "+7800": {"search_messages": True},
        })
        db.get_setting = AsyncMock(return_value=existing)
        db.set_setting = AsyncMock()
        db.get_accounts = AsyncMock(return_value=[
            SimpleNamespace(phone="+7900"),
            SimpleNamespace(phone="+7800"),
        ])
        perms = {"search_messages": True}
        await save_tool_permissions(db, perms, phone="+7900")
        saved_json = db.set_setting.call_args[0][1]
        parsed = json.loads(saved_json)
        assert parsed["+7900"]["search_messages"] is True
        assert parsed["+7800"]["search_messages"] is True


# ── load_tool_permissions_union ───────────────────────────────────────────────


class TestLoadToolPermissionsUnion:
    @pytest.mark.asyncio
    async def test_no_setting_all_enabled(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        result = await load_tool_permissions_union(db)
        assert all(result.values())

    @pytest.mark.asyncio
    async def test_flat_format(self):
        db = MagicMock()
        raw = json.dumps({"search_messages": False, "send_message": True})
        db.get_setting = AsyncMock(return_value=raw)
        result = await load_tool_permissions_union(db)
        assert result["search_messages"] is False
        assert result["send_message"] is True

    @pytest.mark.asyncio
    async def test_union_across_phones(self):
        """Union: True if ANY phone allows the tool."""
        db = MagicMock()
        raw = json.dumps({
            "+7900": {"search_messages": False},
            "+7800": {"search_messages": True},
        })
        db.get_setting = AsyncMock(return_value=raw)
        result = await load_tool_permissions_union(db)
        assert result["search_messages"] is True

    @pytest.mark.asyncio
    async def test_all_phones_disable_means_disabled(self):
        db = MagicMock()
        raw = json.dumps({
            "+7900": {"search_messages": False},
            "+7800": {"search_messages": False},
        })
        db.get_setting = AsyncMock(return_value=raw)
        result = await load_tool_permissions_union(db)
        assert result["search_messages"] is False

    @pytest.mark.asyncio
    async def test_cache_returns_same_result(self):
        import src.agent.tools.permissions as perm_mod
        # Clear the module-level cache so we get a fresh start
        perm_mod._permissions_cache = None

        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        r1 = await load_tool_permissions_union(db, use_cache=True)
        r2 = await load_tool_permissions_union(db, use_cache=True)
        assert r1 is r2
        # Second call should use cache, not query DB again
        assert db.get_setting.await_count == 1


# ── get_all_allowed_tools ─────────────────────────────────────────────────────


class TestGetAllAllowedTools:
    def test_returns_list(self):
        tools = get_all_allowed_tools()
        assert isinstance(tools, list)
        assert len(tools) == len(TOOL_CATEGORIES)

    def test_mcp_prefix_applied(self):
        tools = get_all_allowed_tools()
        for tool in tools:
            if tool in BUILTIN_TOOLS:
                assert not tool.startswith(MCP_PREFIX)
            else:
                assert tool.startswith(MCP_PREFIX)

    def test_builtin_tools_bare_names(self):
        tools = get_all_allowed_tools()
        for builtin in BUILTIN_TOOLS:
            assert builtin in tools

    def test_caches_result(self):
        r1 = get_all_allowed_tools()
        r2 = get_all_allowed_tools()
        assert r1 is r2


# ── filter_allowed_tools ──────────────────────────────────────────────────────


class TestFilterAllowedTools:
    def test_all_allowed(self):
        all_tools = [f"{MCP_PREFIX}search_messages", f"{MCP_PREFIX}send_message"]
        perms = {"search_messages": True, "send_message": True}
        result = filter_allowed_tools(all_tools, perms)
        assert result == all_tools

    def test_filters_disabled(self):
        all_tools = [f"{MCP_PREFIX}search_messages", f"{MCP_PREFIX}send_message"]
        perms = {"search_messages": True, "send_message": False}
        result = filter_allowed_tools(all_tools, perms)
        assert result == [f"{MCP_PREFIX}search_messages"]

    def test_unknown_tools_denied(self):
        all_tools = [f"{MCP_PREFIX}unknown_tool", "WebSearch"]
        perms = {"search_messages": True}  # unknown_tool not in perms
        result = filter_allowed_tools(all_tools, perms)
        assert result == []

    def test_builtin_tools_without_prefix(self):
        all_tools = ["WebSearch", "WebFetch"]
        perms = {"WebSearch": True, "WebFetch": True}
        result = filter_allowed_tools(all_tools, perms)
        assert result == ["WebSearch", "WebFetch"]

    def test_empty_tools_list(self):
        result = filter_allowed_tools([], {"search_messages": True})
        assert result == []


# ── build_template_context ────────────────────────────────────────────────────


class TestBuildTemplateContext:
    def test_returns_dict_with_expected_keys(self):
        perms = _default_permissions()
        ctx = build_template_context(perms)
        assert "tool_permission_categories" in ctx
        assert "tool_permission_modules" in ctx

    def test_categories_have_read_write_delete(self):
        perms = _default_permissions()
        ctx = build_template_context(perms)
        cats = ctx["tool_permission_categories"]
        assert "read" in cats
        assert "write" in cats
        assert "delete" in cats

    def test_all_tools_in_categories(self):
        perms = _default_permissions()
        ctx = build_template_context(perms)
        cats = ctx["tool_permission_categories"]
        total = sum(len(v) for v in cats.values())
        assert total == len(TOOL_CATEGORIES)

    def test_module_entries_have_tools(self):
        perms = _default_permissions()
        ctx = build_template_context(perms)
        modules = ctx["tool_permission_modules"]
        assert len(modules) == len(MODULE_GROUPS)
        for mod in modules:
            assert "name" in mod
            assert "tools" in mod
            assert len(mod["tools"]) > 0

    def test_tool_enabled_reflects_permissions(self):
        perms = _default_permissions()
        perms["search_messages"] = False
        ctx = build_template_context(perms)
        # Find search_messages in categories
        cats = ctx["tool_permission_categories"]
        all_entries = []
        for entries in cats.values():
            all_entries.extend(entries)
        sm = next(e for e in all_entries if e["name"] == "search_messages")
        assert sm["enabled"] is False

    def test_empty_permissions(self):
        ctx = build_template_context({})
        # Should not crash; enabled defaults to False
        cats = ctx["tool_permission_categories"]
        total = sum(len(v) for v in cats.values())
        assert total == len(TOOL_CATEGORIES)
