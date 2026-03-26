"""Tests for agent tool permission system — 3-layer gates and permission storage."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.tools._registry import (
    normalize_phone,
    require_confirmation,
    require_phone_permission,
    require_pool,
)
from src.agent.tools.permissions import (
    TOOL_CATEGORIES,
    _is_per_phone_format,
    build_template_context,
    filter_allowed_tools,
    load_tool_permissions,
    load_tool_permissions_all_phones,
    save_tool_permissions,
)
from src.database import Database


@pytest.fixture
def mock_db():
    return MagicMock(spec=Database)


def _text(result: dict) -> str:
    return result["content"][0]["text"]


# ---------------------------------------------------------------------------
# _registry.normalize_phone
# ---------------------------------------------------------------------------


class TestNormalizePhone:
    def test_adds_plus_prefix(self):
        assert normalize_phone("79990001111") == "+79990001111"

    def test_preserves_existing_plus(self):
        assert normalize_phone("+79990001111") == "+79990001111"

    def test_strips_whitespace(self):
        assert normalize_phone("  +79990001111  ") == "+79990001111"

    def test_empty_string(self):
        assert normalize_phone("") == ""


# ---------------------------------------------------------------------------
# _registry.require_confirmation
# ---------------------------------------------------------------------------


class TestRequireConfirmation:
    def test_confirm_true_returns_none(self):
        assert require_confirmation("удалит канал", {"confirm": True}) is None

    def test_confirm_false_returns_warning(self):
        result = require_confirmation("удалит канал", {"confirm": False})
        assert result is not None
        assert "Подтвердите" in _text(result)

    def test_confirm_missing_returns_warning(self):
        result = require_confirmation("удалит канал", {})
        assert result is not None
        assert "Подтвердите" in _text(result)

    def test_action_description_in_text(self):
        result = require_confirmation("выйдет из 3 диалогов", {})
        assert "выйдет из 3 диалогов" in _text(result)

    def test_response_format(self):
        result = require_confirmation("test", {})
        assert isinstance(result, dict)
        assert result["content"][0]["type"] == "text"
        assert isinstance(result["content"][0]["text"], str)


# ---------------------------------------------------------------------------
# _registry.require_pool
# ---------------------------------------------------------------------------


class TestRequirePool:
    def test_pool_present_returns_none(self):
        assert require_pool(MagicMock()) is None

    def test_pool_none_returns_error(self):
        result = require_pool(None)
        assert "требует Telegram-клиент" in _text(result)

    def test_custom_action_in_text(self):
        result = require_pool(None, action="Выход из диалогов")
        assert "Выход из диалогов" in _text(result)


# ---------------------------------------------------------------------------
# _registry.require_phone_permission
# ---------------------------------------------------------------------------


class TestRequirePhonePermission:
    async def test_no_setting_allows(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_db_error_allows(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("DB down"))
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_malformed_json_allows(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value="not json{{{")
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_phone_in_allowed(self, mock_db):
        perms = {"+79990001111": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_phone_not_in_allowed(self, mock_db):
        perms = {"+79990002222": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        text = _text(result)
        assert "не разрешён" in text
        assert "+79990002222" in text

    async def test_tool_not_restricted_allows(self, mock_db):
        perms = {"+79990001111": {"some_other_tool": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_empty_phone_hint(self, mock_db):
        perms = {"+79990001111": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "", "leave_dialogs")
        text = _text(result)
        assert "укажи параметр phone" in text
        assert "+79990001111" in text


# ---------------------------------------------------------------------------
# permissions.load_tool_permissions
# ---------------------------------------------------------------------------


class TestLoadToolPermissions:
    async def test_no_setting_returns_defaults(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        result = await load_tool_permissions(mock_db)
        assert all(v is True for v in result.values())
        assert set(result.keys()) == set(TOOL_CATEGORIES.keys())

    async def test_malformed_json_returns_defaults(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value="{bad json")
        result = await load_tool_permissions(mock_db)
        assert all(v is True for v in result.values())

    async def test_flat_format(self, mock_db):
        saved = {"search_messages": False}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db)
        assert result["search_messages"] is False
        assert result["list_channels"] is True

    async def test_per_phone_matching(self, mock_db):
        saved = {"+79990001111": {"search_messages": False, "leave_dialogs": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db, phone="+79990001111")
        assert result["search_messages"] is False
        assert result["leave_dialogs"] is False
        assert result["list_channels"] is True  # default

    async def test_per_phone_not_matching_returns_defaults(self, mock_db):
        saved = {"+79990001111": {"search_messages": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db, phone="+79990009999")
        assert all(v is True for v in result.values())

    async def test_per_phone_no_phone_uses_primary(self, mock_db):
        saved = {"+79990001111": {"search_messages": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79990001111", is_primary=True)]
        )
        result = await load_tool_permissions(mock_db, phone=None)
        assert result["search_messages"] is False

    async def test_per_phone_no_accounts_returns_defaults(self, mock_db):
        saved = {"+79990001111": {"search_messages": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        mock_db.get_accounts = AsyncMock(return_value=[])
        result = await load_tool_permissions(mock_db, phone=None)
        assert all(v is True for v in result.values())


# ---------------------------------------------------------------------------
# permissions.load_tool_permissions_all_phones
# ---------------------------------------------------------------------------


class TestLoadToolPermissionsAllPhones:
    async def test_no_setting_defaults_for_all(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        accounts = [SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        assert set(result.keys()) == {"+7111", "+7222"}
        assert all(v is True for v in result["+7111"].values())

    async def test_per_phone_format(self, mock_db):
        saved = {"+7111": {"search_messages": False}, "+7222": {"leave_dialogs": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        accounts = [SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        assert result["+7111"]["search_messages"] is False
        assert result["+7111"]["leave_dialogs"] is True
        assert result["+7222"]["leave_dialogs"] is False

    async def test_legacy_flat_applies_to_all(self, mock_db):
        saved = {"search_messages": False}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        accounts = [SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        assert result["+7111"]["search_messages"] is False
        assert result["+7222"]["search_messages"] is False


# ---------------------------------------------------------------------------
# permissions.save_tool_permissions
# ---------------------------------------------------------------------------


class TestSaveToolPermissions:
    async def test_flat_save(self, mock_db):
        mock_db.set_setting = AsyncMock()
        perms = {"search_messages": True, "leave_dialogs": False}
        await save_tool_permissions(mock_db, perms, phone=None)
        mock_db.set_setting.assert_awaited_once()
        saved_json = mock_db.set_setting.await_args[0][1]
        assert json.loads(saved_json) == perms

    async def test_per_phone_new_entry(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=json.dumps({"+7111": {"a": True}}))
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"b": True}, phone="+7222")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        assert saved["+7111"] == {"a": True}
        assert saved["+7222"] == {"b": True}

    async def test_per_phone_overwrite(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=json.dumps({"+7111": {"a": True}}))
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"a": False}, phone="+7111")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        assert saved["+7111"] == {"a": False}

    async def test_per_phone_migrates_legacy_flat(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=json.dumps({"search_messages": True}))
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"leave_dialogs": False}, phone="+7111")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        # Legacy flat discarded, only per-phone entry remains
        assert "+7111" in saved
        assert "search_messages" not in saved


# ---------------------------------------------------------------------------
# permissions.filter_allowed_tools
# ---------------------------------------------------------------------------


class TestFilterAllowedTools:
    def test_all_allowed(self):
        tools = ["mcp__telegram_db__search_messages", "mcp__telegram_db__list_channels"]
        perms = {"search_messages": True, "list_channels": True}
        assert filter_allowed_tools(tools, perms) == tools

    def test_one_disabled(self):
        tools = ["mcp__telegram_db__search_messages", "mcp__telegram_db__list_channels"]
        perms = {"search_messages": False, "list_channels": True}
        assert filter_allowed_tools(tools, perms) == ["mcp__telegram_db__list_channels"]

    def test_all_disabled(self):
        tools = ["mcp__telegram_db__search_messages", "mcp__telegram_db__list_channels"]
        perms = {"search_messages": False, "list_channels": False}
        assert filter_allowed_tools(tools, perms) == []

    def test_unknown_tool_denied(self):
        tools = ["mcp__telegram_db__unknown_tool"]
        perms = {"search_messages": True}
        assert filter_allowed_tools(tools, perms) == []

    def test_strips_mcp_prefix(self):
        tools = ["mcp__telegram_db__leave_dialogs"]
        perms = {"leave_dialogs": True}
        assert filter_allowed_tools(tools, perms) == ["mcp__telegram_db__leave_dialogs"]


# ---------------------------------------------------------------------------
# permissions._is_per_phone_format
# ---------------------------------------------------------------------------


class TestIsPerPhoneFormat:
    def test_empty_dict(self):
        assert _is_per_phone_format({}) is False

    def test_flat_format(self):
        assert _is_per_phone_format({"search_messages": True}) is False

    def test_per_phone_format(self):
        assert _is_per_phone_format({"+7111": {"search_messages": True}}) is True


# ---------------------------------------------------------------------------
# permissions.build_template_context
# ---------------------------------------------------------------------------


class TestBuildTemplateContext:
    def test_categories_keys(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        ctx = build_template_context(perms)
        assert set(ctx["tool_permission_categories"].keys()) == {"read", "write", "delete"}
        for cat_list in ctx["tool_permission_categories"].values():
            assert len(cat_list) > 0

    def test_modules_non_empty(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        ctx = build_template_context(perms)
        assert len(ctx["tool_permission_modules"]) > 0

    def test_disabled_tool_reflected(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        perms["leave_dialogs"] = False
        ctx = build_template_context(perms)
        # Find leave_dialogs in categories
        delete_tools = ctx["tool_permission_categories"]["delete"]
        leave = next(t for t in delete_tools if t["name"] == "leave_dialogs")
        assert leave["enabled"] is False


# ---------------------------------------------------------------------------
# End-to-end: 3-layer permission chain on leave_dialogs
# ---------------------------------------------------------------------------


def _get_tool_handlers(mock_db, client_pool=None, config=None):
    captured_tools = []
    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kwargs: captured_tools.extend(kwargs.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config)
    return {t.name: t.handler for t in captured_tools}


class TestEndToEndPermissionFlow:
    """Integration tests: pool gate → phone permission → confirmation → business logic."""

    async def test_allowed_and_confirmed(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        pool = MagicMock()
        with patch("src.services.channel_service.ChannelService") as mock_svc_cls:
            mock_svc_cls.return_value.leave_dialogs = AsyncMock(return_value={123: True})
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["leave_dialogs"]({
                "phone": "+79990001111",
                "dialog_ids": "123",
                "confirm": True,
            })
        assert "покинут" in _text(result)

    async def test_no_confirm_returns_warning(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
        })
        assert "Подтвердите" in _text(result)

    async def test_retry_with_confirm_succeeds(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        # First call — no confirm
        r1 = await handlers["leave_dialogs"]({"phone": "+79990001111", "dialog_ids": "123"})
        assert "Подтвердите" in _text(r1)
        # Retry with confirm
        with patch("src.services.channel_service.ChannelService") as mock_svc_cls:
            mock_svc_cls.return_value.leave_dialogs = AsyncMock(return_value={123: True})
            r2 = await handlers["leave_dialogs"]({
                "phone": "+79990001111",
                "dialog_ids": "123",
                "confirm": True,
            })
        assert "покинут" in _text(r2)

    async def test_phone_not_allowed(self, mock_db):
        perms = {"+79990002222": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
            "confirm": True,
        })
        assert "не разрешён" in _text(result)

    async def test_no_pool_cli_mode(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
            "confirm": True,
        })
        assert "требует Telegram-клиент" in _text(result)
