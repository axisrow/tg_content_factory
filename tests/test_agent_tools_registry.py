"""Tests for src/agent/tools/_registry.py — helper functions for agent tools."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.tools._registry import (
    _text_response,
    normalize_phone,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_phone,
)

# ── _text_response ────────────────────────────────────────────────────────────


def test_text_response_structure():
    result = _text_response("hello")
    assert result == {"content": [{"type": "text", "text": "hello"}]}


def test_text_response_empty_string():
    result = _text_response("")
    assert result["content"][0]["text"] == ""


# ── normalize_phone ───────────────────────────────────────────────────────────


class TestNormalizePhone:
    def test_adds_plus(self):
        assert normalize_phone("79001234567") == "+79001234567"

    def test_keeps_existing_plus(self):
        assert normalize_phone("+79001234567") == "+79001234567"

    def test_strips_whitespace(self):
        assert normalize_phone("  +79001234567  ") == "+79001234567"

    def test_empty_string(self):
        assert normalize_phone("") == ""

    def test_whitespace_only(self):
        assert normalize_phone("   ") == ""

    def test_adds_plus_after_strip(self):
        assert normalize_phone("  7900  ") == "+7900"


# ── require_confirmation ──────────────────────────────────────────────────────


class TestRequireConfirmation:
    def test_returns_none_when_confirmed(self):
        assert require_confirmation("удалит канал", {"confirm": True}) is None

    def test_returns_none_when_confirmed_false_value(self):
        """confirm=1 is truthy and passes the truthy check."""
        # Python truthiness: 1 is truthy, so args.get("confirm") → 1 which is truthy
        assert require_confirmation("test", {"confirm": 1}) is None

    def test_returns_warning_when_not_confirmed(self):
        result = require_confirmation("удалит канал 'X'", {})
        assert result is not None
        text = result["content"][0]["text"]
        assert "удалит канал 'X'" in text
        assert "confirm=true" in text

    def test_returns_warning_when_confirm_false(self):
        result = require_confirmation("удалит канал", {"confirm": False})
        assert result is not None
        assert "confirm=true" in result["content"][0]["text"]


# ── require_pool ──────────────────────────────────────────────────────────────


class TestRequirePool:
    def test_returns_none_when_pool_exists(self):
        assert require_pool(MagicMock()) is None

    def test_returns_error_when_pool_none(self):
        result = require_pool(None)
        assert result is not None
        text = result["content"][0]["text"]
        assert "требует Telegram-клиент" in text
        assert "CLI-режиме" in text

    def test_custom_action_name(self):
        result = require_pool(None, action="Отправка сообщения")
        text = result["content"][0]["text"]
        assert "Отправка сообщения" in text


# ── resolve_phone ─────────────────────────────────────────────────────────────


class TestResolvePhone:
    @pytest.mark.anyio
    async def test_non_empty_phone_passed_through(self):
        db = MagicMock()
        phone, err = await resolve_phone(db, "+79001234567")
        assert phone == "+79001234567"
        assert err is None

    @pytest.mark.anyio
    async def test_normalizes_phone_without_plus(self):
        db = MagicMock()
        phone, err = await resolve_phone(db, "79001234567")
        assert phone == "+79001234567"
        assert err is None

    @pytest.mark.anyio
    async def test_empty_phone_defaults_to_primary(self):
        primary = SimpleNamespace(phone="+11111111111", is_primary=True)
        secondary = SimpleNamespace(phone="+22222222222", is_primary=False)
        db = MagicMock()
        db.get_accounts = AsyncMock(return_value=[secondary, primary])

        phone, err = await resolve_phone(db, "")
        assert phone == "+11111111111"
        assert err is None

    @pytest.mark.anyio
    async def test_empty_phone_no_primary_picks_first(self):
        acc1 = SimpleNamespace(phone="+111", is_primary=False)
        acc2 = SimpleNamespace(phone="+222", is_primary=False)
        db = MagicMock()
        db.get_accounts = AsyncMock(return_value=[acc1, acc2])

        phone, err = await resolve_phone(db, "")
        assert phone == "+111"
        assert err is None

    @pytest.mark.anyio
    async def test_empty_phone_no_accounts(self):
        db = MagicMock()
        db.get_accounts = AsyncMock(return_value=[])
        phone, err = await resolve_phone(db, "")
        assert phone == ""
        assert err is not None
        assert "нет подключённых" in err["content"][0]["text"]

    @pytest.mark.anyio
    async def test_db_exception_returns_error(self):
        db = MagicMock()
        db.get_accounts = AsyncMock(side_effect=Exception("DB down"))
        phone, err = await resolve_phone(db, "")
        assert phone == ""
        assert err is not None
        assert "не удалось получить" in err["content"][0]["text"]


# ── require_phone_permission ──────────────────────────────────────────────────


class TestRequirePhonePermission:
    @pytest.mark.anyio
    async def test_no_setting_allows_all(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)
        result = await require_phone_permission(db, "+7900", "search_messages")
        assert result is None

    @pytest.mark.anyio
    async def test_empty_setting_allows_all(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value="")
        result = await require_phone_permission(db, "+7900", "search_messages")
        assert result is None

    @pytest.mark.anyio
    async def test_malformed_json_blocks(self):
        db = MagicMock()
        db.get_setting = AsyncMock(return_value="not-json")
        result = await require_phone_permission(db, "+7900", "search_messages")
        assert result is not None
        assert "заблокировано" in result["content"][0]["text"]

    @pytest.mark.anyio
    async def test_db_exception_blocks(self):
        db = MagicMock()
        db.get_setting = AsyncMock(side_effect=Exception("err"))
        result = await require_phone_permission(db, "+7900", "search_messages")
        assert result is not None
        assert "заблокировано" in result["content"][0]["text"]

    @pytest.mark.anyio
    async def test_phone_in_allowed_list(self):
        db = MagicMock()
        perms = {"+7900": {"search_messages": True}}
        db.get_setting = AsyncMock(return_value=__import__("json").dumps(perms))
        result = await require_phone_permission(db, "+7900", "search_messages")
        assert result is None

    @pytest.mark.anyio
    async def test_phone_not_allowed(self):
        """Phone is in perms dict but tool is disabled for it → blocked."""
        db = MagicMock()
        perms = {
            "+7900": {"search_messages": True},
            "+7800": {"search_messages": False},
        }
        db.get_setting = AsyncMock(return_value=__import__("json").dumps(perms))
        with patch("src.agent.permission_gate.get_gate", return_value=None):
            result = await require_phone_permission(db, "+7800", "search_messages")
        assert result is not None
        text = result["content"][0]["text"]
        assert "не разрешён" in text
        assert "+7900" in text

    @pytest.mark.anyio
    async def test_phone_not_in_perms_defaults_allowed(self):
        """Phone not in perms dict at all → defaults to allowed."""
        db = MagicMock()
        perms = {"+7900": {"search_messages": True}}
        db.get_setting = AsyncMock(return_value=__import__("json").dumps(perms))
        with patch("src.agent.permission_gate.get_gate", return_value=None):
            result = await require_phone_permission(db, "+7800", "search_messages")
        # Phone not in perms dict → allowed (returns None)
        assert result is None

    @pytest.mark.anyio
    async def test_no_phone_shows_phone_list(self):
        db = MagicMock()
        perms = {"+7900": {"search_messages": True}}
        db.get_setting = AsyncMock(return_value=__import__("json").dumps(perms))
        with patch("src.agent.permission_gate.get_gate", return_value=None):
            result = await require_phone_permission(db, "", "search_messages")
        assert result is not None
        text = result["content"][0]["text"]
        assert "укажи параметр phone" in text or "Разрешённые" in text

    @pytest.mark.anyio
    async def test_tool_not_restricted_for_any_phone(self):
        """If no phone has this tool enabled, tool is not restricted → allow."""
        db = MagicMock()
        perms = {"+7900": {"other_tool": True}}
        db.get_setting = AsyncMock(return_value=__import__("json").dumps(perms))
        result = await require_phone_permission(db, "+7900", "search_messages")
        assert result is None
