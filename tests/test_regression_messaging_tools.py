"""Tests for agent messaging tools — all messaging tool handlers and error paths."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database


def _downloads_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "downloads"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    db._db_path = ":memory:"
    db._session_encryption_secret = None
    return db




def _get_messaging_handlers(mock_db, client_pool=None):
    """Build MCP tools and return messaging handlers keyed by name."""
    get_setting = getattr(mock_db, "get_setting", None)
    if isinstance(get_setting, AsyncMock) and get_setting.side_effect is None and isinstance(
        get_setting.return_value, (AsyncMock, MagicMock)
    ):
        get_setting.return_value = None
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server
        make_mcp_server(mock_db, client_pool=client_pool)

    return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}


def _text(result) -> str:
    """Extract text from tool result payload."""
    if isinstance(result, dict):
        return result["content"][0]["text"]
    if hasattr(result, "content"):
        return result.content[0].text if hasattr(result.content[0], "text") else str(result.content[0])
    return str(result)




# ===========================================================================
# 1. cli/commands/test.py — read/write check functions
# ===========================================================================




class TestMessagingTools:
    @pytest.fixture
    def messaging_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock()

        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)

        handlers = _get_messaging_handlers(mock_db, client_pool=pool)
        return handlers, pool, mock_db

    async def test_send_message_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["send_message"]({
            "phone": "+1111", "recipient": "@user", "text": "hi", "confirm": True,
        })
        assert "отправлено" in _text(result).lower()

    async def test_edit_message_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1, "text": "edited", "confirm": True,
        })
        assert "отредактировано" in _text(result).lower()

    async def test_delete_message_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["delete_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_ids": "1,2", "confirm": True,
        })
        assert "удалено" in _text(result).lower()

    async def test_forward_messages_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["forward_messages"]({
            "phone": "+1111", "from_chat": "@a", "to_chat": "@b", "message_ids": "1,2", "confirm": True,
        })
        assert "переслано" in _text(result).lower()

    async def test_pin_message_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["pin_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1, "notify": False, "confirm": True,
        })
        assert "закреплено" in _text(result).lower()

    async def test_unpin_message_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["unpin_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": None, "confirm": True,
        })
        assert "откреплено" in _text(result).lower()

    async def test_get_participants_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        p1 = SimpleNamespace(id=1, first_name="A", last_name="B", username="ab")
        mock_client.get_participants = AsyncMock(return_value=[p1])
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["get_participants"]({
            "phone": "+1111", "chat_id": "@ch", "limit": 10, "search": "",
        })
        assert "участник" in _text(result).lower() or "1:" in _text(result)

    async def test_edit_admin_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_admin"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u", "is_admin": True,
            "title": "mod", "confirm": True,
        })
        assert "обновлены" in _text(result).lower()

    async def test_edit_permissions_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_permissions"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "send_messages": False, "send_media": None, "until_date": None, "confirm": True,
        })
        assert "обновлены" in _text(result).lower()

    async def test_kick_participant_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["kick_participant"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u", "confirm": True,
        })
        assert "исключён" in _text(result).lower()

    async def test_get_broadcast_stats_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        mock_stats = SimpleNamespace(
            followers=SimpleNamespace(current=100, previous=90),
            views_per_post=None, shares_per_post=None,
            reactions_per_post=None, forwards_per_post=None,
            period=None, enabled_notifications=None,
        )
        mock_client.get_broadcast_stats = AsyncMock(return_value=mock_stats)
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["get_broadcast_stats"]({"phone": "+1111", "chat_id": "@ch"})
        assert "статистика" in _text(result).lower() or "followers" in _text(result).lower()

    async def test_archive_chat_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["archive_chat"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "архивирован" in _text(result).lower()

    async def test_unarchive_chat_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["unarchive_chat"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "разархивирован" in _text(result).lower()

    async def test_mark_read_success(self, messaging_setup):
        handlers, pool, db = messaging_setup
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["mark_read"]({"phone": "+1111", "chat_id": "@ch", "max_id": None})
        assert "прочит" in _text(result).lower() or "read" in _text(result).lower()

    async def test_send_message_missing_fields(self, messaging_setup):
        handlers, pool, db = messaging_setup
        result = await handlers["send_message"]({
            "phone": "+1111", "recipient": "", "text": "hi", "confirm": True,
        })
        assert "обязател" in _text(result).lower() or "ошибка" in _text(result).lower()

    async def test_send_message_no_confirmation(self, messaging_setup):
        handlers, pool, db = messaging_setup
        result = await handlers["send_message"]({
            "phone": "+1111", "recipient": "@user", "text": "hi", "confirm": False,
        })
        text = _text(result)
        assert "confirm" in text.lower() or "подтвер" in text.lower()


# ===========================================================================
# 4. agent/tools/deepagents_sync.py — remaining sync wrappers
# ===========================================================================




class TestMessagingToolErrors:
    """Cover error branches in all messaging tool handlers."""

    @pytest.fixture
    def msg_err_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)

        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)

        handlers = _get_messaging_handlers(mock_db, client_pool=pool)
        return handlers, pool, mock_db

    async def test_send_message_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["send_message"]({
            "phone": "+1111", "recipient": "@u", "text": "hi", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_send_message_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["send_message"]({
            "phone": "+1111", "recipient": "@u", "text": "hi", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_edit_message_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["edit_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1, "text": "x", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_edit_message_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1, "text": "x", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_delete_message_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["delete_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_ids": "1", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_delete_message_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["delete_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_ids": "1", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_forward_messages_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["forward_messages"]({
            "phone": "+1111", "from_chat": "@a", "to_chat": "@b",
            "message_ids": "1", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_forward_messages_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["forward_messages"]({
            "phone": "+1111", "from_chat": "@a", "to_chat": "@b",
            "message_ids": "1", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_forward_messages_missing_fields(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["forward_messages"]({
            "phone": "+1111", "from_chat": "", "to_chat": "@b",
            "message_ids": "1", "confirm": True,
        })
        assert "обязател" in _text(result).lower()

    async def test_pin_message_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["pin_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1, "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_pin_message_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["pin_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1, "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_unpin_message_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["unpin_message"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_unpin_message_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["unpin_message"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_download_media_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["download_media"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1,
        })
        assert "не найден" in _text(result).lower()

    async def test_download_media_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["download_media"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1,
        })
        assert "ошибка" in _text(result).lower()

    async def test_download_media_missing_fields(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["download_media"]({
            "phone": "+1111", "chat_id": "", "message_id": 1,
        })
        assert "обязател" in _text(result).lower()

    async def test_get_participants_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["get_participants"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "не найден" in _text(result).lower()

    async def test_get_participants_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["get_participants"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "ошибка" in _text(result).lower()

    async def test_get_participants_empty_list(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_participants = AsyncMock(return_value=[])
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["get_participants"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "не найден" in _text(result).lower()

    async def test_edit_admin_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["edit_admin"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "is_admin": True, "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_edit_admin_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_admin"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "is_admin": True, "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_edit_permissions_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["edit_permissions"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "send_messages": False, "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_edit_permissions_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_permissions"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "send_messages": False, "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_edit_permissions_no_flags(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["edit_permissions"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "send_messages": None, "send_media": None, "confirm": True,
        })
        assert "флаг" in _text(result).lower() or "ошибка" in _text(result).lower()

    async def test_kick_participant_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["kick_participant"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_kick_participant_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["kick_participant"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_get_broadcast_stats_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["get_broadcast_stats"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "не найден" in _text(result).lower()

    async def test_get_broadcast_stats_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["get_broadcast_stats"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "ошибка" in _text(result).lower()

    async def test_archive_chat_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["archive_chat"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_archive_chat_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["archive_chat"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_unarchive_chat_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["unarchive_chat"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_unarchive_chat_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["unarchive_chat"]({
            "phone": "+1111", "chat_id": "@ch", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_mark_read_client_none(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        result = await handlers["mark_read"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "не найден" in _text(result).lower()

    async def test_mark_read_exception(self, msg_err_setup):
        handlers, pool, _ = msg_err_setup
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["mark_read"]({
            "phone": "+1111", "chat_id": "@ch",
        })
        assert "ошибка" in _text(result).lower()

    async def test_messaging_no_pool(self):
        """All messaging tools should error if pool is None."""
        mock_db = _make_mock_db()
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_messaging_handlers(mock_db, client_pool=None)
        result = await handlers["send_message"]({"phone": "+1111", "recipient": "@u", "text": "hi"})
        assert "cli" in _text(result).lower() or "telegram" in _text(result).lower()


# ===========================================================================
# 13. agent/tools/images.py — generate_image URL download + errors
# ===========================================================================




class TestMessagingPhonePermGates:
    """Cover resolve_phone error and require_phone_permission gate for each messaging handler."""

    @pytest.fixture
    def msg_phone_err_setup(self):
        """Setup where resolve_phone returns error (no accounts)."""
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[])  # no accounts
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)

        handlers = _get_messaging_handlers(mock_db, client_pool=pool)
        return handlers, pool, mock_db

    @pytest.fixture
    def msg_perm_gate_setup(self):
        """Setup where require_phone_permission blocks the call."""
        import json
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.repos.settings = MagicMock()
        # Set up tool permissions that explicitly block +1111 but allow +2222
        disabled = {k: False for k in ["send_message", "edit_message", "delete_message",
                                        "forward_messages", "pin_message", "unpin_message",
                                        "download_media", "get_participants", "edit_admin",
                                        "edit_permissions", "kick_participant",
                                        "get_broadcast_stats", "archive_chat",
                                        "unarchive_chat", "mark_read"]}
        perm_data = {
            "+1111": disabled,
            "+2222": {k: True for k in disabled},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perm_data))
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)

        handlers = _get_messaging_handlers(mock_db, client_pool=pool)
        return handlers, pool, mock_db

    async def test_send_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["send_message"]({"phone": "", "recipient": "@u", "text": "hi"})
        assert "аккаунт" in _text(r).lower()

    async def test_edit_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["edit_message"]({"phone": "", "chat_id": "@ch", "message_id": 1, "text": "x"})
        assert "аккаунт" in _text(r).lower()

    async def test_delete_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["delete_message"]({"phone": "", "chat_id": "@ch", "message_ids": "1"})
        assert "аккаунт" in _text(r).lower()

    async def test_forward_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["forward_messages"]({"phone": "", "from_chat": "@a", "to_chat": "@b", "message_ids": "1"})
        assert "аккаунт" in _text(r).lower()

    async def test_pin_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["pin_message"]({"phone": "", "chat_id": "@ch", "message_id": 1})
        assert "аккаунт" in _text(r).lower()

    async def test_unpin_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["unpin_message"]({"phone": "", "chat_id": "@ch"})
        assert "аккаунт" in _text(r).lower()

    async def test_download_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["download_media"]({"phone": "", "chat_id": "@ch", "message_id": 1})
        assert "аккаунт" in _text(r).lower()

    async def test_participants_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["get_participants"]({"phone": "", "chat_id": "@ch"})
        assert "аккаунт" in _text(r).lower()

    async def test_edit_admin_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["edit_admin"]({"phone": "", "chat_id": "@ch", "user_id": "@u", "is_admin": True, "confirm": True})
        assert "аккаунт" in _text(r).lower()

    async def test_edit_permissions_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        args = {"phone": "", "chat_id": "@ch", "user_id": "@u", "send_messages": False, "confirm": True}
        r = await h["edit_permissions"](args)
        assert "аккаунт" in _text(r).lower()

    async def test_kick_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["kick_participant"]({"phone": "", "chat_id": "@ch", "user_id": "@u", "confirm": True})
        assert "аккаунт" in _text(r).lower()

    async def test_broadcast_stats_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["get_broadcast_stats"]({"phone": "", "chat_id": "@ch"})
        assert "аккаунт" in _text(r).lower()

    async def test_archive_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["archive_chat"]({"phone": "", "chat_id": "@ch", "confirm": True})
        assert "аккаунт" in _text(r).lower()

    async def test_unarchive_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["unarchive_chat"]({"phone": "", "chat_id": "@ch", "confirm": True})
        assert "аккаунт" in _text(r).lower()

    async def test_mark_read_phone_err(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        r = await h["mark_read"]({"phone": "", "chat_id": "@ch"})
        assert "аккаунт" in _text(r).lower()

    # Permission gate tests
    async def test_send_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["send_message"]({"phone": "+1111", "recipient": "@u", "text": "hi", "confirm": True})
        text = _text(r)
        assert "phone" in text.lower() or "+2222" in text or "не разрешен" in text.lower()

    async def test_edit_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["edit_message"]({"phone": "+1111", "chat_id": "@ch", "message_id": 1, "text": "x", "confirm": True})
        text = _text(r)
        assert "phone" in text.lower() or "+2222" in text or "не разрешен" in text.lower()

    async def test_delete_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["delete_message"]({"phone": "+1111", "chat_id": "@ch", "message_ids": "1", "confirm": True})
        text = _text(r)
        assert "phone" in text.lower() or "+2222" in text or "не разрешен" in text.lower()

    async def test_forward_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        args = {"phone": "+1111", "from_chat": "@a", "to_chat": "@b", "message_ids": "1", "confirm": True}
        r = await h["forward_messages"](args)
        text = _text(r)
        assert "phone" in text.lower() or "+2222" in text or "не разрешен" in text.lower()

    async def test_pin_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["pin_message"]({"phone": "+1111", "chat_id": "@ch", "message_id": 1, "confirm": True})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_unpin_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["unpin_message"]({"phone": "+1111", "chat_id": "@ch", "confirm": True})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_download_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["download_media"]({"phone": "+1111", "chat_id": "@ch", "message_id": 1})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_participants_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["get_participants"]({"phone": "+1111", "chat_id": "@ch"})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_edit_admin_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["edit_admin"]({"phone": "+1111", "chat_id": "@ch", "user_id": "@u", "confirm": True})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_edit_permissions_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        args = {"phone": "+1111", "chat_id": "@ch", "user_id": "@u", "send_messages": False, "confirm": True}
        r = await h["edit_permissions"](args)
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_kick_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["kick_participant"]({"phone": "+1111", "chat_id": "@ch", "user_id": "@u", "confirm": True})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_broadcast_stats_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["get_broadcast_stats"]({"phone": "+1111", "chat_id": "@ch"})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_archive_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["archive_chat"]({"phone": "+1111", "chat_id": "@ch", "confirm": True})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_unarchive_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["unarchive_chat"]({"phone": "+1111", "chat_id": "@ch", "confirm": True})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    async def test_mark_read_perm_gate(self, msg_perm_gate_setup):
        h, _, _ = msg_perm_gate_setup
        r = await h["mark_read"]({"phone": "+1111", "chat_id": "@ch"})
        assert "+2222" in _text(r) or "phone" in _text(r).lower()

    # Additional missing field validations
    async def test_pin_missing_fields(self, msg_phone_err_setup):
        h, _, _ = msg_phone_err_setup
        from src.models import Account
        h2, _, db2 = msg_phone_err_setup
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db2.get_accounts = AsyncMock(return_value=[acc])
        r = await h2["pin_message"]({"phone": "+1111", "chat_id": "", "message_id": None})
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_unpin_missing_chat(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["unpin_message"]({"phone": "+1111", "chat_id": ""})
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_delete_message_no_valid_ids(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["delete_message"]({
            "phone": "+1111", "chat_id": "@ch", "message_ids": "abc", "confirm": True,
        })
        assert "валидн" in _text(r).lower() or "ошибка" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_forward_messages_no_valid_ids(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["forward_messages"]({
            "phone": "+1111", "from_chat": "@a", "to_chat": "@b",
            "message_ids": "abc", "confirm": True,
        })
        assert "валидн" in _text(r).lower() or "ошибка" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_edit_admin_missing_fields(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["edit_admin"]({
            "phone": "+1111", "chat_id": "", "user_id": "@u", "confirm": True,
        })
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_kick_missing_fields(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["kick_participant"]({
            "phone": "+1111", "chat_id": "", "user_id": "@u", "confirm": True,
        })
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_broadcast_stats_missing_chat(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["get_broadcast_stats"]({"phone": "+1111", "chat_id": ""})
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_archive_missing_chat(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["archive_chat"]({"phone": "+1111", "chat_id": "", "confirm": True})
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_mark_read_missing_chat(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["mark_read"]({"phone": "+1111", "chat_id": ""})
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()

    async def test_participants_missing_chat(self, msg_phone_err_setup):
        h, _, db = msg_phone_err_setup
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        db.get_accounts = AsyncMock(return_value=[acc])
        r = await h["get_participants"]({"phone": "+1111", "chat_id": ""})
        assert "обязател" in _text(r).lower() or "аккаунт" in _text(r).lower()


# ===========================================================================
# 27. cli/commands/dialogs.py — no accounts + client unavailable branches
# ===========================================================================




class TestMessagingFinalEdgeCases:
    @pytest.fixture
    def handlers_with_pool(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        pool = MagicMock()
        from src.models import Account
        acc = Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        handlers = _get_messaging_handlers(mock_db, client_pool=pool)
        return handlers, pool, mock_db

    async def test_download_media_no_media(self, handlers_with_pool):
        """download_media where message exists but has no media (path is None)."""
        handlers, pool, _ = handlers_with_pool
        mock_client = AsyncMock()
        mock_msg = MagicMock()

        async def fake_iter(*args, **kwargs):
            yield mock_msg

        mock_client.iter_messages = fake_iter
        mock_client.download_media = AsyncMock(return_value=None)
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["download_media"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1,
        })
        assert "нет медиа" in _text(result).lower()

    async def test_download_media_success(self, handlers_with_pool, tmp_path):
        """download_media where media downloads successfully."""
        handlers, pool, _ = handlers_with_pool
        mock_client = AsyncMock()
        mock_msg = MagicMock()

        async def fake_iter(*args, **kwargs):
            yield mock_msg

        mock_client.iter_messages = fake_iter
        # Return a path within the expected output directory
        data_dir = await asyncio.to_thread(_downloads_dir)
        local = str(data_dir / "test.jpg")
        mock_client.download_media = AsyncMock(return_value=local)
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["download_media"]({
            "phone": "+1111", "chat_id": "@ch", "message_id": 1,
        })
        text = _text(result)
        assert "загружено" in text.lower() or "test.jpg" in text

    async def test_participants_over_50(self, handlers_with_pool):
        """get_participants with >50 participants shows truncation."""
        handlers, pool, _ = handlers_with_pool
        mock_client = AsyncMock()
        participants = [
            SimpleNamespace(id=i, first_name=f"User{i}", last_name="", username=None)
            for i in range(55)
        ]
        mock_client.get_participants = AsyncMock(return_value=participants)
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["get_participants"]({
            "phone": "+1111", "chat_id": "@ch", "limit": 100,
        })
        text = _text(result)
        assert "ещё 5" in text or "55" in text

    async def test_edit_permissions_with_send_media(self, handlers_with_pool):
        """edit_permissions with send_media not None."""
        handlers, pool, _ = handlers_with_pool
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_permissions"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "send_messages": None, "send_media": True,
            "until_date": None, "confirm": True,
        })
        assert "обновлены" in _text(result).lower()

    async def test_edit_permissions_with_until_date(self, handlers_with_pool):
        """edit_permissions with until_date set."""
        handlers, pool, _ = handlers_with_pool
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["edit_permissions"]({
            "phone": "+1111", "chat_id": "@ch", "user_id": "@u",
            "send_messages": False, "send_media": None,
            "until_date": "2025-12-31T23:59:59", "confirm": True,
        })
        assert "обновлены" in _text(result).lower()


# ===========================================================================
# 29. dialogs tools — phone/perm gate paths
# ===========================================================================

