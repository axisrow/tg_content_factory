"""Tests for agent tools: messaging.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Account
from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_account(phone="+79001234567", is_active=True, is_primary=True):
    acc = MagicMock(spec=Account)
    acc.id = 1
    acc.phone = phone
    acc.is_active = is_active
    acc.is_primary = is_primary
    acc.session_string = "fake"
    return acc


def _make_mock_pool():
    """Create a mock client pool that returns a native client."""
    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=123456))
    mock_client.send_message = AsyncMock()
    mock_client.edit_message = AsyncMock()
    mock_client.delete_messages = AsyncMock(return_value=[MagicMock(pts_count=1)])
    mock_client.forward_messages = AsyncMock(return_value=[MagicMock()])
    mock_client.pin_message = AsyncMock()
    mock_client.unpin_message = AsyncMock()
    mock_client.get_participants = AsyncMock(return_value=[])
    mock_client.kick_participant = AsyncMock()
    mock_client.edit_folder = AsyncMock()
    mock_client.send_read_acknowledge = AsyncMock()
    mock_client.edit_admin = AsyncMock()
    mock_client.edit_permissions = AsyncMock()

    mock_session = MagicMock()
    mock_pool = MagicMock()
    mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, None))
    mock_pool.get_client_by_phone = AsyncMock(return_value=(mock_session, None))
    mock_pool.resolve_dialog_entity = AsyncMock(return_value=MagicMock(id=123456))
    return mock_pool, mock_client


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_message"]({"phone": "+79001234567", "recipient": "@user", "text": "hi"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({"phone": "+79001234567", "recipient": "@user", "text": "hello"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"](
            {"phone": "+79001234567", "recipient": "@user", "text": "hello", "confirm": True}
        )
        text = _text(result)
        assert "отправлено" in text

    @pytest.mark.asyncio
    async def test_missing_recipient_or_text_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({"phone": "+79001234567"})
        text = _text(result)
        assert "обязательны" in text


class TestEditMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 1, "text": "new"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 1, "text": "new"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 5, "text": "updated text", "confirm": True}
        )
        text = _text(result)
        assert "отредактировано" in text

    @pytest.mark.asyncio
    async def test_missing_message_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"]({"phone": "+79001234567", "chat_id": "123", "text": "new"})
        text = _text(result)
        assert "обязательны" in text


class TestDeleteMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_message"]({"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_message_ids_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "abc,xyz"}
        )
        assert "валидные message_ids" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2,3"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2", "confirm": True}
        )
        text = _text(result)
        assert "Удалено" in text


class TestForwardMessages:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "1"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_ids_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "abc"}
        )
        assert "валидные message_ids" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "1,2"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {
                "phone": "+79001234567",
                "from_chat": "chatA",
                "to_chat": "chatB",
                "message_ids": "1,2",
                "confirm": True,
            }
        )
        assert "Переслано" in _text(result)


class TestPinMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["pin_message"]({"phone": "+79001234567", "chat_id": "chat", "message_id": 1})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"]({"phone": "+79001234567", "chat_id": "chat", "message_id": 10})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"](
            {"phone": "+79001234567", "chat_id": "chat", "message_id": 10, "confirm": True}
        )
        assert "закреплено" in _text(result)


class TestUnpinMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat", "confirm": True})
        assert "откреплено" in _text(result)


class TestGetParticipants:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_participants(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_client.get_participants = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_participants(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        participant = MagicMock()
        participant.id = 111
        participant.first_name = "John"
        participant.last_name = "Doe"
        participant.username = "johndoe"
        mock_client.get_participants = AsyncMock(return_value=[participant])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        text = _text(result)
        assert "111" in text
        assert "John" in text

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567"})
        assert "chat_id обязателен" in _text(result)


class TestKickParticipant:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["kick_participant"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "confirm": True}
        )
        assert "исключён" in _text(result)


class TestArchiveChat:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat", "confirm": True})
        assert "архивирован" in _text(result)


class TestMarkRead:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567"})
        assert "chat_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pool_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "прочитанные" in _text(result)


class TestEditAdmin:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_promote_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "is_admin": True, "confirm": True}
        )
        assert "обновлены" in _text(result)


class TestEditPermissions:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "send_messages": False}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_flags_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        text = _text(result)
        assert "флаг" in text

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {
                "phone": "+79001234567",
                "chat_id": "chat",
                "user_id": "111",
                "send_messages": False,
                "confirm": True,
            }
        )
        assert "обновлены" in _text(result)
