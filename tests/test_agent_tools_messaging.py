"""Tests for agent tools: messaging.py."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Account, TelegramCommandStatus
from tests.agent_tools_helpers import _get_tool_handlers, _text, assert_tool_text


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
    mock_client.send_reaction = AsyncMock()
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


def _setup_command_queue(mock_db, *, command_id: int = 41, status=TelegramCommandStatus.PENDING):
    repo = MagicMock()
    repo.find_active_by_type = AsyncMock(return_value=None)
    repo.create_command = AsyncMock(return_value=command_id)
    repo.get_command = AsyncMock(return_value=SimpleNamespace(id=command_id, status=status))
    mock_db.repos = MagicMock()
    mock_db.repos.telegram_commands = repo
    return repo


class TestSendMessage:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        await assert_tool_text(
            handlers["send_message"],
            {"phone": "+79001234567", "recipient": "@user", "text": "hi"},
            "CLI-режиме",
        )

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        await assert_tool_text(
            handlers["send_message"],
            {"phone": "+79001234567", "recipient": "@user", "text": "hello"},
            "confirm=true",
        )

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"](
            {"phone": "+79001234567", "recipient": "@user", "text": "hello", "confirm": True}
        )
        text = _text(result)
        assert "отправлено" in text

    @pytest.mark.anyio
    async def test_missing_recipient_or_text_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        await assert_tool_text(handlers["send_message"], {"phone": "+79001234567"}, "обязательны")


class TestSendReaction:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_reaction"](
            {"phone": "+79001234567", "chat_id": "@chat", "message_id": 1, "emoji": "👍"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_missing_args_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reaction"]({"phone": "+79001234567", "chat_id": "@chat"})
        assert "обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reaction"](
            {"phone": "+79001234567", "chat_id": "@chat", "message_id": 5, "emoji": "🔥"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        repo = _setup_command_queue(mock_db)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reaction"](
            {"phone": "+79001234567", "chat_id": "@chat", "message_id": 5, "emoji": "🔥", "confirm": True}
        )

        assert "Реакция '🔥' принята в очередь" in _text(result)
        assert "задача #41" in _text(result)
        repo.create_command.assert_awaited_once()
        mock_client.get_entity.assert_not_awaited()
        mock_client.send_reaction.assert_not_awaited()

    @pytest.mark.anyio
    async def test_with_confirm_reuses_existing_queue_item(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        repo = _setup_command_queue(mock_db, command_id=77, status=TelegramCommandStatus.RUNNING)
        repo.find_active_by_type.return_value = SimpleNamespace(id=77)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)

        result = await handlers["send_reaction"](
            {"phone": "+79001234567", "chat_id": "@chat", "message_id": 5, "emoji": "🔥", "confirm": True}
        )

        assert "задача #77" in _text(result)
        assert "статус running" in _text(result)
        repo.create_command.assert_not_awaited()
        mock_client.send_reaction.assert_not_awaited()


class TestSendReactions:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_reactions"](
            {"phone": "+79001234567", "chat_id": "@chat", "items_json": '[{"message_id": 1, "emoji": "👍"}]'}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_invalid_json_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reactions"](
            {"phone": "+79001234567", "chat_id": "@chat", "items_json": "not-json", "confirm": True}
        )
        assert "корректным JSON" in _text(result)

    @pytest.mark.anyio
    async def test_invalid_emoji_aborts_batch(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        repo = _setup_command_queue(mock_db)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reactions"](
            {
                "phone": "+79001234567",
                "chat_id": "@chat",
                "items_json": '[{"message_id": 1, "emoji": "👍"}, {"message_id": 2, "emoji": "not-an-emoji"}]',
                "confirm": True,
            }
        )
        # A single invalid item aborts the whole batch before any enqueue.
        assert "не принимает реакцию" in _text(result)
        repo.create_command.assert_not_awaited()

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reactions"](
            {
                "phone": "+79001234567",
                "chat_id": "@chat",
                "items_json": '[{"message_id": 1, "emoji": "👍"}, {"message_id": 2, "emoji": "🔥"}]',
            }
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_enqueues_each_item(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        repo = _setup_command_queue(mock_db)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reactions"](
            {
                "phone": "+79001234567",
                "chat_id": "@chat",
                "items_json": '[{"message_id": 10, "emoji": "👍"}, {"message_id": 11, "emoji": "🔥"}]',
                "confirm": True,
            }
        )
        assert "Поставлено или уже было в очереди реакций: 2 из 2" in _text(result)
        assert repo.create_command.await_count == 2
        mock_client.send_reaction.assert_not_awaited()

    @pytest.mark.anyio
    async def test_batch_over_limit_aborts_before_enqueue(self, mock_db):
        """A batch larger than MAX_REACTION_BATCH is rejected before any enqueue
        so a runaway/injected prompt cannot starve the DB write-lock (#736)."""
        from src.agent.tools.messaging_write import MAX_REACTION_BATCH

        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        repo = _setup_command_queue(mock_db)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        oversized = [{"message_id": i, "emoji": "👍"} for i in range(MAX_REACTION_BATCH + 1)]
        result = await handlers["send_reactions"](
            {
                "phone": "+79001234567",
                "chat_id": "@chat",
                "items_json": json.dumps(oversized),
                "confirm": True,
            }
        )
        assert f"не может превышать {MAX_REACTION_BATCH}" in _text(result)
        repo.create_command.assert_not_awaited()

    @pytest.mark.anyio
    async def test_fractional_message_id_aborts_batch(self, mock_db):
        """A fractional JSON number must not be silently truncated to a different
        message id — the batch is rejected before any enqueue (#736 Codex review)."""
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        repo = _setup_command_queue(mock_db)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_reactions"](
            {
                "phone": "+79001234567",
                "chat_id": "@chat",
                "items_json": '[{"message_id": 10.9, "emoji": "👍"}]',
                "confirm": True,
            }
        )
        assert "должен быть целым числом" in _text(result)
        repo.create_command.assert_not_awaited()


class TestEditMessage:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 1, "text": "new"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 1, "text": "new"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 5, "text": "updated text", "confirm": True}
        )
        text = _text(result)
        assert "отредактировано" in text

    @pytest.mark.anyio
    async def test_missing_message_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"]({"phone": "+79001234567", "chat_id": "123", "text": "new"})
        text = _text(result)
        assert "обязательны" in text


class TestDeleteMessage:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_message"]({"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_invalid_message_ids_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "abc,xyz"}
        )
        assert "валидные message_ids" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2,3"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "1"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_invalid_ids_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "abc"}
        )
        assert "валидные message_ids" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "1,2"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["pin_message"]({"phone": "+79001234567", "chat_id": "chat", "message_id": 1})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"]({"phone": "+79001234567", "chat_id": "chat", "message_id": 10})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"](
            {"phone": "+79001234567", "chat_id": "chat", "message_id": 10, "confirm": True}
        )
        assert "закреплено" in _text(result)


class TestUnpinMessage:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat", "confirm": True})
        assert "откреплено" in _text(result)


class TestGetParticipants:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_empty_participants(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_client.get_participants = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567"})
        assert "chat_id обязателен" in _text(result)


class TestKickParticipant:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["kick_participant"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "confirm": True}
        )
        assert "исключён" in _text(result)


class TestArchiveChat:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat", "confirm": True})
        assert "архивирован" in _text(result)


class TestMarkRead:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567"})
        assert "chat_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_with_pool_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "прочитанные" in _text(result)


class TestEditAdmin:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_promote_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "is_admin": True, "confirm": True}
        )
        assert "обновлены" in _text(result)


class TestEditPermissions:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "send_messages": False}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_flags_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        text = _text(result)
        assert "флаг" in text

    @pytest.mark.anyio
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


class TestTranslateMessage:
    @pytest.mark.anyio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["translate_message"]({})
        assert "message_db_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_message_not_found(self, mock_db):
        mock_db.repos.messages.get_by_id = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["translate_message"]({"message_db_id": 5})
        assert "не найдено" in _text(result)

    @pytest.mark.anyio
    async def test_empty_text(self, mock_db):
        mock_db.repos.messages.get_by_id = AsyncMock(return_value=SimpleNamespace(text="   "))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["translate_message"]({"message_db_id": 5})
        assert "нет текста" in _text(result)

    @pytest.mark.anyio
    async def test_success(self, mock_db):
        mock_db.repos.messages.get_by_id = AsyncMock(return_value=SimpleNamespace(text="Привет мир"))
        mock_db.repos.messages.update_translation = AsyncMock()
        mock_db.get_setting = AsyncMock(return_value=None)
        with patch("src.services.provider_service.build_provider_service", AsyncMock(return_value=MagicMock())), \
             patch("src.services.translation_service.TranslationService") as mock_svc:
            mock_svc.return_value.translate_batch = AsyncMock(return_value=[(5, "Hello world")])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["translate_message"]({"message_db_id": 5, "target": "en"})
        text = _text(result)
        assert "Hello world" in text
        mock_db.repos.messages.update_translation.assert_called_once_with(5, "en", "Hello world")

    @pytest.mark.anyio
    async def test_no_result(self, mock_db):
        mock_db.repos.messages.get_by_id = AsyncMock(return_value=SimpleNamespace(text="text"))
        mock_db.get_setting = AsyncMock(return_value=None)
        with patch("src.services.provider_service.build_provider_service", AsyncMock(return_value=MagicMock())), \
             patch("src.services.translation_service.TranslationService") as mock_svc:
            mock_svc.return_value.translate_batch = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["translate_message"]({"message_db_id": 5})
        assert "Перевод не выполнен" in _text(result)
