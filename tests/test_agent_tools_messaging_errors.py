"""Tests for src/agent/tools/messaging.py — exception paths, validation, entity resolution."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database import Database
from tests.agent_tools_helpers import _get_tool_handlers, _text


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.get_accounts = AsyncMock(return_value=[
        SimpleNamespace(id=1, phone="+79001234567", is_primary=True)
    ])
    return db


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    return pool


def _make_client():
    """Create a mock Telethon-like client."""
    client = MagicMock()
    client.send_message = AsyncMock()
    client.edit_message = AsyncMock()
    client.delete_messages = AsyncMock()
    client.forward_messages = AsyncMock()
    client.pin_message = AsyncMock()
    client.unpin_message = AsyncMock()
    client.download_media = AsyncMock(return_value="/tmp/test.png")
    client.get_participants = AsyncMock(return_value=[])
    client.edit_admin = AsyncMock()
    client.edit_permissions = AsyncMock()
    client.kick_participant = AsyncMock()
    client.get_broadcast_stats = AsyncMock()
    client.edit_folder = AsyncMock()
    client.send_read_acknowledge = AsyncMock()
    client.iter_messages = AsyncMock(return_value=[])
    return client


def _setup_resolve_entity(pool, client, entity=None):
    """Configure pool to return a working client+entity pair."""
    if entity is None:
        entity = SimpleNamespace(id=100)
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, None))
    pool.get_client_by_phone = AsyncMock(return_value=(MagicMock(), None))
    # Non-numeric path
    client.get_entity = AsyncMock(return_value=entity)
    return entity


# ---------------------------------------------------------------------------
# send_message — exception path (lines 60-61)
# ---------------------------------------------------------------------------


class TestSendMessageExceptions:
    @pytest.mark.anyio
    async def test_send_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.send_message = AsyncMock(side_effect=Exception("rate limited"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({
            "phone": "+79001234567",
            "recipient": "@test",
            "text": "Hello",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка отправки сообщения" in text
        assert "rate limited" in text


# ---------------------------------------------------------------------------
# edit_message — exception path (lines 104-105)
# ---------------------------------------------------------------------------


class TestEditMessageExceptions:
    @pytest.mark.anyio
    async def test_edit_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.edit_message = AsyncMock(side_effect=Exception("not owner"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 42,
            "text": "Updated",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка редактирования сообщения" in text
        assert "not owner" in text


# ---------------------------------------------------------------------------
# delete_message — validation + exception (lines 135, 150-151)
# ---------------------------------------------------------------------------


class TestDeleteMessageValidation:
    @pytest.mark.anyio
    async def test_no_valid_ids(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_ids": "abc,def",
            "confirm": True,
        })
        assert "не указаны валидные message_ids" in _text(result)

    @pytest.mark.anyio
    async def test_delete_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.delete_messages = AsyncMock(side_effect=Exception("forbidden"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_ids": "1,2",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка удаления сообщений" in text
        assert "forbidden" in text


# ---------------------------------------------------------------------------
# forward_messages — to_entity resolve error + exception (lines 196, 199-200)
# ---------------------------------------------------------------------------


class TestForwardMessagesErrors:
    @pytest.mark.anyio
    async def test_to_entity_resolve_error(self, mock_db, mock_pool):
        client = _make_client()
        # First resolve (from_chat) succeeds, second (to_chat) fails
        call_count = 0

        async def side_effect_get_entity(cid):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return SimpleNamespace(id=100)
            raise Exception("not found")

        client.get_entity = side_effect_get_entity
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=(client, None))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"]({
            "phone": "+79001234567",
            "from_chat": "@from",
            "to_chat": "@missing",
            "message_ids": "1,2",
            "confirm": True,
        })

        text = _text(result)
        assert "не удалось найти" in text

    @pytest.mark.anyio
    async def test_forward_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.forward_messages = AsyncMock(side_effect=Exception("flood"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"]({
            "phone": "+79001234567",
            "from_chat": "@from",
            "to_chat": "@to",
            "message_ids": "1",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка пересылки" in text
        assert "flood" in text


# ---------------------------------------------------------------------------
# pin_message — exception (lines 240-241)
# ---------------------------------------------------------------------------


class TestPinMessageExceptions:
    @pytest.mark.anyio
    async def test_pin_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.pin_message = AsyncMock(side_effect=Exception("no rights"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 10,
            "notify": True,
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка закрепления сообщения" in text


# ---------------------------------------------------------------------------
# unpin_message — exception (lines 280-281)
# ---------------------------------------------------------------------------


class TestUnpinMessageExceptions:
    @pytest.mark.anyio
    async def test_unpin_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.unpin_message = AsyncMock(side_effect=Exception("error"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка открепления сообщения" in text


# ---------------------------------------------------------------------------
# download_media — no message found + path outside dir + exception (lines 327, 329-330)
# ---------------------------------------------------------------------------


class TestDownloadMediaErrors:
    @pytest.mark.anyio
    async def test_message_not_found(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        async def empty_iter(*a, **kw):
            return
            yield  # make it an async generator

        client.iter_messages = empty_iter

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 999,
        })

        assert "не найдено" in _text(result)

    @pytest.mark.anyio
    async def test_no_media_in_message(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.download_media = AsyncMock(return_value=None)

        msg = SimpleNamespace(id=5, text="hello", media=None)

        async def single_msg(*a, **kw):
            yield msg

        client.iter_messages = single_msg

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 5,
        })

        assert "нет медиа" in _text(result)

    @pytest.mark.anyio
    async def test_download_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.download_media = AsyncMock(side_effect=Exception("timeout"))

        msg = SimpleNamespace(id=5, text="hello", media="photo")

        async def single_msg(*a, **kw):
            yield msg

        client.iter_messages = single_msg

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 5,
        })

        text = _text(result)
        assert "Ошибка загрузки медиа" in text


# ---------------------------------------------------------------------------
# get_participants — exception (lines 376-377)
# ---------------------------------------------------------------------------


class TestGetParticipantsExceptions:
    @pytest.mark.anyio
    async def test_exception_returns_error(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.get_participants = AsyncMock(side_effect=Exception("not a group"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        text = _text(result)
        assert "Ошибка получения участников" in text

    @pytest.mark.anyio
    async def test_with_participants(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        participant = SimpleNamespace(
            id=42, first_name="John", last_name="Doe", username="johndoe"
        )
        client.get_participants = AsyncMock(return_value=[participant])

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "limit": 50,
            "search": "john",
        })

        text = _text(result)
        assert "John Doe" in text
        assert "@johndoe" in text

    @pytest.mark.anyio
    async def test_empty_participants(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.get_participants = AsyncMock(return_value=[])

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        assert "Участники не найдены" in _text(result)


# ---------------------------------------------------------------------------
# edit_admin — user resolve error + exception (lines 421, 427-428)
# ---------------------------------------------------------------------------


class TestEditAdminErrors:
    @pytest.mark.anyio
    async def test_user_resolve_error(self, mock_db, mock_pool):
        client = _make_client()
        call_count = 0

        async def side_effect_get_entity(cid):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return SimpleNamespace(id=100)  # chat entity
            raise Exception("user not found")

        client.get_entity = side_effect_get_entity
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=(client, None))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@missing_user",
            "is_admin": True,
            "confirm": True,
        })

        text = _text(result)
        assert "не удалось найти" in text

    @pytest.mark.anyio
    async def test_edit_admin_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.edit_admin = AsyncMock(side_effect=Exception("no rights"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "is_admin": True,
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка изменения прав администратора" in text

    @pytest.mark.anyio
    async def test_demote_user(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "is_admin": False,
            "title": "Ex-admin",
            "confirm": True,
        })

        text = _text(result)
        assert "Права администратора обновлены" in text
        client.edit_admin.assert_awaited_once()
        call_kwargs = client.edit_admin.await_args.kwargs
        assert call_kwargs["is_admin"] is False
        assert call_kwargs["title"] == "Ex-admin"


# ---------------------------------------------------------------------------
# edit_permissions — validation + errors (lines 465, 480, 489-490)
# ---------------------------------------------------------------------------


class TestEditPermissionsErrors:
    @pytest.mark.anyio
    async def test_missing_chat_and_user_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({
            "phone": "+79001234567",
            "send_messages": True,
            "confirm": True,
        })
        assert "chat_id и user_id обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_user_resolve_error(self, mock_db, mock_pool):
        client = _make_client()
        call_count = 0

        async def side_effect_get_entity(cid):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return SimpleNamespace(id=100)
            raise Exception("no such user")

        client.get_entity = side_effect_get_entity
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=(client, None))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@ghost",
            "send_messages": True,
            "confirm": True,
        })

        text = _text(result)
        assert "не удалось найти" in text

    @pytest.mark.anyio
    async def test_edit_permissions_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.edit_permissions = AsyncMock(side_effect=Exception("denied"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "send_messages": False,
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка изменения ограничений" in text

    @pytest.mark.anyio
    async def test_no_flags_set_returns_error(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "confirm": True,
        })
        assert "укажите хотя бы один флаг" in _text(result)

    @pytest.mark.anyio
    async def test_with_until_date(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "send_messages": False,
            "until_date": "2025-12-31T23:59:59",
            "confirm": True,
        })

        text = _text(result)
        assert "Ограничения обновлены" in text


# ---------------------------------------------------------------------------
# kick_participant — user resolve error + exception (lines 529, 532-533)
# ---------------------------------------------------------------------------


class TestKickParticipantErrors:
    @pytest.mark.anyio
    async def test_user_resolve_error(self, mock_db, mock_pool):
        client = _make_client()
        call_count = 0

        async def side_effect_get_entity(cid):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return SimpleNamespace(id=100)
            raise Exception("unknown user")

        client.get_entity = side_effect_get_entity
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=(client, None))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@ghost",
            "confirm": True,
        })

        text = _text(result)
        assert "не удалось найти" in text

    @pytest.mark.anyio
    async def test_kick_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.kick_participant = AsyncMock(side_effect=Exception("no admin"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка исключения участника" in text


# ---------------------------------------------------------------------------
# get_broadcast_stats — exception (lines 589-590)
# ---------------------------------------------------------------------------


class TestGetBroadcastStatsErrors:
    @pytest.mark.anyio
    async def test_exception_returns_error(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.get_broadcast_stats = AsyncMock(side_effect=Exception("not admin"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        text = _text(result)
        assert "Ошибка получения статистики" in text

    @pytest.mark.anyio
    async def test_with_stats(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        stat_val = SimpleNamespace(current=1000, previous=800)
        stats = SimpleNamespace(
            followers=stat_val,
            views_per_post=stat_val,
            shares_per_post=None,
            reactions_per_post=None,
            forwards_per_post=None,
            period=SimpleNamespace(min_date="2025-01-01", max_date="2025-01-31"),
            enabled_notifications=True,
        )
        client.get_broadcast_stats = AsyncMock(return_value=stats)

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        text = _text(result)
        assert "1000" in text
        assert "800" in text
        assert "period" in text

    @pytest.mark.anyio
    async def test_stats_no_fields_falls_to_raw(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        stats = SimpleNamespace(
            followers=None,
            views_per_post=None,
            shares_per_post=None,
            reactions_per_post=None,
            forwards_per_post=None,
            period=None,
            enabled_notifications=None,
        )
        client.get_broadcast_stats = AsyncMock(return_value=stats)

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        text = _text(result)
        assert "raw" in text


# ---------------------------------------------------------------------------
# archive_chat — exception (lines 626-627)
# ---------------------------------------------------------------------------


class TestArchiveChatExceptions:
    @pytest.mark.anyio
    async def test_archive_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.edit_folder = AsyncMock(side_effect=Exception("already archived"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка архивирования" in text


# ---------------------------------------------------------------------------
# unarchive_chat — exception (lines 663-664)
# ---------------------------------------------------------------------------


class TestUnarchiveChatExceptions:
    @pytest.mark.anyio
    async def test_unarchive_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.edit_folder = AsyncMock(side_effect=Exception("not archived"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "confirm": True,
        })

        text = _text(result)
        assert "Ошибка разархивирования" in text


# ---------------------------------------------------------------------------
# mark_read — exception (lines 698-699)
# ---------------------------------------------------------------------------


class TestMarkReadExceptions:
    @pytest.mark.anyio
    async def test_mark_read_exception(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)
        client.send_read_acknowledge = AsyncMock(side_effect=Exception("error"))

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "max_id": 100,
        })

        text = _text(result)
        assert "Ошибка отметки сообщений" in text

    @pytest.mark.anyio
    async def test_mark_read_success(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "max_id": 50,
        })

        text = _text(result)
        assert "отмечены как прочитанные" in text


# ---------------------------------------------------------------------------
# read_messages — full tool coverage (lines 715-757)
# ---------------------------------------------------------------------------


class TestReadMessagesTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["read_messages"]({"chat_id": "@test"})
        assert "требует Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
        })
        assert "chat_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_with_messages(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        msg = SimpleNamespace(
            id=1, sender_id=42, date=None, text="Hello world"
        )

        async def iter_msgs(*a, **kw):
            yield msg

        client.iter_messages = iter_msgs

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "limit": 10,
        })

        text = _text(result)
        assert "Hello world" in text
        assert "1 сообщений" in text

    @pytest.mark.anyio
    async def test_no_text_messages(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        msg = SimpleNamespace(id=1, sender_id=42, date=None, text=None)

        async def iter_msgs(*a, **kw):
            yield msg

        client.iter_messages = iter_msgs

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        assert "Сообщений с текстом не найдено" in _text(result)

    @pytest.mark.anyio
    async def test_with_date_and_sender(self, mock_db, mock_pool):
        from datetime import datetime

        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        msg = SimpleNamespace(
            id=5,
            sender_id=42,
            date=datetime(2025, 6, 15, 12, 30),
            text="Dated message",
        )

        async def iter_msgs(*a, **kw):
            yield msg

        client.iter_messages = iter_msgs

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "limit": 100,
        })

        text = _text(result)
        assert "2025-06-15 12:30" in text
        assert "[id:42]" in text

    @pytest.mark.anyio
    async def test_exception_returns_error(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        def _raise(*args, **kwargs):
            raise Exception("flood")

        client.iter_messages = _raise

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })

        text = _text(result)
        assert "Ошибка чтения сообщений" in text

    @pytest.mark.anyio
    async def test_invalid_limit_defaults_to_100(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        msg = SimpleNamespace(id=1, sender_id=42, date=None, text="Hi")

        async def iter_msgs(*a, **kw):
            yield msg

        client.iter_messages = iter_msgs

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "limit": "not_a_number",
        })

        text = _text(result)
        # Should still work with default limit=100
        assert "1 сообщений" in text

    @pytest.mark.anyio
    async def test_character_budget_truncation(self, mock_db, mock_pool):
        client = _make_client()
        _setup_resolve_entity(mock_pool, client)

        long_text = "x" * 1000
        messages = []
        for i in range(200):
            messages.append(SimpleNamespace(
                id=i, sender_id=42, date=None, text=long_text
            ))

        msg_iter = iter(messages)

        async def iter_msgs(*a, **kw):
            for m in msg_iter:
                yield m

        client.iter_messages = iter_msgs

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["read_messages"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "limit": 500,
        })

        text = _text(result)
        assert "обрезан" in text


# ---------------------------------------------------------------------------
# Additional validation coverage
# ---------------------------------------------------------------------------


class TestSendValidation:
    @pytest.mark.anyio
    async def test_missing_recipient(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({
            "phone": "+79001234567",
            "text": "Hello",
            "confirm": True,
        })
        assert "recipient и text обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_missing_text(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({
            "phone": "+79001234567",
            "recipient": "@test",
            "confirm": True,
        })
        assert "recipient и text обязательны" in _text(result)


class TestEditValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id, message_id и text обязательны" in _text(result)


class TestDeleteValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id и message_ids обязательны" in _text(result)


class TestForwardValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "from_chat, to_chat и message_ids обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_no_valid_ids(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"]({
            "phone": "+79001234567",
            "from_chat": "@a",
            "to_chat": "@b",
            "message_ids": "abc",
            "confirm": True,
        })
        assert "не указаны валидные message_ids" in _text(result)


class TestPinValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id и message_id обязательны" in _text(result)


class TestUnpinValidation:
    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id обязателен" in _text(result)


class TestDownloadValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })
        assert "chat_id и message_id обязательны" in _text(result)


class TestParticipantsValidation:
    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({
            "phone": "+79001234567",
        })
        assert "chat_id обязателен" in _text(result)


class TestEditAdminValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id и user_id обязательны" in _text(result)


class TestKickValidation:
    @pytest.mark.anyio
    async def test_missing_fields(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id и user_id обязательны" in _text(result)


class TestBroadcastStatsValidation:
    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({
            "phone": "+79001234567",
        })
        assert "chat_id обязателен" in _text(result)


class TestArchiveValidation:
    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id обязателен" in _text(result)


class TestUnarchiveValidation:
    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({
            "phone": "+79001234567",
            "confirm": True,
        })
        assert "chat_id обязателен" in _text(result)


class TestMarkReadValidation:
    @pytest.mark.anyio
    async def test_missing_chat_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({
            "phone": "+79001234567",
        })
        assert "chat_id обязателен" in _text(result)


# ---------------------------------------------------------------------------
# Confirmation gate tests for messaging tools
# ---------------------------------------------------------------------------


class TestConfirmationGates:
    @pytest.mark.anyio
    async def test_send_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({
            "phone": "+79001234567",
            "recipient": "@test",
            "text": "Hi",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_edit_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 1,
            "text": "Hi",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_delete_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_ids": "1",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_forward_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"]({
            "phone": "+79001234567",
            "from_chat": "@a",
            "to_chat": "@b",
            "message_ids": "1",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_pin_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "message_id": 1,
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_unpin_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_archive_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_unarchive_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({
            "phone": "+79001234567",
            "chat_id": "@test",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_edit_admin_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_edit_permissions_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
            "send_messages": True,
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_kick_requires_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({
            "phone": "+79001234567",
            "chat_id": "@test",
            "user_id": "@user",
        })
        assert "confirm=true" in _text(result).lower()
