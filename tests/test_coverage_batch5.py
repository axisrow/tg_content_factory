"""Coverage batch 5 — messaging.py remaining tools, agent/manager.py paths,
deepagents_sync remaining tools, and agent_provider_service.py paths.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    return db


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []
    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config, **kwargs)
    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    return result["content"][0]["text"]


def _make_account(acc_id=1, phone="+79001234567", is_active=True):
    a = MagicMock()
    a.id = acc_id
    a.phone = phone
    a.is_active = is_active
    a.is_primary = True
    a.flood_wait_until = None
    return a


def _make_pool_with_client():
    """Create a mock pool that returns a mock native client."""
    mock_client = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, None))
    return mock_pool, mock_client


def _build_sync_tools(mock_db, config=None, client_pool=None):
    from src.agent.tools.deepagents_sync import build_deepagents_tools

    return {t.__name__: t for t in build_deepagents_tools(mock_db, client_pool=client_pool, config=config)}


# ===========================================================================
# messaging.py — edit_admin
# ===========================================================================


class TestEditAdminTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chan", "user_id": "user1", "confirm": True}
        )
        text = _text(result).lower()
        assert "pool" in text or "недоступен" in text or "изменение прав" in text

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chan", "user_id": "user1"})
        assert "confirm" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "user_id": "user1", "confirm": True})
        assert "обязательн" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_promote_success(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.edit_admin = AsyncMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chan", "user_id": "user1", "is_admin": True, "confirm": True}
        )
        assert "обновлены" in _text(result).lower() or "администратора" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_with_title(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.edit_admin = AsyncMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {
                "phone": "+79001234567", "chat_id": "chan", "user_id": "user1",
                "is_admin": True, "title": "Moderator", "confirm": True,
            }
        )
        assert "обновлены" in _text(result).lower() or "администратора" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_error_propagates(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(side_effect=Exception("permission denied"))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chan", "user_id": "user1", "confirm": True}
        )
        assert "Ошибка" in _text(result)

    @pytest.mark.asyncio
    async def test_client_not_found(self, mock_db):
        mock_pool = MagicMock()
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=None)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chan", "user_id": "user1", "confirm": True}
        )
        assert "не найден" in _text(result).lower()


# ===========================================================================
# messaging.py — edit_permissions
# ===========================================================================


class TestEditPermissionsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "c", "user_id": "u", "send_messages": False, "confirm": True}
        )
        assert "недоступен" in _text(result).lower() or "pool" in _text(result).lower() or "Изменение" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "c", "user_id": "u", "send_messages": False}
        )
        assert "confirm" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_flags_returns_error(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "c", "user_id": "u", "confirm": True}
        )
        assert "флаг" in _text(result).lower() or "ограничени" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_success_path(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.edit_permissions = AsyncMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "c", "user_id": "u", "send_messages": False, "confirm": True}
        )
        assert "обновлены" in _text(result).lower() or "ограничени" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_with_until_date(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.edit_permissions = AsyncMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {
                "phone": "+79001234567", "chat_id": "c", "user_id": "u",
                "send_messages": False, "until_date": "2030-01-01T00:00:00", "confirm": True,
            }
        )
        assert "обновлены" in _text(result).lower() or "ограничени" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_error_propagates(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(side_effect=Exception("forbidden"))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "c", "user_id": "u", "send_messages": False, "confirm": True}
        )
        assert "Ошибка" in _text(result)


# ===========================================================================
# messaging.py — get_broadcast_stats
# ===========================================================================


class TestGetBroadcastStatsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567", "chat_id": "chan"})
        text = _text(result).lower()
        assert "недоступен" in text or "pool" in text or "статистик" in text

    @pytest.mark.asyncio
    async def test_missing_chat_id(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567"})
        assert "обязателен" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_success_with_stats_fields(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        stats_mock = MagicMock()
        followers_val = MagicMock()
        followers_val.current = 1000
        followers_val.previous = 900
        stats_mock.followers = followers_val
        stats_mock.views_per_post = None
        stats_mock.shares_per_post = None
        stats_mock.reactions_per_post = None
        stats_mock.forwards_per_post = None
        period_mock = MagicMock()
        period_mock.min_date = "2026-01-01"
        period_mock.max_date = "2026-03-27"
        stats_mock.period = period_mock
        stats_mock.enabled_notifications = "0.5"
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.get_broadcast_stats = AsyncMock(return_value=stats_mock)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567", "chat_id": "chan"})
        text = _text(result)
        assert "Статистика" in text
        assert "followers" in text or "1000" in text

    @pytest.mark.asyncio
    async def test_stats_without_current_field(self, mock_db):
        """Branch: val.current is None → use str(val)."""
        mock_pool, mock_client = _make_pool_with_client()
        stats_mock = MagicMock()
        plain_val = MagicMock()
        plain_val.current = None
        stats_mock.followers = plain_val
        stats_mock.views_per_post = None
        stats_mock.shares_per_post = None
        stats_mock.reactions_per_post = None
        stats_mock.forwards_per_post = None
        stats_mock.period = None
        stats_mock.enabled_notifications = None
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.get_broadcast_stats = AsyncMock(return_value=stats_mock)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567", "chat_id": "chan"})
        assert "Статистика" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_stats_falls_back_to_raw(self, mock_db):
        """Branch: no fields parsed → fields['raw'] = str(stats)."""
        mock_pool, mock_client = _make_pool_with_client()
        stats_mock = MagicMock()
        stats_mock.followers = None
        stats_mock.views_per_post = None
        stats_mock.shares_per_post = None
        stats_mock.reactions_per_post = None
        stats_mock.forwards_per_post = None
        stats_mock.period = None
        stats_mock.enabled_notifications = None
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.get_broadcast_stats = AsyncMock(return_value=stats_mock)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567", "chat_id": "chan"})
        assert "raw" in _text(result) or "Статистика" in _text(result)

    @pytest.mark.asyncio
    async def test_error_propagates(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(side_effect=Exception("chan error"))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567", "chat_id": "chan"})
        assert "Ошибка" in _text(result)

    @pytest.mark.asyncio
    async def test_client_not_found(self, mock_db):
        mock_pool = MagicMock()
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=None)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_broadcast_stats"]({"phone": "+79001234567", "chat_id": "chan"})
        assert "не найден" in _text(result).lower()


# ===========================================================================
# messaging.py — unarchive_chat
# ===========================================================================


class TestUnarchiveChatTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["unarchive_chat"]({"phone": "+79001234567", "chat_id": "chan", "confirm": True})
        text = _text(result).lower()
        assert "недоступен" in text or "pool" in text or "разархивирование" in text

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({"phone": "+79001234567", "chat_id": "chan"})
        assert "confirm" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_chat_id(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({"phone": "+79001234567", "confirm": True})
        assert "обязателен" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_success(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.edit_folder = AsyncMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({"phone": "+79001234567", "chat_id": "chan", "confirm": True})
        assert "разархивирован" in _text(result).lower()
        mock_client.edit_folder.assert_awaited_once_with(mock_client.get_entity.return_value, 0)

    @pytest.mark.asyncio
    async def test_error_propagates(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(side_effect=Exception("fail"))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unarchive_chat"]({"phone": "+79001234567", "chat_id": "chan", "confirm": True})
        assert "Ошибка" in _text(result)


# ===========================================================================
# messaging.py — download_media
# ===========================================================================


class TestDownloadMediaTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["download_media"]({"phone": "+79001234567", "chat_id": "me", "message_id": 1})
        assert "недоступен" in _text(result).lower() or "pool" in _text(result).lower() or "Загрузка" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_chat_id(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({"phone": "+79001234567", "message_id": 1})
        assert "обязательн" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_message_not_found(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())

        async def _iter_empty(*a, **kw):
            return
            yield  # make it an async generator

        mock_client.iter_messages = _iter_empty
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({"phone": "+79001234567", "chat_id": "me", "message_id": 42})
        assert "не найдено" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_no_media_in_message(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        msg_mock = MagicMock()

        async def _iter_with_msg(*a, **kw):
            yield msg_mock

        mock_client.iter_messages = _iter_with_msg
        mock_client.download_media = AsyncMock(return_value=None)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({"phone": "+79001234567", "chat_id": "me", "message_id": 42})
        assert "нет медиа" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_error_propagates(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(side_effect=Exception("network error"))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({"phone": "+79001234567", "chat_id": "me", "message_id": 1})
        assert "Ошибка" in _text(result)

    @pytest.mark.asyncio
    async def test_client_not_found(self, mock_db):
        mock_pool = MagicMock()
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=None)
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["download_media"]({"phone": "+79001234567", "chat_id": "me", "message_id": 1})
        assert "не найден" in _text(result).lower()


# ===========================================================================
# messaging.py — mark_read
# ===========================================================================


class TestMarkReadTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chan"})
        text = _text(result).lower()
        assert "недоступен" in text or "pool" in text or "прочитанн" in text

    @pytest.mark.asyncio
    async def test_missing_chat_id(self, mock_db):
        mock_pool, _ = _make_pool_with_client()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567"})
        assert "обязателен" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_success(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.send_read_acknowledge = AsyncMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chan", "max_id": 100})
        assert "прочитанны" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_error_propagates(self, mock_db):
        mock_pool, mock_client = _make_pool_with_client()
        mock_client.get_entity = AsyncMock(side_effect=Exception("flood"))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chan"})
        assert "Ошибка" in _text(result)


# ===========================================================================
# deepagents_sync — list_channels / get_channel_stats
# ===========================================================================


class TestDeepagentsSyncChannels:
    def test_list_channels_no_channels(self, mock_db):
        mock_db.get_channels = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_channels"]()
        assert "не найдены" in result.lower()

    def test_list_channels_with_channels(self, mock_db):
        ch = SimpleNamespace(title="Chan1", channel_id=111, is_active=True)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_channels"]()
        assert "Chan1" in result
        assert "111" in result

    def test_list_channels_error(self, mock_db):
        mock_db.get_channels = AsyncMock(side_effect=Exception("db fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_channels"]()
        assert "Ошибка" in result

    def test_get_channel_stats_empty(self, mock_db):
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(return_value={})
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_channel_stats"]()
        assert "не собрана" in result.lower()

    def test_get_channel_stats_with_data(self, mock_db):
        stat = SimpleNamespace(subscriber_count=500)
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(return_value={10: stat})
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_channel_stats"]()
        assert "500" in result

    def test_get_channel_stats_error(self, mock_db):
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(side_effect=Exception("oops"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_channel_stats"]()
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — moderation tools
# ===========================================================================


class TestDeepagentsSyncModeration:
    def test_list_pending_moderation_empty(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_pending_moderation"]()
        assert "нет черновиков" in result.lower()

    def test_list_pending_moderation_with_runs(self, mock_db):
        run = SimpleNamespace(id=1, pipeline_id=2, generated_text="sample text")
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[run])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_pending_moderation"]()
        assert "run_id=1" in result

    def test_approve_run_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["approve_run"](5)
        assert "одобрен" in result.lower()

    def test_approve_run_error(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(side_effect=Exception("fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["approve_run"](5)
        assert "Ошибка" in result

    def test_reject_run_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["reject_run"](3)
        assert "отклонён" in result.lower()

    def test_bulk_approve_runs(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_approve_runs"]("1,2,3")
        assert "Одобрено: 3" in result

    def test_bulk_reject_runs(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_reject_runs"]("10,20")
        assert "Отклонено: 2" in result


# ===========================================================================
# deepagents_sync — accounts / scheduler / threads
# ===========================================================================


class TestDeepagentsSyncAccounts:
    def test_list_accounts_no_accounts(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_accounts"]()
        assert "не найдены" in result.lower()

    def test_list_accounts_with_data(self, mock_db):
        acc = SimpleNamespace(id=1, phone="+79001234567", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_accounts"]()
        assert "+79001234567" in result

    def test_toggle_account_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["toggle_account"](999)
        assert "не найден" in result.lower()

    def test_toggle_account_success(self, mock_db):
        acc = SimpleNamespace(id=1, phone="+79001234567", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.set_account_active = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["toggle_account"](1)
        assert "переключён" in result.lower()

    def test_delete_account_success(self, mock_db):
        mock_db.delete_account = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["delete_account"](1)
        assert "удалён" in result.lower()

    def test_get_flood_status_no_accounts(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_flood_status"]()
        assert "не найдены" in result.lower()

    def test_get_flood_status_with_flood(self, mock_db):
        acc = SimpleNamespace(id=1, phone="+79001234567", flood_wait_until="2030-01-01")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_flood_status"]()
        assert "2030-01-01" in result


class TestDeepagentsSyncThreads:
    def test_list_agent_threads_empty(self, mock_db):
        mock_db.get_agent_threads = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_agent_threads"]()
        assert "не найдены" in result.lower()

    def test_list_agent_threads_with_data(self, mock_db):
        mock_db.get_agent_threads = AsyncMock(return_value=[{"id": 1, "title": "My Thread"}])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_agent_threads"]()
        assert "My Thread" in result

    def test_create_agent_thread(self, mock_db):
        mock_db.create_agent_thread = AsyncMock(return_value=42)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["create_agent_thread"]("Test")
        assert "id=42" in result

    def test_delete_agent_thread_success(self, mock_db):
        mock_db.delete_agent_thread = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["delete_agent_thread"](7)
        assert "удалён" in result.lower()

    def test_rename_agent_thread_success(self, mock_db):
        mock_db.rename_agent_thread = AsyncMock()
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["rename_agent_thread"](3, "New Name")
        assert "New Name" in result

    def test_get_thread_messages_empty(self, mock_db):
        mock_db.get_agent_messages = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_thread_messages"](5)
        assert "нет сообщений" in result.lower()

    def test_get_thread_messages_with_data(self, mock_db):
        msg = {"role": "user", "content": "hello there"}
        mock_db.get_agent_messages = AsyncMock(return_value=[msg])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_thread_messages"](5)
        assert "hello there" in result


# ===========================================================================
# deepagents_sync — settings / system info
# ===========================================================================


class TestDeepagentsSyncSettings:
    def test_get_settings_success(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value="60")
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_settings"]()
        assert "Настройки" in result

    def test_get_settings_error(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("db fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_settings"]()
        assert "Ошибка" in result

    def test_get_system_info_success(self, mock_db):
        mock_db.get_stats = AsyncMock(return_value={"messages": 100, "channels": 5})
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_system_info"]()
        assert "messages" in result or "channels" in result

    def test_get_system_info_error(self, mock_db):
        mock_db.get_stats = AsyncMock(side_effect=Exception("stats fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_system_info"]()
        assert "Ошибка" in result


# ===========================================================================
# agent/manager.py — _build_prompt_stats_only edge cases
# ===========================================================================


class TestBuildPromptStatsOnly:
    def _make_manager(self, mock_db):
        from src.agent.manager import AgentManager
        from src.config import AppConfig

        config = AppConfig()
        return AgentManager(mock_db, config=config)

    def test_empty_history(self, mock_db):
        mgr = self._make_manager(mock_db)
        stats = mgr._build_prompt_stats_only([], "hello")
        assert stats["total_msgs"] == 0
        assert stats["kept_msgs"] == 0
        assert stats["total_chars"] == len("hello")
        assert stats["prompt_chars"] > 0

    def test_small_history_kept(self, mock_db):
        mgr = self._make_manager(mock_db)
        history = [
            {"role": "user", "content": "first question"},
            {"role": "assistant", "content": "first answer"},
        ]
        stats = mgr._build_prompt_stats_only(history, "second question")
        assert stats["total_msgs"] == 2
        assert stats["kept_msgs"] == 2
        assert stats["total_chars"] > 0

    def test_large_history_trimmed(self, mock_db):
        mgr = self._make_manager(mock_db)
        # Create history that exceeds budget
        big_content = "x" * 60_000
        history = [{"role": "user", "content": big_content} for _ in range(10)]
        stats = mgr._build_prompt_stats_only(history, "query")
        assert stats["total_msgs"] == 10
        # Not all history can fit in budget
        assert stats["kept_msgs"] < 10

    def test_single_message_fits(self, mock_db):
        mgr = self._make_manager(mock_db)
        history = [{"role": "user", "content": "short"}]
        stats = mgr._build_prompt_stats_only(history, "next")
        assert stats["kept_msgs"] == 1


# ===========================================================================
# agent/manager.py — _extract_result_text
# ===========================================================================


class TestExtractResultText:
    def _make_backend(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        db = MagicMock(spec=Database)
        return DeepagentsBackend(db, AppConfig())

    def test_string_input(self):
        backend = self._make_backend()
        assert backend._extract_result_text("hello") == "hello"

    def test_dict_with_messages_string_content(self):
        backend = self._make_backend()
        msg = MagicMock()
        msg.content = "answer text"
        result = backend._extract_result_text({"messages": [msg]})
        assert result == "answer text"

    def test_dict_with_messages_list_content(self):
        backend = self._make_backend()
        msg = MagicMock()
        msg.content = [{"text": "part1"}, {"text": "part2"}]
        result = backend._extract_result_text({"messages": [msg]})
        assert "part1" in result
        assert "part2" in result

    def test_dict_with_no_messages(self):
        backend = self._make_backend()
        result = backend._extract_result_text({"messages": []})
        # Falls back to str(result)
        assert isinstance(result, str)

    def test_dict_without_messages_key(self):
        backend = self._make_backend()
        result = backend._extract_result_text({"other": "val"})
        assert isinstance(result, str)

    def test_non_dict_non_string(self):
        backend = self._make_backend()
        result = backend._extract_result_text(42)
        assert result == "42"


# ===========================================================================
# agent/manager.py — DeepagentsBackend properties
# ===========================================================================


class TestDeepagentsBackendProperties:
    def _make_backend(self, config=None):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        db = MagicMock(spec=Database)
        return DeepagentsBackend(db, config or AppConfig())

    def test_fallback_model_uses_last_used(self):
        backend = self._make_backend()
        backend._last_used_model = "openai:gpt-4"
        assert backend.fallback_model == "openai:gpt-4"

    def test_fallback_provider_uses_last_used(self):
        backend = self._make_backend()
        backend._last_used_provider = "openai"
        assert backend.fallback_provider == "openai"

    def test_available_when_preflight_set_true(self):
        backend = self._make_backend()
        backend._preflight_available = True
        assert backend.available is True

    def test_available_when_preflight_set_false(self):
        backend = self._make_backend()
        backend._preflight_available = False
        assert backend.available is False

    def test_configured_false_when_no_models(self):
        backend = self._make_backend()
        backend._cached_db_configs = []
        assert backend.configured is False

    def test_init_error_returns_stored(self):
        backend = self._make_backend()
        backend._init_error = "some error"
        assert backend.init_error == "some error"

    def test_provider_from_model_with_colon(self):
        backend = self._make_backend()
        assert backend._provider_from_model("openai:gpt-4") == "openai"

    def test_provider_from_model_without_colon_returns_none(self):
        backend = self._make_backend()
        assert backend._provider_from_model("gpt-4") is None

    def test_classify_probe_failure_timeout(self):
        backend = self._make_backend()
        exc = Exception("Connection timed out")
        status, reason = backend._classify_probe_failure(exc)
        assert status == "unknown"
        assert "timed out" in reason.lower()

    def test_classify_probe_failure_unauthorized(self):
        backend = self._make_backend()
        exc = Exception("Unauthorized access 401")
        status, reason = backend._classify_probe_failure(exc)
        assert status == "unknown"

    def test_classify_probe_failure_other(self):
        backend = self._make_backend()
        exc = Exception("tool call loop incomplete")
        status, reason = backend._classify_probe_failure(exc)
        assert status == "unsupported"


# ===========================================================================
# agent_provider_service.py — core methods
# ===========================================================================


class TestAgentProviderService:
    def _make_service(self):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        db = MagicMock(spec=Database)
        db.get_setting = AsyncMock(return_value=None)
        db.set_setting = AsyncMock()
        return AgentProviderService(db, AppConfig()), db

    def _make_cfg(self, provider="openai", model="gpt-4o", **kwargs):
        from src.agent.provider_registry import ProviderRuntimeConfig

        return ProviderRuntimeConfig(
            provider=provider,
            enabled=True,
            priority=0,
            selected_model=model,
            plain_fields={"base_url": ""},
            secret_fields={"api_key": "sk-test"},
            **kwargs,
        )

    @pytest.mark.asyncio
    async def test_load_provider_configs_empty(self):
        svc, db = self._make_service()
        configs = await svc.load_provider_configs()
        assert configs == []

    @pytest.mark.asyncio
    async def test_load_provider_configs_invalid_json(self):
        svc, db = self._make_service()
        db.get_setting = AsyncMock(return_value="not-json{{{")
        configs = await svc.load_provider_configs()
        assert configs == []

    @pytest.mark.asyncio
    async def test_load_provider_configs_not_list(self):
        svc, db = self._make_service()
        db.get_setting = AsyncMock(return_value=json.dumps({"key": "value"}))
        configs = await svc.load_provider_configs()
        assert configs == []

    @pytest.mark.asyncio
    async def test_load_model_cache_empty(self):
        svc, db = self._make_service()
        cache = await svc.load_model_cache()
        assert cache == {}

    @pytest.mark.asyncio
    async def test_load_model_cache_invalid_json(self):
        svc, db = self._make_service()
        db.get_setting = AsyncMock(return_value="{{broken")
        cache = await svc.load_model_cache()
        assert cache == {}

    def test_validate_provider_config_unknown_provider(self):
        svc, _ = self._make_service()
        from src.agent.provider_registry import ProviderRuntimeConfig

        cfg = ProviderRuntimeConfig(
            provider="unknown_xyz",
            enabled=True,
            priority=0,
            selected_model="model",
            plain_fields={},
            secret_fields={},
        )
        error = svc.validate_provider_config(cfg)
        assert "Unknown provider" in error

    def test_validate_provider_config_missing_required_secret(self):
        svc, _ = self._make_service()
        from src.agent.provider_registry import ProviderRuntimeConfig

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4o",
            plain_fields={"base_url": ""},
            secret_fields={},  # missing api_key
        )
        error = svc.validate_provider_config(cfg)
        assert "API key" in error or "required" in error.lower()

    def test_validate_provider_config_missing_model(self):
        svc, _ = self._make_service()
        from src.agent.provider_registry import ProviderRuntimeConfig

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="",
            plain_fields={"base_url": ""},
            secret_fields={"api_key": "sk-test"},
        )
        error = svc.validate_provider_config(cfg)
        assert "Model" in error or "required" in error.lower()

    def test_validate_provider_config_valid(self):
        svc, _ = self._make_service()
        cfg = self._make_cfg(provider="openai", model="gpt-4o")
        error = svc.validate_provider_config(cfg)
        assert error == ""

    def test_config_fingerprint_stable(self):
        svc, _ = self._make_service()
        cfg = self._make_cfg()
        fp1 = svc.config_fingerprint(cfg)
        fp2 = svc.config_fingerprint(cfg)
        assert fp1 == fp2
        assert len(fp1) == 64  # sha256 hex

    def test_config_fingerprint_different_for_different_models(self):
        svc, _ = self._make_service()
        cfg1 = self._make_cfg(model="gpt-4o")
        cfg2 = self._make_cfg(model="gpt-4o-mini")
        assert svc.config_fingerprint(cfg1) != svc.config_fingerprint(cfg2)

    def test_is_compatibility_record_fresh_no_tested_at(self):
        from src.services.agent_provider_service import ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        record = ProviderModelCompatibilityRecord(model="gpt-4o", status="supported", tested_at="")
        assert not svc.is_compatibility_record_fresh(record)

    def test_is_compatibility_record_fresh_recent(self):
        from src.services.agent_provider_service import ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        tested_at = datetime.now(UTC).isoformat()
        record = ProviderModelCompatibilityRecord(model="gpt-4o", status="supported", tested_at=tested_at)
        assert svc.is_compatibility_record_fresh(record)

    def test_is_compatibility_record_fresh_old(self):
        from src.services.agent_provider_service import ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        old = (datetime.now(UTC) - timedelta(hours=48)).isoformat()
        record = ProviderModelCompatibilityRecord(model="gpt-4o", status="supported", tested_at=old)
        assert not svc.is_compatibility_record_fresh(record)

    def test_is_compatibility_record_fresh_invalid_date(self):
        from src.services.agent_provider_service import ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        record = ProviderModelCompatibilityRecord(model="m", status="s", tested_at="not-a-date")
        assert not svc.is_compatibility_record_fresh(record)

    def test_is_compatibility_record_fresh_no_tz(self):
        from src.services.agent_provider_service import ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        naive_dt = datetime.now().isoformat()  # naive, no tz
        record = ProviderModelCompatibilityRecord(model="m", status="s", tested_at=naive_dt)
        # Should handle naive datetime without crashing
        result = svc.is_compatibility_record_fresh(record)
        assert isinstance(result, bool)

    def test_compatibility_warning_no_record(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        cache_entry = ProviderModelCacheEntry(provider="openai", models=[], source="static")
        warning = svc.compatibility_warning_for_config(cfg, cache_entry)
        assert "не проверялась" in warning.lower() or "не подтверждена" in warning.lower()

    def test_compatibility_warning_unknown_status(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry, ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        fp = svc.config_fingerprint(cfg)
        record = ProviderModelCompatibilityRecord(
            model="gpt-4o",
            status="unknown",
            reason="probe incomplete",
            tested_at=datetime.now(UTC).isoformat(),
            config_fingerprint=fp,
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai", models=["gpt-4o"], source="static",
            compatibility={fp: record},
        )
        warning = svc.compatibility_warning_for_config(cfg, cache_entry)
        assert "probe incomplete" in warning or "не подтверждена" in warning

    def test_compatibility_warning_supported_fresh(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry, ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        fp = svc.config_fingerprint(cfg)
        record = ProviderModelCompatibilityRecord(
            model="gpt-4o",
            status="supported",
            tested_at=datetime.now(UTC).isoformat(),
            config_fingerprint=fp,
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai", models=["gpt-4o"], source="static",
            compatibility={fp: record},
        )
        warning = svc.compatibility_warning_for_config(cfg, cache_entry)
        assert warning == ""

    def test_compatibility_warning_supported_stale(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry, ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        fp = svc.config_fingerprint(cfg)
        old_ts = (datetime.now(UTC) - timedelta(hours=48)).isoformat()
        record = ProviderModelCompatibilityRecord(
            model="gpt-4o",
            status="supported",
            tested_at=old_ts,
            config_fingerprint=fp,
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai", models=["gpt-4o"], source="static",
            compatibility={fp: record},
        )
        warning = svc.compatibility_warning_for_config(cfg, cache_entry)
        assert "устарел" in warning.lower()

    def test_compatibility_error_no_record(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        cache_entry = ProviderModelCacheEntry(provider="openai", models=[], source="static")
        error = svc.compatibility_error_for_config(cfg, cache_entry)
        assert error == ""

    def test_compatibility_error_unsupported(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry, ProviderModelCompatibilityRecord

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        fp = svc.config_fingerprint(cfg)
        recent_ts = datetime.now(UTC).isoformat()
        record = ProviderModelCompatibilityRecord(
            model="gpt-4o",
            status="unsupported",
            reason="no tool call support",
            tested_at=recent_ts,
            config_fingerprint=fp,
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai", models=["gpt-4o"], source="static",
            compatibility={fp: record},
        )
        error = svc.compatibility_error_for_config(cfg, cache_entry)
        assert "no tool call support" in error

    def test_create_empty_config(self):
        svc, _ = self._make_service()
        cfg = svc.create_empty_config("openai", priority=1)
        assert cfg.provider == "openai"
        assert cfg.priority == 1
        assert cfg.selected_model != ""  # has default model

    def test_create_empty_config_unknown_provider(self):
        svc, _ = self._make_service()
        with pytest.raises(RuntimeError, match="Unknown provider"):
            svc.create_empty_config("nonexistent_provider_xyz", priority=0)

    def test_writes_enabled_no_cipher(self):
        svc, _ = self._make_service()
        # No encryption key → cipher is None
        assert svc.writes_enabled is False

    @pytest.mark.asyncio
    async def test_save_provider_configs_raises_without_encryption(self):
        svc, _ = self._make_service()
        cfg = self._make_cfg()
        with pytest.raises(RuntimeError, match="SESSION_ENCRYPTION_KEY"):
            await svc.save_provider_configs([cfg])

    def test_build_provider_views_empty(self):
        svc, _ = self._make_service()
        views = svc.build_provider_views([], {})
        assert views == []

    def test_build_provider_views_with_config(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry

        svc, _ = self._make_service()
        cfg = self._make_cfg(provider="openai", model="gpt-4o")
        cache = {
            "openai": ProviderModelCacheEntry(
                provider="openai", models=["gpt-4o", "gpt-4o-mini"], source="static"
            )
        }
        views = svc.build_provider_views([cfg], cache)
        assert len(views) == 1
        v = views[0]
        assert v["provider"] == "openai"
        assert "gpt-4o" in v["models"]

    def test_build_provider_views_selected_model_not_in_list(self):
        """Selected model not in cache models — should be prepended."""
        from src.services.agent_provider_service import ProviderModelCacheEntry

        svc, _ = self._make_service()
        cfg = self._make_cfg(provider="openai", model="gpt-custom-model")
        cache = {
            "openai": ProviderModelCacheEntry(
                provider="openai", models=["gpt-4o"], source="static"
            )
        }
        views = svc.build_provider_views([cfg], cache)
        assert "gpt-custom-model" in views[0]["models"]

    @pytest.mark.asyncio
    async def test_save_and_load_model_cache(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry

        svc, db = self._make_service()
        saved_json: list[str] = []

        async def mock_set_setting(key, value):
            saved_json.append(value)

        async def mock_get_setting(key):
            return saved_json[-1] if saved_json else None

        db.set_setting = mock_set_setting
        db.get_setting = mock_get_setting

        cache = {
            "openai": ProviderModelCacheEntry(
                provider="openai",
                models=["gpt-4o"],
                source="live",
                fetched_at=datetime.now(UTC).isoformat(),
            )
        }
        await svc.save_model_cache(cache)
        loaded = await svc.load_model_cache()
        assert "openai" in loaded
        assert "gpt-4o" in loaded["openai"].models

    def test_get_compatibility_record_none_cache(self):
        svc, _ = self._make_service()
        cfg = self._make_cfg()
        record = svc.get_compatibility_record(None, cfg)
        assert record is None

    def test_get_compatibility_record_missing_fingerprint(self):
        from src.services.agent_provider_service import ProviderModelCacheEntry

        svc, _ = self._make_service()
        cfg = self._make_cfg()
        cache_entry = ProviderModelCacheEntry(provider="openai", models=[], source="static")
        record = svc.get_compatibility_record(cache_entry, cfg)
        assert record is None

    def test_normalize_ollama_base_url(self):
        svc, _ = self._make_service()
        url = svc.normalize_ollama_base_url("http://localhost:11434", "")
        assert "11434" in url
