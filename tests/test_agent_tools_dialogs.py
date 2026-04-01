"""Tests for agent tools: dialogs.py MCP tools."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


class TestDialogsToolSearchDialogs:
    @pytest.mark.asyncio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["search_dialogs"]({"phone": "+7123456"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_dialogs(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=[])
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_dialogs(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        dialogs = [
            {"title": "My Channel", "channel_id": 111, "channel_type": "channel"},
            {"title": "My Group", "channel_id": 222, "channel_type": "group"},
        ]
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=dialogs)
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "My Channel" in text
        assert "id=111" in text


class TestDialogsToolRefreshDialogs:
    @pytest.mark.asyncio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["refresh_dialogs"]({"phone": "+7123456"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_refresh_success(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(
            return_value=[{"title": "X", "channel_id": 1, "channel_type": "channel"}]
        )
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["refresh_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "обновлены" in text
        assert "1" in text


class TestDialogsToolLeaveDialogs:
    @pytest.mark.asyncio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["leave_dialogs"]({"phone": "+7123456", "dialog_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_dialog_ids(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["leave_dialogs"](
            {"phone": "+79001234567", "dialog_ids": "", "confirm": True}
        )
        assert "dialog_ids обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["leave_dialogs"](
            {"phone": "+79001234567", "dialog_ids": "1,2", "confirm": False}
        )
        assert "Подтвердите" in _text(result)


class TestDialogsToolGetForumTopics:
    @pytest.mark.asyncio
    async def test_missing_channel_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({})
        assert "channel_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_topics(self, mock_db):
        mock_db.get_forum_topics = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_topics(self, mock_db):
        topics = [
            {"topic_id": 1, "title": "General"},
            {"topic_id": 2, "title": "Off-topic"},
        ]
        mock_db.get_forum_topics = AsyncMock(return_value=topics)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        text = _text(result)
        assert "General" in text
        assert "Off-topic" in text
        assert "id=1" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        mock_db.get_forum_topics = AsyncMock(side_effect=Exception("no access"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "Ошибка" in _text(result)


class TestDialogsToolClearDialogCache:
    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_dialog_cache"]({"phone": "+79001234567", "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.asyncio
    async def test_clears_for_phone(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_dialogs = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.invalidate_dialogs_cache = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["clear_dialog_cache"]({"phone": "+79001234567", "confirm": True})
        assert "очищен" in _text(result)
        mock_db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+79001234567")

    @pytest.mark.asyncio
    async def test_clears_all_when_no_phone(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_dialog_cache"]({"phone": "", "confirm": True})
        assert "очищен" in _text(result)
        mock_db.repos.dialog_cache.clear_all_dialogs.assert_awaited_once()


class TestDialogsToolGetCacheStatus:
    @pytest.mark.asyncio
    async def test_empty_cache(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "пуст" in _text(result)

    @pytest.mark.asyncio
    async def test_with_cache_entries(self, mock_db):
        from datetime import datetime, timezone

        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=["+79001234567"])
        mock_db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=42)
        mock_db.repos.dialog_cache.get_cached_at = AsyncMock(
            return_value=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        text = _text(result)
        assert "+79001234567" in text
        assert "42" in text
        assert "2026-01-01" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(side_effect=Exception("cache err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "Ошибка" in _text(result)
