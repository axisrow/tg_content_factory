"""Tests for agent tools: collection.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_channel(
    pk=1,
    channel_id=100,
    title="TestChan",
    username="testchan",
    is_active=True,
    is_filtered=False,
    channel_type="channel",
):
    ch = MagicMock()
    ch.id = pk
    ch.channel_id = channel_id
    ch.title = title
    ch.username = username
    ch.is_active = is_active
    ch.is_filtered = is_filtered
    ch.channel_type = channel_type
    return ch


class TestCollectChannelTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_channel"]({"pk": 1})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_filtered_channel_without_force_returns_warning(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(is_filtered=True, title="SpamChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 1})
        assert "отфильтрован" in _text(result)
        assert "force=true" in _text(result)

    @pytest.mark.asyncio
    async def test_filtered_channel_with_force_enqueues(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(is_filtered=True, title="SpamChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.create_collection_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 1, "force": True})
        assert "поставлен в очередь" in _text(result)

    @pytest.mark.asyncio
    async def test_normal_channel_enqueues(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(title="GoodChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.create_collection_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 1})
        assert "поставлен в очередь" in _text(result)
        assert "GoodChan" in _text(result)
        mock_db.create_collection_task.assert_awaited_once()


class TestCollectAllChannelsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_all_channels"]({})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_channels_returns_message(self, mock_db):
        pool = MagicMock()
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_channels"]({})
        assert "Нет активных каналов" in _text(result)

    @pytest.mark.asyncio
    async def test_enqueues_all_active_channels(self, mock_db):
        pool = MagicMock()
        channels = [_make_channel(pk=i, channel_id=i * 100, title=f"Chan{i}") for i in range(3)]
        mock_db.get_channels = AsyncMock(return_value=channels)
        mock_db.create_collection_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_channels"]({})
        text = _text(result)
        assert "3 каналов" in text
        assert mock_db.create_collection_task.await_count == 3


class TestCollectChannelStatsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_channel_stats"]({"pk": 1})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel_stats"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel_stats"]({"pk": 99})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_enqueues_stats_task(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(title="StatsChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.create_stats_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel_stats"]({"pk": 1})
        assert "поставлен в очередь" in _text(result)
        assert "StatsChan" in _text(result)
        mock_db.create_stats_task.assert_awaited_once()


class TestCollectAllStatsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_all_stats"]({})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_channels_returns_message(self, mock_db):
        pool = MagicMock()
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_stats"]({})
        assert "Нет активных каналов" in _text(result)

    @pytest.mark.asyncio
    async def test_enqueues_stats_for_all(self, mock_db):
        pool = MagicMock()
        channels = [_make_channel(pk=i, channel_id=i * 100) for i in range(5)]
        mock_db.get_channels = AsyncMock(return_value=channels)
        mock_db.create_stats_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_stats"]({})
        assert "5 каналов" in _text(result)
        mock_db.create_stats_task.assert_awaited_once()
