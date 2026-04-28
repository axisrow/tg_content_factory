"""Tests for agent tools: scheduler.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_scheduler_mgr():
    """Build a standard mock SchedulerManager."""
    mgr = MagicMock()
    mgr.is_running = True
    mgr.interval_minutes = 60
    mgr.get_potential_jobs = AsyncMock(return_value=[{"id": "collect", "enabled": True}])
    mgr.get_all_jobs_next_run = MagicMock(return_value={"collect": "2025-01-01 12:00"})
    mgr.is_job_enabled = AsyncMock(return_value=True)
    mgr.sync_job_state = AsyncMock()
    mgr.start = AsyncMock()
    mgr.stop = AsyncMock()
    mgr.trigger_now = AsyncMock(return_value="ok")
    return mgr


class TestGetSchedulerStatusTool:
    @pytest.mark.anyio
    async def test_no_manager_returns_error(self, mock_db):
        """Without a scheduler_manager the tool catches RuntimeError and returns text."""
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_scheduler_status"]({})
        assert "Ошибка получения статуса планировщика" in _text(result)

    @pytest.mark.anyio
    async def test_with_manager_shows_running_state(self, mock_db):
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["get_scheduler_status"]({})
        text = _text(result)
        assert "запущен" in text
        assert "60 мин" in text

    @pytest.mark.anyio
    async def test_with_manager_lists_jobs(self, mock_db):
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["get_scheduler_status"]({})
        text = _text(result)
        assert "collect" in text
        assert "вкл" in text
        assert "2025-01-01 12:00" in text

    @pytest.mark.anyio
    async def test_with_manager_stopped(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_running = False
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["get_scheduler_status"]({})
        assert "остановлен" in _text(result)


class TestStartSchedulerTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["start_scheduler"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_pool_and_confirm_starts(self, mock_db):
        mgr = _make_scheduler_mgr()
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Планировщик запущен" in _text(result)
        mgr.start.assert_awaited_once()

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.start = AsyncMock(side_effect=Exception("already running"))
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Ошибка запуска планировщика" in _text(result)


class TestStopSchedulerTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["stop_scheduler"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_stops(self, mock_db):
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["stop_scheduler"]({"confirm": True})
        assert "Планировщик остановлен" in _text(result)
        mgr.stop.assert_awaited_once()

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.stop = AsyncMock(side_effect=Exception("not running"))
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["stop_scheduler"]({"confirm": True})
        assert "Ошибка остановки планировщика" in _text(result)


class TestTriggerCollectionTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["trigger_collection"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["trigger_collection"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_pool_and_confirm_triggers(self, mock_db):
        mgr = _make_scheduler_mgr()
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["trigger_collection"]({"confirm": True})
        text = _text(result)
        assert "Сбор запущен" in text
        mgr.trigger_now.assert_awaited_once()

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.trigger_now = AsyncMock(side_effect=Exception("busy"))
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["trigger_collection"]({"confirm": True})
        assert "Ошибка запуска сбора" in _text(result)


class TestToggleSchedulerJobTool:
    @pytest.mark.anyio
    async def test_missing_job_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_scheduler_job"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_enabled_job_becomes_disabled(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_job_enabled = AsyncMock(return_value=True)
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect"})
        text = _text(result)
        assert "collect" in text
        assert "выключена" in text
        mgr.sync_job_state.assert_awaited_once_with("collect", False)

    @pytest.mark.anyio
    async def test_disabled_job_becomes_enabled(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_job_enabled = AsyncMock(return_value=False)
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "notify"})
        text = _text(result)
        assert "notify" in text
        assert "включена" in text
        mgr.sync_job_state.assert_awaited_once_with("notify", True)

    @pytest.mark.anyio
    async def test_no_manager_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect"})
        assert "Ошибка переключения задачи" in _text(result)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_job_enabled = AsyncMock(side_effect=Exception("unknown job"))
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "bad_job"})
        assert "Ошибка переключения задачи" in _text(result)
