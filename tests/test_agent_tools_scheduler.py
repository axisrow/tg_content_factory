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
    async def test_no_pool_with_live_manager_returns_pool_gate(self, mock_db):
        """Live-manager path still needs the local Telegram pool."""
        handlers = _get_tool_handlers(
            mock_db, client_pool=None, scheduler_manager=_make_scheduler_mgr()
        )
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_no_pool_no_manager_falls_back_to_command(self, mock_db):
        """#1568: without a live manager the op is handed to the worker — no pool needed."""
        mock_db.repos = MagicMock()
        mock_db.set_setting = AsyncMock()
        mock_db.repos.telegram_commands.find_active_by_type = AsyncMock(return_value=None)
        mock_db.repos.telegram_commands.create_command = AsyncMock(return_value=11)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["start_scheduler"]({"confirm": True})
        text = _text(result)
        assert "запущен" in text
        assert "#11" in text
        mock_db.set_setting.assert_awaited_once_with("scheduler_autostart", "1")

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
    async def test_no_pool_with_live_manager_returns_pool_gate(self, mock_db):
        """Live-manager path still needs the local Telegram pool."""
        handlers = _get_tool_handlers(
            mock_db, client_pool=None, scheduler_manager=_make_scheduler_mgr()
        )
        result = await handlers["trigger_collection"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_no_pool_no_manager_falls_back_to_command(self, mock_db):
        """#1568: without a live manager the op is handed to the worker — no pool needed."""
        mock_db.repos = MagicMock()
        mock_db.repos.telegram_commands.find_active_by_type = AsyncMock(return_value=None)
        mock_db.repos.telegram_commands.create_command = AsyncMock(return_value=12)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["trigger_collection"]({"confirm": True})
        text = _text(result)
        assert "Сбор запущен" in text
        assert "#12" in text
        cmd = mock_db.repos.telegram_commands.create_command.await_args.args[0]
        assert cmd.command_type == "scheduler.trigger_collection"

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

    @staticmethod
    def _settings_db(mock_db, current):
        mock_db.repos = MagicMock()
        mock_db.repos.settings.get_setting = AsyncMock(return_value=current)
        mock_db.repos.settings.record_setting_decision = AsyncMock()
        mock_db.repos.telegram_commands.find_active_by_type = AsyncMock(return_value=None)
        mock_db.repos.telegram_commands.create_command = AsyncMock(return_value=21)

    @pytest.mark.anyio
    async def test_live_manager_toggle_is_journaled_and_reconciled(self, mock_db):
        """#1661 review: with a manager present the toggle used to flip only the
        in-memory job — no scheduler_job_disabled:* key, no journal entry, lost on
        restart. It must take the same journaled + reconcile path as web/CLI."""
        self._settings_db(mock_db, None)
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect_all"})
        text = _text(result)
        assert "выключена" in text
        assert "#21" in text
        mock_db.repos.settings.record_setting_decision.assert_awaited_once_with(
            "scheduler_job_disabled:collect_all", "1", origin="auto", actor="agent"
        )
        mgr.sync_job_state.assert_not_awaited()

    @pytest.mark.anyio
    async def test_live_manager_toggle_reenables_disabled_job(self, mock_db):
        """A second toggle must read the persisted key and turn the job back on."""
        self._settings_db(mock_db, "1")
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect_all"})
        assert "включена" in _text(result)
        mock_db.repos.settings.record_setting_decision.assert_awaited_once_with(
            "scheduler_job_disabled:collect_all", "0", origin="auto", actor="agent"
        )

    @pytest.mark.anyio
    async def test_live_manager_toggle_canonicalizes_pipeline_run_id(self, mock_db):
        self._settings_db(mock_db, None)
        handlers = _get_tool_handlers(mock_db, scheduler_manager=_make_scheduler_mgr())
        await handlers["toggle_scheduler_job"]({"job_id": "pipeline_run_5"})
        mock_db.repos.settings.record_setting_decision.assert_awaited_once_with(
            "scheduler_job_disabled:content_generate_5", "1", origin="auto", actor="agent"
        )

    @pytest.mark.anyio
    async def test_no_manager_falls_back_to_disabled_key(self, mock_db):
        """#1568: without a live manager the toggle writes the settings key + reconcile."""
        mock_db.repos = MagicMock()
        mock_db.repos.settings.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings.record_setting_decision = AsyncMock()
        mock_db.repos.telegram_commands.find_active_by_type = AsyncMock(return_value=None)
        mock_db.repos.telegram_commands.create_command = AsyncMock(return_value=13)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect"})
        text = _text(result)
        assert "выключена" in text
        assert "#13" in text
        mock_db.repos.settings.record_setting_decision.assert_awaited_once_with(
            "scheduler_job_disabled:collect", "1", origin="auto", actor="agent"
        )

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        self._settings_db(mock_db, None)
        mock_db.repos.settings.get_setting = AsyncMock(side_effect=Exception("db down"))
        handlers = _get_tool_handlers(mock_db, scheduler_manager=_make_scheduler_mgr())
        result = await handlers["toggle_scheduler_job"]({"job_id": "bad_job"})
        assert "Ошибка переключения задачи" in _text(result)
