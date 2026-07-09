"""Tests for agent tools: settings.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


class TestGetSettingsTool:
    @pytest.mark.anyio
    async def test_shows_all_settings_keys(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        text = _text(result)
        assert "collect_interval_minutes" in text
        assert "agent_prompt_template" in text
        assert "Настройки системы" in text

    @pytest.mark.anyio
    async def test_shows_set_values(self, mock_db):
        async def fake_get(key):
            return "60" if key == "collect_interval_minutes" else None

        mock_db.get_setting = fake_get
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "60" in _text(result)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("no table"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "Ошибка получения настроек" in _text(result)

    @pytest.mark.anyio
    async def test_returns_settings(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=lambda k: {"collect_interval_minutes": "60"}.get(k))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        text = _text(result)
        assert "Настройки системы" in text
        assert "collect_interval_minutes" in text

    @pytest.mark.anyio
    async def test_error_handling(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "Ошибка получения настроек" in _text(result)


class TestSaveAgentSettingsTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"backend": "claude"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_saves_prompt_template(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"prompt_template": "You are a helpful bot.", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("agent_prompt_template", "You are a helpful bot.")

    @pytest.mark.anyio
    async def test_saves_backend_override(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"backend": "deepagents", "confirm": True})
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("agent_backend_override", "deepagents")

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"prompt_template": "new template"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_saves_prompt_template_via_call(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"prompt_template": "new template", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_call("agent_prompt_template", "new template")

    @pytest.mark.anyio
    async def test_saves_backend(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"backend": "claude-agent-sdk", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_call("agent_backend_override", "claude-agent-sdk")


class TestSaveFilterSettingsTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"]({"low_uniqueness_threshold": 0.5})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_saves_thresholds(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"](
            {"low_uniqueness_threshold": 0.3, "low_subscriber_ratio_threshold": 0.1, "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("low_uniqueness_threshold", "0.3")
        mock_db.set_setting.assert_any_await("low_subscriber_ratio_threshold", "0.1")

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"]({"low_uniqueness_threshold": 0.3})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_saves_thresholds_via_call(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"](
            {"low_uniqueness_threshold": 0.3, "low_subscriber_ratio_threshold": 0.05, "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_call("low_uniqueness_threshold", "0.3")


def _mock_command_service(mock_db):
    """Stub ``db.repos.telegram_commands`` so ``TelegramCommandService.enqueue`` works.

    ``save_scheduler_settings`` constructs ``TelegramCommandService(db)`` and calls
    ``enqueue`` — which reaches into ``db.repos.telegram_commands``. Wire the two repo
    methods enqueue touches (dedup lookup + insert) so the tool runs against the mock_db.
    Returns the ``telegram_commands`` repo mock for assertions.
    """
    repo = MagicMock()
    repo.find_active_by_type = AsyncMock(return_value=None)
    repo.create_command = AsyncMock(return_value=7)
    mock_db.repos = MagicMock()
    mock_db.repos.telegram_commands = repo
    return repo


class TestSaveSchedulerSettingsTool:
    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"collect_interval_minutes": 30})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_saves_interval(self, mock_db):
        mock_db.set_setting = AsyncMock()
        _mock_command_service(mock_db)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"](
            {"collect_interval_minutes": 30, "confirm": True}
        )
        assert "30 мин" in _text(result)
        mock_db.set_setting.assert_called_with("collect_interval_minutes", "30")

    @pytest.mark.anyio
    async def test_clamps_interval_to_range(self, mock_db):
        mock_db.set_setting = AsyncMock()
        _mock_command_service(mock_db)
        handlers = _get_tool_handlers(mock_db)
        # Test lower bound
        result = await handlers["save_scheduler_settings"](
            {"collect_interval_minutes": 0, "confirm": True}
        )
        assert "1 мин" in _text(result)

        # Test upper bound
        result = await handlers["save_scheduler_settings"](
            {"collect_interval_minutes": 2000, "confirm": True}
        )
        assert "1440 мин" in _text(result)

    # --- #1266: web-mode scheduler mutation must reach the worker via a command ---

    @pytest.mark.anyio
    async def test_enqueues_scheduler_reconcile(self, mock_db):
        """The tool must enqueue a ``scheduler.reconcile`` command, not poke a no-op shim.

        In web-mode the injected scheduler_manager is the read-only snapshot shim; the
        live SchedulerManager lives in the worker. The only way a chat-driven interval
        change reaches it is a ``scheduler.reconcile`` telegram command. Regression guard
        for #1266 — mutation-red: without the enqueue, ``create_command`` is never awaited.
        """
        mock_db.set_setting = AsyncMock()
        repo = _mock_command_service(mock_db)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"](
            {"collect_interval_minutes": 30, "confirm": True}
        )
        assert "30 мин" in _text(result)
        # dedup lookup + insert are both against command_type "scheduler.reconcile"
        repo.find_active_by_type.assert_awaited_once()
        assert repo.find_active_by_type.await_args.args[0] == "scheduler.reconcile"
        repo.create_command.assert_awaited_once()
        created = repo.create_command.await_args.args[0]
        assert created.command_type == "scheduler.reconcile"
        assert created.payload == {}

    @pytest.mark.anyio
    async def test_interval_saved_before_reconcile(self, mock_db):
        """The interval must be persisted before the reconcile is enqueued.

        The worker's ``_handle_scheduler_reconcile`` re-reads ``collect_interval_minutes``
        from the DB, so the write must land first or the reconcile would rebuild the
        trigger from the stale value.
        """
        calls: list[str] = []
        mock_db.set_setting = AsyncMock(side_effect=lambda *a, **k: calls.append("set_setting"))
        repo = _mock_command_service(mock_db)
        repo.create_command = AsyncMock(side_effect=lambda *a, **k: calls.append("enqueue") or 7)
        handlers = _get_tool_handlers(mock_db)
        await handlers["save_scheduler_settings"]({"collect_interval_minutes": 45, "confirm": True})
        assert calls == ["set_setting", "enqueue"]

    @pytest.mark.anyio
    async def test_no_confirm_does_not_enqueue(self, mock_db):
        """The confirm gate must fire before any DB write or enqueue."""
        mock_db.set_setting = AsyncMock()
        repo = _mock_command_service(mock_db)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"collect_interval_minutes": 30})
        assert "confirm=true" in _text(result).lower()
        mock_db.set_setting.assert_not_awaited()
        repo.create_command.assert_not_awaited()

    @pytest.mark.anyio
    async def test_enqueues_reconcile_row_in_real_db(self, db):
        """Integration guard: a real ``scheduler.reconcile`` row lands in telegram_commands.

        Mirrors the web-route contract from #1257 (``tests/test_web_scheduler_reconcile_enqueue.py``)
        but drives the agent tool against a real in-memory Database instead of the web app.
        """
        handlers = _get_tool_handlers(db)
        result = await handlers["save_scheduler_settings"](
            {"collect_interval_minutes": 90, "confirm": True}
        )
        assert "90 мин" in _text(result)
        assert await db.get_setting("collect_interval_minutes") == "90"
        commands = await db.repos.telegram_commands.list_commands(command_type="scheduler.reconcile")
        assert len(commands) == 1
        assert commands[0].payload == {}

    @pytest.mark.anyio
    async def test_repeated_saves_dedup_into_one_reconcile(self, db):
        """``enqueue`` deduplicates on (type, payload), so repeated saves collapse to one row."""
        handlers = _get_tool_handlers(db)
        await handlers["save_scheduler_settings"]({"collect_interval_minutes": 30, "confirm": True})
        await handlers["save_scheduler_settings"]({"collect_interval_minutes": 45, "confirm": True})
        commands = await db.repos.telegram_commands.list_commands(command_type="scheduler.reconcile")
        assert len(commands) == 1
        # Latest interval still persisted even though the reconcile command was reused.
        assert await db.get_setting("collect_interval_minutes") == "45"


class TestGetSystemInfoTool:
    @pytest.mark.anyio
    async def test_shows_stats(self, mock_db):
        mock_db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 1000})
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        text = _text(result)
        assert "channels" in text
        assert "10" in text
        assert "Системная информация" in text

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_stats = AsyncMock(side_effect=Exception("no stats"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        assert "Ошибка получения системной информации" in _text(result)

    @pytest.mark.anyio
    async def test_returns_stats(self, mock_db):
        mock_db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 5000})
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        text = _text(result)
        assert "Системная информация" in text
        assert "channels" in text

    @pytest.mark.anyio
    async def test_error_handling(self, mock_db):
        mock_db.get_stats = AsyncMock(side_effect=Exception("stats error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        assert "Ошибка получения" in _text(result)


class TestGetServerTimeTool:
    @pytest.mark.anyio
    async def test_returns_utc_time(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_server_time"]({})
        text = _text(result)
        assert "UTC" in text
        assert "ISO8601" in text
        assert "Unix" in text

    @pytest.mark.anyio
    async def test_iso_string_is_parseable(self, mock_db):
        from datetime import datetime

        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_server_time"]({})
        iso_line = next(line for line in _text(result).splitlines() if "ISO8601" in line)
        iso_value = iso_line.split("ISO8601:", 1)[1].strip()
        parsed = datetime.fromisoformat(iso_value)
        assert parsed.tzinfo is not None
