"""Tests for agent tools: settings.py."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


class TestGetSettingsTool:
    @pytest.mark.asyncio
    async def test_shows_all_settings_keys(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        text = _text(result)
        assert "collect_interval_minutes" in text
        assert "agent_prompt_template" in text
        assert "Настройки системы" in text

    @pytest.mark.asyncio
    async def test_shows_set_values(self, mock_db):
        async def fake_get(key):
            return "60" if key == "collect_interval_minutes" else None

        mock_db.get_setting = fake_get
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "60" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("no table"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "Ошибка получения настроек" in _text(result)

    @pytest.mark.asyncio
    async def test_returns_settings(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=lambda k: {"collect_interval_minutes": "60"}.get(k))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        text = _text(result)
        assert "Настройки системы" in text
        assert "collect_interval_minutes" in text

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "Ошибка получения настроек" in _text(result)


class TestSaveAgentSettingsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"backend": "claude"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_saves_prompt_template(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"prompt_template": "You are a helpful bot.", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("agent_prompt_template", "You are a helpful bot.")

    @pytest.mark.asyncio
    async def test_saves_backend_override(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"backend": "deepagents", "confirm": True})
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("agent_backend_override", "deepagents")

    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"prompt_template": "new template"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_saves_prompt_template_via_call(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"prompt_template": "new template", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_call("agent_prompt_template", "new template")

    @pytest.mark.asyncio
    async def test_saves_backend(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"backend": "claude-agent-sdk", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_call("agent_backend_override", "claude-agent-sdk")


class TestSaveFilterSettingsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"]({"low_uniqueness_threshold": 0.5})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_saves_thresholds(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"](
            {"low_uniqueness_threshold": 0.3, "low_subscriber_ratio_threshold": 0.1, "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("low_uniqueness_threshold", "0.3")
        mock_db.set_setting.assert_any_await("low_subscriber_ratio_threshold", "0.1")

    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"]({"low_uniqueness_threshold": 0.3})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_saves_thresholds_via_call(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"](
            {"low_uniqueness_threshold": 0.3, "low_subscriber_ratio_threshold": 0.05, "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_call("low_uniqueness_threshold", "0.3")


class TestSaveSchedulerSettingsTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"collect_interval_minutes": 30})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_saves_interval(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"](
            {"collect_interval_minutes": 30, "confirm": True}
        )
        assert "30 мин" in _text(result)
        mock_db.set_setting.assert_called_with("collect_interval_minutes", "30")

    @pytest.mark.asyncio
    async def test_clamps_interval_to_range(self, mock_db):
        mock_db.set_setting = AsyncMock()
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


class TestGetSystemInfoTool:
    @pytest.mark.asyncio
    async def test_shows_stats(self, mock_db):
        mock_db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 1000})
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        text = _text(result)
        assert "channels" in text
        assert "10" in text
        assert "Системная информация" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_stats = AsyncMock(side_effect=Exception("no stats"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        assert "Ошибка получения системной информации" in _text(result)

    @pytest.mark.asyncio
    async def test_returns_stats(self, mock_db):
        mock_db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 5000})
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        text = _text(result)
        assert "Системная информация" in text
        assert "channels" in text

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_db):
        mock_db.get_stats = AsyncMock(side_effect=Exception("stats error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        assert "Ошибка получения" in _text(result)
