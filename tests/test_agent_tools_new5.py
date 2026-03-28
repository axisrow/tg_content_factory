"""Tests for agent tools: moderation, channels (new), settings (new), pipelines (new).

Tests exercise tool handlers via the @tool decorator's .handler attribute.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    db = MagicMock(spec=Database)
    db.get_setting = AsyncMock(return_value=None)
    db.set_setting = AsyncMock()
    db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 1000})
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
    """Extract text from tool result payload."""
    return result["content"][0]["text"]


def _make_run(run_id=1, pipeline_id=1, status="pending", moderation_status="pending", text="Test text"):
    r = MagicMock()
    r.id = run_id
    r.pipeline_id = pipeline_id
    r.status = status
    r.moderation_status = moderation_status
    r.generated_text = text
    r.created_at = "2025-01-01T12:00:00"
    r.updated_at = "2025-01-01T12:00:00"
    return r


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


def _make_pipeline(
    pk=1,
    name="TestPipeline",
    is_active=True,
    llm_model=None,
    publish_mode="moderated",
    schedule_cron=None,
):
    p = MagicMock()
    p.id = pk
    p.name = name
    p.is_active = is_active
    p.llm_model = llm_model
    p.publish_mode = MagicMock()
    p.publish_mode.value = publish_mode
    p.generation_backend = None
    p.schedule_cron = schedule_cron
    p.prompt_template = "Test template"
    p.generate_interval_minutes = 60
    return p


# ===========================================================================
# moderation.py
# ===========================================================================


class TestListPendingModerationTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        assert "Нет черновиков на модерации" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs_shows_preview(self, mock_db):
        run = _make_run(run_id=1, text="Sample generated text for preview")
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        text = _text(result)
        assert "На модерации (1 шт.)" in text
        assert "run_id=1" in text

    @pytest.mark.asyncio
    async def test_with_pipeline_filter(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        await handlers["list_pending_moderation"]({"pipeline_id": 5, "limit": 10})
        mock_db.repos.generation_runs.list_pending_moderation.assert_called_once_with(
            pipeline_id=5, limit=10
        )

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(
            side_effect=Exception("db error")
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        assert "Ошибка получения очереди модерации" in _text(result)


class TestViewModerationRunTool:
    @pytest.mark.asyncio
    async def test_missing_run_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["view_moderation_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["view_moderation_run"]({"run_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_run_found_shows_text(self, mock_db):
        run = _make_run(run_id=1, text="Full text content")
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["view_moderation_run"]({"run_id": 1})
        text = _text(result)
        assert "Run id=1" in text
        assert "Full text content" in text


class TestApproveRunTool:
    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_approve_success(self, mock_db):
        run = _make_run(run_id=1)
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 1})
        assert "одобрен" in _text(result)
        mock_db.repos.generation_runs.set_moderation_status.assert_called_once_with(1, "approved")


class TestRejectRunTool:
    @pytest.mark.asyncio
    async def test_reject_success(self, mock_db):
        run = _make_run(run_id=2)
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reject_run"]({"run_id": 2})
        assert "отклонён" in _text(result)
        mock_db.repos.generation_runs.set_moderation_status.assert_called_once_with(2, "rejected")


class TestBulkApproveRunsTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1,2,3"})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_invalid_run_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "a,b,c", "confirm": True})
        assert "должны быть числами" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_run_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "", "confirm": True})
        assert "run_ids пуст" in _text(result)

    @pytest.mark.asyncio
    async def test_bulk_approve_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1,2,3", "confirm": True})
        assert "Одобрено 3 run(s)" in _text(result)


class TestBulkRejectRunsTool:
    @pytest.mark.asyncio
    async def test_bulk_reject_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_reject_runs"]({"run_ids": "5,6", "confirm": True})
        assert "Отклонено 2 run(s)" in _text(result)


# ===========================================================================
# settings.py (new tools)
# ===========================================================================


class TestGetSettingsTool:
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
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"prompt_template": "new template"})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_saves_prompt_template(self, mock_db):
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
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"]({"low_uniqueness_threshold": 0.3})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_saves_thresholds(self, mock_db):
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
        assert "подтверждение" in _text(result).lower()

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


# ===========================================================================
# channels.py (new tools) - tags
# ===========================================================================


class TestListTagsTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.repos.channels.list_all_tags = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_tags"]({})
        assert "Теги не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_tags(self, mock_db):
        mock_db.repos.channels.list_all_tags = AsyncMock(return_value=["news", "tech", "fun"])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_tags"]({})
        text = _text(result)
        assert "Теги (3)" in text
        assert "news" in text


class TestCreateTagTool:
    @pytest.mark.asyncio
    async def test_missing_name(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({})
        assert "name обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({"name": "newtag"})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_creates_tag(self, mock_db):
        mock_db.repos.channels.create_tag = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({"name": "newtag", "confirm": True})
        assert "создан" in _text(result)
        mock_db.repos.channels.create_tag.assert_called_once_with("newtag")


class TestDeleteTagTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_tag"]({"name": "oldtag"})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_deletes_tag(self, mock_db):
        mock_db.repos.channels.delete_tag = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_tag"]({"name": "oldtag", "confirm": True})
        assert "удалён" in _text(result)
        mock_db.repos.channels.delete_tag.assert_called_once_with("oldtag")


class TestSetChannelTagsTool:
    @pytest.mark.asyncio
    async def test_missing_pk(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"tags": "news,tech"})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 999, "tags": "news"})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_sets_tags(self, mock_db):
        ch = _make_channel(pk=1, title="TestChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.set_channel_tags = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 1, "tags": "news,tech"})
        assert "обновлены" in _text(result)
        mock_db.repos.channels.set_channel_tags.assert_called_once_with(1, ["news", "tech"])

    @pytest.mark.asyncio
    async def test_clears_tags(self, mock_db):
        ch = _make_channel(pk=1, title="TestChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.set_channel_tags = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 1, "tags": ""})
        assert "очищены" in _text(result)


# ===========================================================================
# pipelines.py (new tools)
# ===========================================================================


class TestListPipelinesTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService.list", AsyncMock(return_value=[])):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["list_pipelines"]({})
            assert "Пайплайны не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pipelines(self, mock_db):
        p = _make_pipeline(pk=1, name="NewsPipeline")
        with patch("src.services.pipeline_service.PipelineService.list", AsyncMock(return_value=[p])):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["list_pipelines"]({})
            text = _text(result)
            assert "Пайплайны (1)" in text
            assert "NewsPipeline" in text


class TestGetPipelineDetailTool:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_pipeline_detail"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        with patch(
            "src.services.pipeline_service.PipelineService.get_detail", AsyncMock(return_value=None)
        ):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 999})
            assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_shows_detail(self, mock_db):
        p = _make_pipeline(pk=1, name="TestPipeline")
        detail = {
            "pipeline": p,
            "source_titles": ["Channel1", "Channel2"],
            "target_refs": ["phone1|123"],
            "source_ids": [100, 101],
            "targets": [MagicMock(phone="phone1", dialog_id=123)],
        }
        with patch(
            "src.services.pipeline_service.PipelineService.get_detail", AsyncMock(return_value=detail)
        ):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 1})
            text = _text(result)
            assert "TestPipeline" in text
            assert "Channel1" in text


class TestGetPipelineQueueTool:
    @pytest.mark.asyncio
    async def test_empty_queue(self, mock_db):
        mock_db.repos.generation_runs.list_by_status = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_pipeline_queue"]({})
        assert "Очередь генерации пуста" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs(self, mock_db):
        run = _make_run(run_id=1, status="pending", text="Generated content preview")
        mock_db.repos.generation_runs.list_by_status = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_pipeline_queue"]({"limit": 10})
        text = _text(result)
        assert "Очередь генерации (1 шт.)" in text
        assert "run_id=1" in text


class TestTogglePipelineTool:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["toggle_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService.toggle", AsyncMock(return_value=False)):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["toggle_pipeline"]({"pipeline_id": 999})
            assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_toggle_activates(self, mock_db):
        p = _make_pipeline(pk=1, name="Test", is_active=True)
        with patch(
            "src.services.pipeline_service.PipelineService.toggle", AsyncMock(return_value=True)
        ), patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=p)):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["toggle_pipeline"]({"pipeline_id": 1})
            assert "активирован" in _text(result) or "деактивирован" in _text(result)


class TestDeletePipelineTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        p = _make_pipeline(pk=1, name="Test")
        with patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=p)):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["delete_pipeline"]({"pipeline_id": 1})
            assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["delete_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)


class TestGetRefinementStepsTool:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_refinement_steps"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_refinement_steps"]({"pipeline_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_no_steps(self, mock_db):
        p = MagicMock()
        p.refinement_steps = []
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=p)
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_refinement_steps"]({"pipeline_id": 1})
        assert "не имеет шагов рефайнмента" in _text(result)

    @pytest.mark.asyncio
    async def test_with_steps(self, mock_db):
        p = MagicMock()
        p.refinement_steps = [
            {"name": "Improve", "prompt": "Improve this text: {text}"},
            {"name": "Translate", "prompt": "Translate to Russian: {text}"},
        ]
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=p)
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_refinement_steps"]({"pipeline_id": 1})
        text = _text(result)
        assert "Шаги рефайнмента" in text
        assert "Improve" in text


class TestSetRefinementStepsTool:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["set_refinement_steps"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["set_refinement_steps"]({"pipeline_id": 1, "steps_json": "[]"})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_invalid_json(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["set_refinement_steps"](
            {"pipeline_id": 1, "steps_json": "not json", "confirm": True}
        )
        assert "парсинга" in _text(result)

    @pytest.mark.asyncio
    async def test_not_a_list(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["set_refinement_steps"](
            {"pipeline_id": 1, "steps_json": '{"key": "value"}', "confirm": True}
        )
        assert "JSON-массивом" in _text(result)

    @pytest.mark.asyncio
    async def test_steps_missing_prompt(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["set_refinement_steps"](
            {"pipeline_id": 1, "steps_json": '[{"name": "test"}]', "confirm": True}
        )
        assert "не содержат поле 'prompt'" in _text(result)

    @pytest.mark.asyncio
    async def test_sets_valid_steps(self, mock_db):
        p = MagicMock()
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=p)
        mock_db.repos.content_pipelines.set_refinement_steps = AsyncMock()
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["set_refinement_steps"](
            {
                "pipeline_id": 1,
                "steps_json": '[{"name": "Test", "prompt": "Process: {text}"}]',
                "confirm": True,
            }
        )
        assert "обновлены" in _text(result)
        mock_db.repos.content_pipelines.set_refinement_steps.assert_called_once()


class TestListPipelineRunsTool:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["list_pipeline_runs"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_runs(self, mock_db):
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})
        assert "Нет генераций" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs(self, mock_db):
        run = _make_run(run_id=1, text="Preview text")
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})
        text = _text(result)
        assert "Генерации пайплайна id=1" in text
        assert "run_id=1" in text


class TestGetPipelineRunTool:
    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_pipeline_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_pipeline_run"]({"run_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_shows_run_text(self, mock_db):
        run = _make_run(run_id=1, text="Full generated text content")
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["get_pipeline_run"]({"run_id": 1})
        text = _text(result)
        assert "Run id=1" in text
        assert "Full generated text content" in text


class TestAddPipelineTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["add_pipeline"]({"name": "Test"})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_required_fields(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["add_pipeline"]({"name": "Test", "confirm": True})
        assert "обязательны" in _text(result)


class TestEditPipelineTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["edit_pipeline"]({"pipeline_id": 1})
        assert "подтверждение" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["edit_pipeline"]({"confirm": True})
        assert "pipeline_id обязателен" in _text(result)
