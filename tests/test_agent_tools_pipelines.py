"""Tests for agent tools: pipelines.py MCP tools."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


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


class TestPipelinesToolListPipelines:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({"active_only": False})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pipeline(self, mock_db):
        p = SimpleNamespace(
            id=5,
            name="Test Pipeline",
            is_active=True,
            llm_model="gpt-4",
            publish_mode="moderated",
            schedule_cron="0 * * * *",
            generation_backend="chain",
        )
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(return_value=[p])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({"active_only": True})
        text = _text(result)
        assert "Test Pipeline" in text
        assert "id=5" in text
        assert "gpt-4" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(side_effect=Exception("db err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({})
        assert "Ошибка" in _text(result)

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


class TestPipelinesToolGetPipelineDetail:
    @pytest.mark.asyncio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_detail"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_found(self, mock_db):
        p = SimpleNamespace(
            id=2,
            name="Detail Pipeline",
            is_active=True,
            llm_model="claude-3",
            publish_mode="auto",
            generation_backend="sdk",
            schedule_cron=None,
            generate_interval_minutes=120,
            prompt_template="Generate about {topic}",
        )
        detail = {
            "pipeline": p,
            "source_titles": ["Channel A", "Channel B"],
            "target_refs": ["@target"],
        }
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 2})
        text = _text(result)
        assert "Detail Pipeline" in text
        assert "Channel A" in text

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


class TestPipelinesToolAddPipeline:
    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "name": "New",
            "prompt_template": "tmpl",
            "source_channel_ids": "1,2",
            "target_refs": "+7123|456",
            "confirm": False,
        })
        assert "Подтвердите" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_required_fields(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({"confirm": True, "name": "only name"})
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_target_ref_format(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "confirm": True,
            "name": "P",
            "prompt_template": "t",
            "source_channel_ids": "1",
            "target_refs": "invalidformat",
        })
        assert "Неверный формат" in _text(result)

    @pytest.mark.asyncio
    async def test_creates_pipeline(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            with patch("src.services.pipeline_service.PipelineTargetRef"):
                mock_svc.return_value.add = AsyncMock(return_value=10)
                handlers = _get_tool_handlers(mock_db)
                result = await handlers["add_pipeline"]({
                    "confirm": True,
                    "name": "My Pipeline",
                    "prompt_template": "Write about {topic}",
                    "source_channel_ids": "1,2",
                    "target_refs": "+7123456|789",
                    "llm_model": "gpt-4",
                    "publish_mode": "auto",
                })
        text = _text(result)
        assert "id=10" in text
        assert "My Pipeline" in text

    @pytest.mark.asyncio
    async def test_requires_confirm_new(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["add_pipeline"]({"name": "Test"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_required_fields_new(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["add_pipeline"]({"name": "Test", "confirm": True})
        assert "обязательны" in _text(result)


class TestPipelinesToolTogglePipeline:
    @pytest.mark.asyncio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 99})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_toggled(self, mock_db):
        p = SimpleNamespace(id=1, name="P1", is_active=True)
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(return_value=True)
            mock_svc.return_value.get = AsyncMock(return_value=p)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 1})
        text = _text(result)
        assert "P1" in text
        assert "активирован" in text

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


class TestPipelinesToolDeletePipeline:
    @pytest.mark.asyncio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        p = SimpleNamespace(id=1, name="PipeX")
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=p)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({"pipeline_id": 1, "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.asyncio
    async def test_deletes_with_confirmation(self, mock_db):
        p = SimpleNamespace(id=1, name="PipeToDelete")
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=p)
            mock_svc.return_value.delete = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({"pipeline_id": 1, "confirm": True})
        text = _text(result)
        assert "PipeToDelete" in text
        assert "удалён" in text

    @pytest.mark.asyncio
    async def test_requires_confirm_new(self, mock_db):
        p = _make_pipeline(pk=1, name="Test")
        with patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=p)):
            handlers = _get_tool_handlers(mock_db, config=MagicMock())
            result = await handlers["delete_pipeline"]({"pipeline_id": 1})
            assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["delete_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)


class TestPipelinesToolListPipelineRuns:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})
        assert "Нет генераций" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs(self, mock_db):
        run = SimpleNamespace(
            id=10,
            status="done",
            moderation_status="approved",
            generated_text="Content here",
            created_at="2026-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1, "limit": 10})
        text = _text(result)
        assert "run_id=10" in text
        assert "approved" in text

    @pytest.mark.asyncio
    async def test_no_runs(self, mock_db):
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})
        assert "Нет генераций" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs_new(self, mock_db):
        run = _make_run(run_id=1, text="Preview text")
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})
        text = _text(result)
        assert "Генерации пайплайна id=1" in text
        assert "run_id=1" in text


class TestPipelinesToolGetPipelineRun:
    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 99})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_found(self, mock_db):
        run = SimpleNamespace(
            id=5,
            pipeline_id=2,
            status="published",
            moderation_status="approved",
            quality_score=0.9,
            created_at="2026-01-01",
            updated_at="2026-01-02",
            generated_text="Full content text",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 5})
        text = _text(result)
        assert "Run id=5" in text
        assert "Full content text" in text
        assert "approved" in text

    @pytest.mark.asyncio
    async def test_run_not_found_new(self, mock_db):
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
        assert "confirm=true" in _text(result).lower()

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


class TestEditPipelineTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["edit_pipeline"]({"pipeline_id": 1})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db, config=MagicMock())
        result = await handlers["edit_pipeline"]({"confirm": True})
        assert "pipeline_id обязателен" in _text(result)
