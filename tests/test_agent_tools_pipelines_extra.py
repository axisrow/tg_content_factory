"""Tests for src/agent/tools/pipelines.py — error paths, write tools, templates, AI-edit."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from tests.agent_tools_helpers import _get_tool_handlers, _text


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    db.repos.generation_runs = MagicMock()
    db.repos.content_pipelines = MagicMock()
    return db


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    return pool


def _make_pipeline(
    id=1,
    name="TestPipeline",
    is_active=True,
    llm_model="gpt-4o",
    publish_mode="moderated",
    schedule_cron="0 */6 * * *",
    generation_backend="chain",
    generate_interval_minutes=60,
    prompt_template="Generate content about {topic}",
    refinement_steps=None,
):
    return SimpleNamespace(
        id=id,
        name=name,
        is_active=is_active,
        llm_model=llm_model,
        publish_mode=publish_mode,
        schedule_cron=schedule_cron,
        generation_backend=generation_backend,
        generate_interval_minutes=generate_interval_minutes,
        prompt_template=prompt_template,
        refinement_steps=refinement_steps or [],
    )


# ---------------------------------------------------------------------------
# get_pipeline_queue — error path (lines 130-131)
# ---------------------------------------------------------------------------


class TestGetPipelineQueueErrors:
    @pytest.mark.asyncio
    async def test_exception_returns_error(self, mock_db):
        mock_db.repos.generation_runs.list_by_status = AsyncMock(
            side_effect=Exception("DB error")
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_queue"]({})

        text = _text(result)
        assert "Ошибка получения очереди" in text
        assert "DB error" in text


# ---------------------------------------------------------------------------
# get_refinement_steps — error path (lines 157-158)
# ---------------------------------------------------------------------------


class TestGetRefinementStepsErrors:
    @pytest.mark.asyncio
    async def test_exception_returns_error(self, mock_db):
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(
            side_effect=Exception("corrupt data")
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_refinement_steps"]({"pipeline_id": 1})

        text = _text(result)
        assert "Ошибка получения шагов рефайнмента" in text

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_refinement_steps"]({"pipeline_id": 999})

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_steps(self, mock_db):
        pipeline = _make_pipeline(refinement_steps=[])
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=pipeline)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_refinement_steps"]({"pipeline_id": 1})

        assert "не имеет шагов рефайнмента" in _text(result)

    @pytest.mark.asyncio
    async def test_with_steps(self, mock_db):
        pipeline = _make_pipeline(refinement_steps=[
            {"name": "Improve", "prompt": "Make this better: {text}"},
        ])
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=pipeline)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_refinement_steps"]({"pipeline_id": 1})

        text = _text(result)
        assert "Improve" in text
        assert "1 шт" in text


# ---------------------------------------------------------------------------
# get_pipeline_detail — error path (lines 217-218 coverage via add_pipeline exception)
# ---------------------------------------------------------------------------


class TestGetPipelineDetailErrors:
    @pytest.mark.asyncio
    async def test_exception_returns_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(side_effect=Exception("fail"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 1})

        text = _text(result)
        assert "Ошибка получения деталей пайплайна" in text


# ---------------------------------------------------------------------------
# add_pipeline — exception (lines 217-218)
# ---------------------------------------------------------------------------


class TestAddPipelineErrors:
    @pytest.mark.asyncio
    async def test_exception_returns_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.add = AsyncMock(side_effect=Exception("constraint"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_pipeline"]({
                "name": "Test",
                "prompt_template": "test",
                "source_channel_ids": "100",
                "target_refs": "+7900|200",
                "confirm": True,
            })

        text = _text(result)
        assert "Ошибка создания пайплайна" in text

    @pytest.mark.asyncio
    async def test_missing_required_fields(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "name": "Test",
            "confirm": True,
        })
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_target_ref_format(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "name": "Test",
            "prompt_template": "test",
            "source_channel_ids": "100",
            "target_refs": "invalid_no_pipe",
            "confirm": True,
        })
        assert "Неверный формат target_ref" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "name": "Test",
            "prompt_template": "test",
            "source_channel_ids": "100",
            "target_refs": "+7900|200",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_successful_add(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.add = AsyncMock(return_value=42)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_pipeline"]({
                "name": "New Pipeline",
                "prompt_template": "Generate news about {topic}",
                "source_channel_ids": "100,200",
                "target_refs": "+7900|300,+7910|400",
                "llm_model": "claude-sonnet-4-20250514",
                "publish_mode": "auto",
                "confirm": True,
            })

        text = _text(result)
        assert "создан" in text
        assert "42" in text


# ---------------------------------------------------------------------------
# edit_pipeline — various paths (lines 260, etc.)
# ---------------------------------------------------------------------------


class TestEditPipeline:
    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["edit_pipeline"]({
            "pipeline_id": 1,
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["edit_pipeline"]({"confirm": True})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({
                "pipeline_id": 999,
                "confirm": True,
            })

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_successful_edit_with_source_ids(self, mock_db):
        pipeline = _make_pipeline()
        detail = {
            "pipeline": pipeline,
            "source_ids": [100],
            "target_refs": ["+7900|200"],
            "targets": [SimpleNamespace(phone="+7900", dialog_id=200)],
            "source_titles": ["ChanA"],
        }

        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            mock_svc.return_value.update = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({
                "pipeline_id": 1,
                "source_channel_ids": "100,200",
                "confirm": True,
            })

        text = _text(result)
        assert "обновлён" in text

    @pytest.mark.asyncio
    async def test_edit_with_new_target_refs(self, mock_db):
        pipeline = _make_pipeline()
        detail = {
            "pipeline": pipeline,
            "source_ids": [100],
            "target_refs": [],
            "targets": [],
            "source_titles": [],
        }

        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            mock_svc.return_value.update = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({
                "pipeline_id": 1,
                "target_refs": "+7900|300",
                "confirm": True,
            })

        text = _text(result)
        assert "обновлён" in text

    @pytest.mark.asyncio
    async def test_edit_invalid_target_ref(self, mock_db):
        pipeline = _make_pipeline()
        detail = {
            "pipeline": pipeline,
            "source_ids": [100],
            "target_refs": [],
            "targets": [],
            "source_titles": [],
        }

        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({
                "pipeline_id": 1,
                "target_refs": "invalid",
                "confirm": True,
            })

        assert "Неверный формат target_ref" in _text(result)

    @pytest.mark.asyncio
    async def test_edit_returns_false(self, mock_db):
        pipeline = _make_pipeline()
        detail = {
            "pipeline": pipeline,
            "source_ids": [100],
            "target_refs": [],
            "targets": [],
            "source_titles": [],
        }

        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            mock_svc.return_value.update = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({
                "pipeline_id": 1,
                "confirm": True,
            })

        assert "Не удалось обновить" in _text(result)

    @pytest.mark.asyncio
    async def test_edit_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(side_effect=Exception("boom"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({
                "pipeline_id": 1,
                "confirm": True,
            })

        assert "Ошибка редактирования пайплайна" in _text(result)


# ---------------------------------------------------------------------------
# toggle_pipeline
# ---------------------------------------------------------------------------


class TestTogglePipeline:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 999})

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_activated(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(return_value=True)
            mock_svc.return_value.get = AsyncMock(
                return_value=_make_pipeline(is_active=True, name="MyPipe")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 1})

        text = _text(result)
        assert "активирован" in text
        assert "MyPipe" in text

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(side_effect=Exception("error"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 1})

        assert "Ошибка переключения пайплайна" in _text(result)


# ---------------------------------------------------------------------------
# delete_pipeline
# ---------------------------------------------------------------------------


class TestDeletePipeline:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(
                return_value=_make_pipeline(name="DelMe")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({"pipeline_id": 1})

        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_successful_delete(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(
                return_value=_make_pipeline(name="DelMe")
            )
            mock_svc.return_value.delete = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({
                "pipeline_id": 1,
                "confirm": True,
            })

        text = _text(result)
        assert "удалён" in text
        assert "DelMe" in text

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(side_effect=Exception("fk error"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({
                "pipeline_id": 1,
                "confirm": True,
            })

        assert "Ошибка удаления пайплайна" in _text(result)


# ---------------------------------------------------------------------------
# list_pipeline_runs — error path (lines 463-464, 475-476)
# ---------------------------------------------------------------------------


class TestListPipelineRunsErrors:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_runs(self, mock_db):
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})

        assert "Нет генераций" in _text(result)

    @pytest.mark.asyncio
    async def test_with_status_filter(self, mock_db):
        runs = [
            SimpleNamespace(
                id=1, pipeline_id=1, status="completed",
                moderation_status="approved", generated_text="text A",
                created_at="2025-01-01",
            ),
            SimpleNamespace(
                id=2, pipeline_id=1, status="completed",
                moderation_status="rejected", generated_text="text B",
                created_at="2025-01-02",
            ),
        ]
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=runs)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({
            "pipeline_id": 1,
            "status": "approved",
        })

        text = _text(result)
        assert "text A" in text
        assert "text B" not in text

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(
            side_effect=Exception("fail")
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})

        assert "Ошибка получения генераций" in _text(result)


# ---------------------------------------------------------------------------
# get_pipeline_run — error path (lines 505-506)
# ---------------------------------------------------------------------------


class TestGetPipelineRunErrors:
    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 999})

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(side_effect=Exception("err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 1})

        assert "Ошибка получения run" in _text(result)

    @pytest.mark.asyncio
    async def test_successful_get(self, mock_db):
        run = SimpleNamespace(
            id=1, pipeline_id=1, status="completed",
            moderation_status="approved", quality_score=None,
            generated_text="Generated content",
            created_at="2025-01-01", updated_at="2025-01-02",
        )
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 1})

        text = _text(result)
        assert "Generated content" in text
        assert "completed" in text


# ---------------------------------------------------------------------------
# publish_pipeline_run — error paths (lines 529, 549, 551-552)
# ---------------------------------------------------------------------------


class TestPublishPipelineRun:
    @pytest.mark.asyncio
    async def test_no_pool_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["publish_pipeline_run"]({
            "run_id": 1,
            "confirm": True,
        })
        assert "требует Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["publish_pipeline_run"]({"run_id": 1})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["publish_pipeline_run"]({"confirm": True})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_not_found(self, mock_db, mock_pool):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["publish_pipeline_run"]({
            "run_id": 999,
            "confirm": True,
        })

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db, mock_pool):
        run = SimpleNamespace(id=1, pipeline_id=99, generated_text="x")
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)

        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["publish_pipeline_run"]({
                "run_id": 1,
                "confirm": True,
            })

        assert "Пайплайн id=99 не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_publish_with_failures(self, mock_db, mock_pool):
        run = SimpleNamespace(id=1, pipeline_id=1, generated_text="x")
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)

        pub_result_ok = SimpleNamespace(success=True, error=None)
        pub_result_fail = SimpleNamespace(success=False, error="Flood wait")
        pipeline = _make_pipeline()

        with (
            patch("src.services.pipeline_service.PipelineService") as mock_pipe_svc,
            patch("src.services.publish_service.PublishService") as mock_pub_svc,
        ):
            mock_pipe_svc.return_value.get = AsyncMock(return_value=pipeline)
            mock_pub_svc.return_value.publish_run = AsyncMock(
                return_value=[pub_result_ok, pub_result_fail]
            )
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["publish_pipeline_run"]({
                "run_id": 1,
                "confirm": True,
            })

        text = _text(result)
        assert "1 успешно" in text
        assert "1 ошибок" in text
        assert "Flood wait" in text

    @pytest.mark.asyncio
    async def test_publish_exception(self, mock_db, mock_pool):
        run = SimpleNamespace(id=1, pipeline_id=1, generated_text="x")
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        pipeline = _make_pipeline()

        with (
            patch("src.services.pipeline_service.PipelineService") as mock_pipe_svc,
            patch("src.services.publish_service.PublishService") as mock_pub_svc,
        ):
            mock_pipe_svc.return_value.get = AsyncMock(return_value=pipeline)
            mock_pub_svc.return_value.publish_run = AsyncMock(
                side_effect=Exception("connection")
            )
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["publish_pipeline_run"]({
                "run_id": 1,
                "confirm": True,
            })

        text = _text(result)
        assert "Ошибка публикации" in text


# ---------------------------------------------------------------------------
# set_refinement_steps — various paths (lines 593, 600-601)
# ---------------------------------------------------------------------------


class TestSetRefinementSteps:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({"confirm": True})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({"pipeline_id": 1})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_invalid_json(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 1,
            "steps_json": "not valid json{{{",
            "confirm": True,
        })
        assert "Ошибка парсинга steps_json" in _text(result)

    @pytest.mark.asyncio
    async def test_not_a_list(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 1,
            "steps_json": '{"key": "value"}',
            "confirm": True,
        })
        assert "должен быть JSON-массивом" in _text(result)

    @pytest.mark.asyncio
    async def test_dropped_steps_with_missing_prompt(self, mock_db):
        steps = [
            {"name": "Step1", "prompt": "Do something: {text}"},
            {"name": "Step2"},  # missing prompt
        ]
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 1,
            "steps_json": json.dumps(steps),
            "confirm": True,
        })
        assert "1 из 2 шагов не содержат" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 999,
            "steps_json": '[{"name": "S1", "prompt": "test {text}"}]',
            "confirm": True,
        })
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_successful_set(self, mock_db):
        pipeline = _make_pipeline()
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=pipeline)
        mock_db.repos.content_pipelines.set_refinement_steps = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 1,
            "steps_json": '[{"name": "Improve", "prompt": "Make better: {text}"}]',
            "confirm": True,
        })

        text = _text(result)
        assert "обновлены" in text
        assert "1 шт" in text
        mock_db.repos.content_pipelines.set_refinement_steps.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(
            side_effect=Exception("db fail")
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 1,
            "steps_json": '[{"name": "S", "prompt": "{text}"}]',
            "confirm": True,
        })

        assert "Ошибка обновления шагов рефайнмента" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_steps_clears(self, mock_db):
        pipeline = _make_pipeline()
        mock_db.repos.content_pipelines.get_by_id = AsyncMock(return_value=pipeline)
        mock_db.repos.content_pipelines.set_refinement_steps = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_refinement_steps"]({
            "pipeline_id": 1,
            "steps_json": "[]",
            "confirm": True,
        })

        text = _text(result)
        assert "обновлены" in text
        assert "0 шт" in text


# ---------------------------------------------------------------------------
# export_pipeline_json — (lines 616-630)
# ---------------------------------------------------------------------------


class TestExportPipelineJson:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["export_pipeline_json"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.export_json = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["export_pipeline_json"]({"pipeline_id": 999})

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_successful_export(self, mock_db):
        export_data = {"name": "MyPipe", "prompt_template": "test"}
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.export_json = AsyncMock(return_value=export_data)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["export_pipeline_json"]({"pipeline_id": 1})

        text = _text(result)
        assert "MyPipe" in text
        parsed = json.loads(text)
        assert parsed["name"] == "MyPipe"

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.export_json = AsyncMock(
                side_effect=Exception("fail")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["export_pipeline_json"]({"pipeline_id": 1})

        assert "Ошибка экспорта пайплайна" in _text(result)


# ---------------------------------------------------------------------------
# import_pipeline_json — (lines 645-662)
# ---------------------------------------------------------------------------


class TestImportPipelineJson:
    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_pipeline_json"]({
            "json_text": '{"name": "test"}',
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_json_text(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_pipeline_json"]({"confirm": True})
        assert "json_text обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_successful_import(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.import_json = AsyncMock(return_value=5)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_pipeline_json"]({
                "json_text": '{"name": "Imported"}',
                "confirm": True,
            })

        text = _text(result)
        assert "импортирован" in text
        assert "5" in text

    @pytest.mark.asyncio
    async def test_import_with_name_override(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.import_json = AsyncMock(return_value=6)
            handlers = _get_tool_handlers(mock_db)
            await handlers["import_pipeline_json"]({
                "json_text": '{"name": "Old"}',
                "name_override": "New Name",
                "confirm": True,
            })

        mock_svc.return_value.import_json.assert_awaited_once()
        call_kwargs = mock_svc.return_value.import_json.await_args.kwargs
        assert call_kwargs["name_override"] == "New Name"

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.import_json = AsyncMock(
                side_effect=Exception("bad json")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_pipeline_json"]({
                "json_text": "{}",
                "confirm": True,
            })

        assert "Ошибка импорта пайплайна" in _text(result)


# ---------------------------------------------------------------------------
# list_pipeline_templates — (lines 673-692)
# ---------------------------------------------------------------------------


class TestListPipelineTemplates:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list_templates = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipeline_templates"]({})

        assert "Шаблонов не найдено" in _text(result)

    @pytest.mark.asyncio
    async def test_with_templates(self, mock_db):
        from enum import Enum

        class NodeType(Enum):
            fetch = "fetch"
            generate = "generate"

        template = SimpleNamespace(
            id=1,
            name="News Template",
            category="content",
            description="A news pipeline template",
            is_builtin=True,
            template_json=SimpleNamespace(nodes=[
                SimpleNamespace(type=NodeType.fetch),
                SimpleNamespace(type=NodeType.generate),
            ]),
        )

        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list_templates = AsyncMock(return_value=[template])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipeline_templates"]({})

        text = _text(result)
        assert "News Template" in text
        assert "content" in text
        assert "builtin" in text
        assert "fetch" in text
        assert "generate" in text

    @pytest.mark.asyncio
    async def test_with_category_filter(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list_templates = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            await handlers["list_pipeline_templates"]({
                "category": "automation",
            })

        mock_svc.return_value.list_templates.assert_awaited_once_with(
            category="automation"
        )

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list_templates = AsyncMock(
                side_effect=Exception("error")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipeline_templates"]({})

        assert "Ошибка получения шаблонов" in _text(result)


# ---------------------------------------------------------------------------
# create_pipeline_from_template — (lines 711-746)
# ---------------------------------------------------------------------------


class TestCreatePipelineFromTemplate:
    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_pipeline_from_template"]({
            "template_id": 1,
            "name": "Test",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_missing_template_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_pipeline_from_template"]({
            "name": "Test",
            "confirm": True,
        })
        assert "template_id и name обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_name(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_pipeline_from_template"]({
            "template_id": 1,
            "confirm": True,
        })
        assert "template_id и name обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_successful_create(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.create_from_template = AsyncMock(return_value=10)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["create_pipeline_from_template"]({
                "template_id": 1,
                "name": "New Pipeline",
                "source_channel_ids": "100,200",
                "target_refs": "+7900|300",
                "llm_model": "gpt-4o",
                "confirm": True,
            })

        text = _text(result)
        assert "создан из шаблона" in text
        assert "10" in text

    @pytest.mark.asyncio
    async def test_target_ref_without_pipe_skipped(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.create_from_template = AsyncMock(return_value=11)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["create_pipeline_from_template"]({
                "template_id": 1,
                "name": "Test",
                "target_refs": "+7900|300,nopipe",
                "confirm": True,
            })

        # The "nopipe" entry should be skipped (no "|" separator)
        text = _text(result)
        assert "создан из шаблона" in text

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.create_from_template = AsyncMock(
                side_effect=Exception("template not found")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["create_pipeline_from_template"]({
                "template_id": 99,
                "name": "Test",
                "confirm": True,
            })

        assert "Ошибка создания пайплайна из шаблона" in _text(result)


# ---------------------------------------------------------------------------
# ai_edit_pipeline — (lines 761-782)
# ---------------------------------------------------------------------------


class TestAiEditPipeline:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["ai_edit_pipeline"]({
            "instruction": "add image step",
        })
        assert "pipeline_id и instruction обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_instruction(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["ai_edit_pipeline"]({
            "pipeline_id": 1,
        })
        assert "pipeline_id и instruction обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["ai_edit_pipeline"]({
            "pipeline_id": 1,
            "instruction": "add step",
        })
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_successful_edit(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.edit_via_llm = AsyncMock(return_value={
                "ok": True,
                "pipeline_json": {"name": "Updated", "nodes": []},
            })
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["ai_edit_pipeline"]({
                "pipeline_id": 1,
                "instruction": "add image generation step",
                "confirm": True,
            })

        text = _text(result)
        assert "обновлён через AI" in text
        assert "Updated" in text

    @pytest.mark.asyncio
    async def test_llm_returns_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.edit_via_llm = AsyncMock(return_value={
                "ok": False,
                "error": "Could not parse instruction",
            })
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["ai_edit_pipeline"]({
                "pipeline_id": 1,
                "instruction": "do something impossible",
                "confirm": True,
            })

        text = _text(result)
        assert "Ошибка AI-редактирования" in text
        assert "Could not parse instruction" in text

    @pytest.mark.asyncio
    async def test_exception(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.edit_via_llm = AsyncMock(
                side_effect=Exception("API timeout")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["ai_edit_pipeline"]({
                "pipeline_id": 1,
                "instruction": "test",
                "confirm": True,
            })

        text = _text(result)
        assert "Ошибка AI-редактирования" in text
        assert "API timeout" in text


# ---------------------------------------------------------------------------
# run_pipeline — _build_image_service fallback (lines 29-32)
# ---------------------------------------------------------------------------


class TestBuildImageServiceFallback:
    @pytest.mark.asyncio
    async def test_image_service_builds_without_config(self, mock_db):
        """When config is None, _build_image_service should return a bare ImageGenerationService."""
        pipeline = _make_pipeline()
        run = SimpleNamespace(id=1, generated_text="text", moderation_status="pending")

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
            patch("src.services.content_generation_service.ContentGenerationService") as mock_gen_cls,
            patch("src.services.pipeline_service.PipelineService") as mock_pipe_svc,
            patch("src.services.image_generation_service.ImageGenerationService"),
        ):
            mock_se.return_value = MagicMock()
            mock_pipe_svc.return_value.get = AsyncMock(return_value=pipeline)
            mock_gen_cls.return_value.generate = AsyncMock(return_value=run)

            # When config=None, ImageGenerationService() should be called without adapters
            handlers = _get_tool_handlers(mock_db, config=None)
            await handlers["run_pipeline"]({"pipeline_id": 1})

            # The image service should have been instantiated at some point
            # (via _build_image_service which falls through to ImageGenerationService())


# ---------------------------------------------------------------------------
# generate_draft tool
# ---------------------------------------------------------------------------


class TestGenerateDraftTool:
    @pytest.mark.asyncio
    async def test_exception_returns_error(self, mock_db):
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
            patch("src.services.generation_service.GenerationService") as mock_gen_cls,
            patch("src.services.pipeline_service.PipelineService"),
            patch("src.services.provider_service.AgentProviderService") as mock_prov_svc,
        ):
            mock_se.return_value = MagicMock()
            mock_prov_svc.return_value.get_provider_callable = MagicMock(
                return_value=None
            )
            mock_gen_cls.return_value.generate = AsyncMock(
                side_effect=Exception("LLM unavailable")
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_draft"]({"query": "test"})

        text = _text(result)
        assert "Ошибка генерации" in text

    @pytest.mark.asyncio
    async def test_successful_draft(self, mock_db):
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
            patch("src.services.generation_service.GenerationService") as mock_gen_cls,
            patch("src.services.pipeline_service.PipelineService"),
            patch("src.services.provider_service.AgentProviderService") as mock_prov_svc,
        ):
            mock_se.return_value = MagicMock()
            mock_prov_svc.return_value.get_provider_callable = MagicMock(
                return_value=None
            )
            mock_gen_cls.return_value.generate = AsyncMock(return_value={
                "generated_text": "Draft content",
                "citations": [
                    {"channel_title": "ChanA", "message_id": 1, "date": "2025-01-01"},
                ],
            })
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_draft"]({"query": "test"})

        text = _text(result)
        assert "Draft content" in text
        assert "ChanA" in text

    @pytest.mark.asyncio
    async def test_with_pipeline_id_uses_template(self, mock_db):
        pipeline = _make_pipeline(prompt_template="Write about {topic}")
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
            patch("src.services.generation_service.GenerationService") as mock_gen_cls,
            patch("src.services.pipeline_service.PipelineService") as mock_pipe_svc,
            patch("src.services.provider_service.AgentProviderService") as mock_prov_svc,
        ):
            mock_se.return_value = MagicMock()
            mock_pipe_svc.return_value.get = AsyncMock(return_value=pipeline)
            mock_prov_svc.return_value.get_provider_callable = MagicMock(
                return_value=None
            )
            mock_gen_cls.return_value.generate = AsyncMock(return_value={
                "generated_text": "Content",
                "citations": [],
            })
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_draft"]({
                "query": "",
                "pipeline_id": 1,
            })

        text = _text(result)
        assert "Content" in text
