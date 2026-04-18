"""Tests for uncovered UnifiedDispatcher handler paths."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
    TranslateBatchTaskPayload,
)
from src.services.unified_dispatcher import UnifiedDispatcher


def _task(task_type, task_id=1, payload=None, status=CollectionTaskStatus.PENDING):
    return CollectionTask(id=task_id, task_type=task_type, status=status, payload=payload)


def _dispatcher(**kw):
    collector = MagicMock()
    collector.is_running = False
    collector.delay_between_channels_sec = 0.0
    collector.collect_channel_stats = AsyncMock(return_value={"subscribers": 100})
    collector.get_stats_availability = AsyncMock(
        return_value=MagicMock(state="ok", next_available_at_utc=None)
    )
    channel_bundle = MagicMock()
    channel_bundle.get_by_channel_id = AsyncMock(return_value=MagicMock(channel_id=42))
    tasks = AsyncMock()
    tasks.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=0)
    tasks.claim_next_due_generic_task = AsyncMock(return_value=None)
    tasks.update_collection_task = AsyncMock()
    tasks.get_collection_task = AsyncMock(return_value=None)
    defaults = dict(
        collector=collector,
        channel_bundle=channel_bundle,
        tasks_repo=tasks,
        poll_interval_sec=0.01,
    )
    defaults.update(kw)
    return UnifiedDispatcher(**defaults)


@pytest.mark.asyncio
async def test_dispatch_unknown_type():
    d = _dispatcher()
    # CHANNEL_COLLECT is not in the handler map, so it triggers "Unknown" path
    t = _task(CollectionTaskType.CHANNEL_COLLECT)
    await d._dispatch(t)
    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED
    assert "Unknown" in call[1]["error"]


@pytest.mark.asyncio
async def test_stats_all_no_id():
    d = _dispatcher()
    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_stats_all_wrong_payload():
    d = _dispatcher()
    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload={"bad": True}))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_stats_all_empty_ids_completes():
    d = _dispatcher()
    p = StatsAllTaskPayload(channel_ids=[])
    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_stats_all_collects_and_completes():
    d = _dispatcher()
    ch = MagicMock(channel_id=42)
    d._channel_bundle.get_by_channel_id = AsyncMock(return_value=ch)
    p = StatsAllTaskPayload(channel_ids=[42])
    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_stats_all_channel_not_found():
    d = _dispatcher()
    d._channel_bundle.get_by_channel_id = AsyncMock(return_value=None)
    p = StatsAllTaskPayload(channel_ids=[999])
    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_stats_all_reschedules_on_flood_wait():
    """_handle_stats_all reschedules same task when all clients flooded."""
    d = _dispatcher()
    ch = MagicMock(channel_id=42)
    d._channel_bundle.get_by_channel_id = AsyncMock(return_value=ch)
    d._collector.collect_channel_stats = AsyncMock(return_value=None)
    d._collector.get_stats_availability = AsyncMock(return_value=MagicMock(
        state="all_flooded",
        next_available_at_utc=datetime.now(timezone.utc),
    ))
    d._tasks.reschedule_stats_task = AsyncMock()
    p = StatsAllTaskPayload(channel_ids=[42, 43])
    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload=p))
    d._tasks.reschedule_stats_task.assert_called_once()
    call_args = d._tasks.reschedule_stats_task.call_args
    assert call_args[0][0] == 1  # task_id
    assert call_args[1]["payload"].next_index == 0


@pytest.mark.asyncio
async def test_sq_stats_no_id():
    d = _dispatcher()
    await d._handle_sq_stats(_task(CollectionTaskType.SQ_STATS, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_sq_stats_no_bundle():
    d = _dispatcher(sq_bundle=None)
    await d._handle_sq_stats(_task(CollectionTaskType.SQ_STATS))
    d._tasks.update_collection_task.assert_called_once()


@pytest.mark.asyncio
async def test_sq_stats_wrong_payload():
    sq_bundle = MagicMock()
    d = _dispatcher(sq_bundle=sq_bundle)
    await d._handle_sq_stats(_task(CollectionTaskType.SQ_STATS, payload={"bad": True}))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_sq_stats_query_not_found():
    sq_bundle = MagicMock()
    sq_bundle.get_by_id = AsyncMock(return_value=None)
    d = _dispatcher(sq_bundle=sq_bundle)
    p = SqStatsTaskPayload(sq_id=99)
    await d._handle_sq_stats(_task(CollectionTaskType.SQ_STATS, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_sq_stats_records_stat():
    sq = MagicMock(query="test query")
    sq_bundle = MagicMock()
    sq_bundle.get_by_id = AsyncMock(return_value=sq)
    sq_bundle.get_fts_daily_stats_for_query = AsyncMock(return_value=[])
    sq_bundle.record_stat = AsyncMock()
    d = _dispatcher(sq_bundle=sq_bundle)
    p = SqStatsTaskPayload(sq_id=1)
    await d._handle_sq_stats(_task(CollectionTaskType.SQ_STATS, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_due_no_id():
    d = _dispatcher()
    await d._handle_photo_due(_task(CollectionTaskType.PHOTO_DUE, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_photo_due_no_service():
    d = _dispatcher(photo_task_service=None)
    await d._handle_photo_due(_task(CollectionTaskType.PHOTO_DUE))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_due_success():
    pts = AsyncMock()
    pts.run_due = AsyncMock(return_value=5)
    d = _dispatcher(photo_task_service=pts)
    await d._handle_photo_due(_task(CollectionTaskType.PHOTO_DUE))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_due_error():
    pts = AsyncMock()
    pts.run_due = AsyncMock(side_effect=RuntimeError("boom"))
    d = _dispatcher(photo_task_service=pts)
    await d._handle_photo_due(_task(CollectionTaskType.PHOTO_DUE))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_photo_auto_no_id():
    d = _dispatcher()
    await d._handle_photo_auto(_task(CollectionTaskType.PHOTO_AUTO, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_photo_auto_no_service():
    d = _dispatcher(photo_auto_upload_service=None)
    await d._handle_photo_auto(_task(CollectionTaskType.PHOTO_AUTO))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_auto_success():
    paus = AsyncMock()
    paus.run_due = AsyncMock(return_value=3)
    d = _dispatcher(photo_auto_upload_service=paus)
    await d._handle_photo_auto(_task(CollectionTaskType.PHOTO_AUTO))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_auto_error():
    paus = AsyncMock()
    paus.run_due = AsyncMock(side_effect=RuntimeError("boom"))
    d = _dispatcher(photo_auto_upload_service=paus)
    await d._handle_photo_auto(_task(CollectionTaskType.PHOTO_AUTO))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_pipeline_run_no_id():
    d = _dispatcher()
    await d._handle_pipeline_run(_task(CollectionTaskType.PIPELINE_RUN, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_pipeline_run_wrong_payload():
    d = _dispatcher()
    await d._handle_pipeline_run(_task(CollectionTaskType.PIPELINE_RUN, payload={"bad": True}))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_pipeline_run_no_deps():
    d = _dispatcher(pipeline_bundle=None, search_engine=None, db=None)
    p = PipelineRunTaskPayload(pipeline_id=1)
    await d._handle_pipeline_run(_task(CollectionTaskType.PIPELINE_RUN, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_content_generate_no_id():
    d = _dispatcher()
    await d._handle_content_generate(_task(CollectionTaskType.CONTENT_GENERATE, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_content_generate_wrong_payload():
    d = _dispatcher()
    await d._handle_content_generate(_task(CollectionTaskType.CONTENT_GENERATE, payload={"bad": True}))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_content_generate_no_deps():
    d = _dispatcher(pipeline_bundle=None, search_engine=None, db=None)
    p = ContentGenerateTaskPayload(pipeline_id=1)
    await d._handle_content_generate(_task(CollectionTaskType.CONTENT_GENERATE, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_content_publish_no_id():
    d = _dispatcher()
    await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_content_publish_wrong_payload():
    d = _dispatcher()
    await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, payload={"bad": True}))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_content_publish_no_deps():
    d = _dispatcher(pipeline_bundle=None, db=None)
    p = ContentPublishTaskPayload(pipeline_id=1)
    await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_translate_batch_no_id():
    d = _dispatcher()
    await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, task_id=None))
    d._tasks.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_translate_batch_wrong_payload():
    d = _dispatcher()
    await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload={"bad": True}))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_translate_batch_no_db():
    d = _dispatcher(db=None)
    p = TranslateBatchTaskPayload()
    await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload=p))
    assert d._tasks.update_collection_task.call_args[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_start_stop():
    d = _dispatcher()
    await d.start()
    assert d._task is not None
    await d.stop()
    assert d._task is None


@pytest.mark.asyncio
async def test_start_idempotent():
    d = _dispatcher()
    await d.start()
    t = d._task
    await d.start()
    assert d._task is t
    await d.stop()


@pytest.mark.asyncio
async def test_build_image_service_no_db():
    d = _dispatcher(db=None, config=None)
    svc = await d._build_image_service()
    assert svc is not None


@pytest.mark.asyncio
async def test_build_image_service_db_exception():
    mock_db = MagicMock()
    mock_config = MagicMock()
    d = _dispatcher(db=mock_db, config=mock_config)
    with patch("src.services.image_provider_service.ImageProviderService", side_effect=Exception("nope")):
        svc = await d._build_image_service()
        assert svc is not None


# ── Issue #463: happy path with semantic assertions ──────────────────────────


def _make_pipeline_for_dispatcher(pipeline_id: int):
    from src.models import ContentPipeline, PipelinePublishMode

    return ContentPipeline(
        id=pipeline_id,
        name="Test",
        llm_model="gpt-4",
        publish_mode=PipelinePublishMode.MODERATED,
        is_active=True,
    )


def _dispatcher_for_pipeline_run(run, *, pipeline_id: int = 7):
    """Build a dispatcher with just enough pipeline deps to reach ContentGenerationService.generate."""
    db = MagicMock()
    db.repos = MagicMock()
    db.repos.generation_runs = MagicMock()
    db.repos.generation_runs.set_status = AsyncMock()

    d = _dispatcher(
        pipeline_bundle=MagicMock(),
        search_engine=MagicMock(),
        db=db,
        llm_provider_service=MagicMock(),
    )
    return d


class TestPipelineRunHappyPathSemantics:
    """Verify that messages_collected on the CollectionTask row reflects
    GenerationRun.result_count exactly — for all three run shapes.
    """

    @pytest.mark.asyncio
    async def test_generation_run_maps_citation_count_to_messages_collected(self, monkeypatch):
        from tests.factories.pipeline_runs import make_generation_run

        run = make_generation_run(run_id=42, pipeline_id=7, citations_count=3)
        d = _dispatcher_for_pipeline_run(run, pipeline_id=7)

        async def fake_get(self, pipeline_id):
            return _make_pipeline_for_dispatcher(pipeline_id)

        async def fake_generate(self, *, pipeline, model, dry_run, since_hours):
            return run

        monkeypatch.setattr(
            "src.services.pipeline_service.PipelineService.get",
            fake_get,
        )
        monkeypatch.setattr(
            "src.services.content_generation_service.ContentGenerationService.generate",
            fake_generate,
        )
        monkeypatch.setattr(
            UnifiedDispatcher,
            "_build_image_service",
            AsyncMock(return_value=MagicMock()),
        )

        task = _task(
            CollectionTaskType.PIPELINE_RUN,
            task_id=100,
            payload=PipelineRunTaskPayload(pipeline_id=7),
        )
        await d._handle_pipeline_run(task)

        call = d._tasks.update_collection_task.call_args
        assert call.args[0] == 100
        assert call.args[1] == CollectionTaskStatus.COMPLETED
        assert call.kwargs["messages_collected"] == 3
        assert "id=42" in call.kwargs["note"]

    @pytest.mark.asyncio
    async def test_action_only_run_propagates_action_count(self, monkeypatch):
        from tests.factories.pipeline_runs import make_action_only_run

        run = make_action_only_run(run_id=43, pipeline_id=7, action_counts={"react": 5})
        d = _dispatcher_for_pipeline_run(run, pipeline_id=7)

        async def fake_get(self, pipeline_id):
            return _make_pipeline_for_dispatcher(pipeline_id)

        async def fake_generate(self, *, pipeline, model, dry_run, since_hours):
            return run

        monkeypatch.setattr("src.services.pipeline_service.PipelineService.get", fake_get)
        monkeypatch.setattr(
            "src.services.content_generation_service.ContentGenerationService.generate",
            fake_generate,
        )
        monkeypatch.setattr(
            UnifiedDispatcher,
            "_build_image_service",
            AsyncMock(return_value=MagicMock()),
        )

        task = _task(
            CollectionTaskType.PIPELINE_RUN,
            task_id=101,
            payload=PipelineRunTaskPayload(pipeline_id=7),
        )
        await d._handle_pipeline_run(task)

        call = d._tasks.update_collection_task.call_args
        assert call.kwargs["messages_collected"] == 5
        assert call.args[1] == CollectionTaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_mixed_run_uses_generation_count(self, monkeypatch):
        """Mixed run: dispatcher stores generation count (not action count),
        consistent with summarize_result() precedence.
        """
        from tests.factories.pipeline_runs import make_mixed_run

        run = make_mixed_run(run_id=44, pipeline_id=7, citations_count=2, action_counts={"react": 9})
        d = _dispatcher_for_pipeline_run(run, pipeline_id=7)

        async def fake_get(self, pipeline_id):
            return _make_pipeline_for_dispatcher(pipeline_id)

        async def fake_generate(self, *, pipeline, model, dry_run, since_hours):
            return run

        monkeypatch.setattr("src.services.pipeline_service.PipelineService.get", fake_get)
        monkeypatch.setattr(
            "src.services.content_generation_service.ContentGenerationService.generate",
            fake_generate,
        )
        monkeypatch.setattr(
            UnifiedDispatcher,
            "_build_image_service",
            AsyncMock(return_value=MagicMock()),
        )

        task = _task(
            CollectionTaskType.PIPELINE_RUN,
            task_id=102,
            payload=PipelineRunTaskPayload(pipeline_id=7),
        )
        await d._handle_pipeline_run(task)

        call = d._tasks.update_collection_task.call_args
        assert call.kwargs["messages_collected"] == 2  # generation wins

    @pytest.mark.asyncio
    async def test_empty_text_positive_count_regression(self, monkeypatch):
        """Issue #463 regression: run with empty text but positive result_count
        must NOT be stored as messages_collected=0.
        """
        from tests.factories.pipeline_runs import make_action_only_run

        run = make_action_only_run(run_id=45, pipeline_id=7, action_counts={"forward": 4})
        assert (run.generated_text or "") == ""
        assert run.result_count == 4

        d = _dispatcher_for_pipeline_run(run, pipeline_id=7)

        async def fake_get(self, pipeline_id):
            return _make_pipeline_for_dispatcher(pipeline_id)

        async def fake_generate(self, *, pipeline, model, dry_run, since_hours):
            return run

        monkeypatch.setattr("src.services.pipeline_service.PipelineService.get", fake_get)
        monkeypatch.setattr(
            "src.services.content_generation_service.ContentGenerationService.generate",
            fake_generate,
        )
        monkeypatch.setattr(
            UnifiedDispatcher,
            "_build_image_service",
            AsyncMock(return_value=MagicMock()),
        )

        task = _task(
            CollectionTaskType.PIPELINE_RUN,
            task_id=103,
            payload=PipelineRunTaskPayload(pipeline_id=7),
        )
        await d._handle_pipeline_run(task)

        call = d._tasks.update_collection_task.call_args
        assert call.kwargs["messages_collected"] == 4, (
            "Dispatcher must derive messages_collected from run.result_count, "
            "NOT from len(generated_text) or similar heuristics."
        )
