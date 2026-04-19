"""Extra tests for UnifiedDispatcher to increase coverage from 78% to 80%+.

Focuses on uncovered paths: translate_batch full flow, content_publish full flow,
content_generate auto-publish failure, run_loop exception recovery with task marking,
pipeline_run outer exception, _build_image_service env-only adapters, and
stats_all stop_event interruption.
"""
from __future__ import annotations

import asyncio
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
from src.services.unified_dispatcher import HANDLED_TYPES, UnifiedDispatcher

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dispatcher(**kw):
    """Create a UnifiedDispatcher with sensible defaults."""
    collector = MagicMock()
    collector.is_running = False
    collector.delay_between_channels_sec = 0.0
    collector.collect_channel_stats = AsyncMock(return_value=MagicMock(subscriber_count=100))
    collector.get_stats_availability = AsyncMock(
        return_value=MagicMock(state="ok", next_available_at_utc=None)
    )
    channel_bundle = MagicMock()
    tasks = MagicMock()
    tasks.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=0)
    tasks.claim_next_due_generic_task = AsyncMock(return_value=None)
    tasks.update_collection_task = AsyncMock()
    tasks.update_collection_task_progress = AsyncMock()
    tasks.persist_stats_progress = AsyncMock()
    tasks.get_collection_task = AsyncMock(return_value=None)
    tasks.create_stats_continuation_task = AsyncMock(return_value=999)
    tasks.reschedule_stats_task = AsyncMock()
    tasks.create_generic_task = AsyncMock(return_value=100)
    defaults = dict(
        collector=collector,
        channel_bundle=channel_bundle,
        tasks_repo=tasks,
        poll_interval_sec=0.01,
    )
    defaults.update(kw)
    return UnifiedDispatcher(**defaults)


def _task(task_type, task_id=1, payload=None, status=CollectionTaskStatus.RUNNING):
    return CollectionTask(id=task_id, task_type=task_type, status=status, payload=payload)


# ---------------------------------------------------------------------------
# translate_batch: full happy path with remaining messages -> self-chaining
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_translate_batch_full_flow_with_chaining():
    """Translate batch processes messages and chains follow-up task when more remain."""
    mock_db = MagicMock()

    # provider_service.get_provider_callable returns a non-default callable
    mock_provider_fn = AsyncMock(return_value="translated text")

    # translation service returns batch results
    mock_msg1 = MagicMock(id=10)
    mock_msg1.id = 10
    mock_msg2 = MagicMock(id=20)
    mock_msg2.id = 20

    mock_db.repos.messages.get_untranslated_messages = AsyncMock(
        side_effect=[[mock_msg1, mock_msg2], [MagicMock(id=30)]]  # first batch, then "remaining"
    )
    mock_db.repos.messages.update_translation = AsyncMock()
    mock_db.get_setting = AsyncMock(return_value=None)

    d = _make_dispatcher(db=mock_db)
    d._tasks.create_generic_task = AsyncMock(return_value=200)

    payload = TranslateBatchTaskPayload(target_lang="en", batch_size=10)

    with (
        patch("src.services.provider_service.AgentProviderService") as mock_aps,
        patch("src.services.translation_service.TranslationService") as mock_ts,
    ):
        aps_instance = mock_aps.return_value
        aps_instance.get_provider_callable.return_value = mock_provider_fn
        # Ensure it's NOT the default stub
        aps_instance._registry = {"default": object(), "real": mock_provider_fn}
        aps_instance.get_provider_callable.return_value = mock_provider_fn

        ts_instance = mock_ts.return_value
        ts_instance.translate_batch = AsyncMock(
            return_value=[(10, "translated text 1"), (20, "translated text 2")]
        )

        await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload=payload))

    # Task should be completed with messages_collected=2
    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert call[1].get("messages_collected") == 2

    # Follow-up task should be created because remaining msgs found
    d._tasks.create_generic_task.assert_called_once()


@pytest.mark.asyncio
async def test_translate_batch_no_remaining_no_chain():
    """Translate batch does not chain when no more messages remain."""
    mock_db = MagicMock()
    mock_msg = MagicMock(id=10)
    mock_msg.id = 10

    mock_db.repos.messages.get_untranslated_messages = AsyncMock(
        side_effect=[[mock_msg], []]  # batch, then no remaining
    )
    mock_db.repos.messages.update_translation = AsyncMock()
    mock_db.get_setting = AsyncMock(return_value=None)

    d = _make_dispatcher(db=mock_db)

    payload = TranslateBatchTaskPayload(target_lang="en")

    mock_provider_fn = AsyncMock(return_value="translated")

    with (
        patch("src.services.provider_service.AgentProviderService") as mock_aps,
        patch("src.services.translation_service.TranslationService") as mock_ts,
    ):
        aps_instance = mock_aps.return_value
        aps_instance._registry = {"default": object(), "real": mock_provider_fn}
        aps_instance.get_provider_callable.return_value = mock_provider_fn

        ts_instance = mock_ts.return_value
        ts_instance.translate_batch = AsyncMock(return_value=[(10, "translated")])

        await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload=payload))

    d._tasks.create_generic_task.assert_not_called()


@pytest.mark.asyncio
async def test_translate_batch_no_messages():
    """Translate batch completes immediately when no messages to translate."""
    mock_db = MagicMock()
    mock_db.repos.messages.get_untranslated_messages = AsyncMock(return_value=[])
    mock_db.get_setting = AsyncMock(return_value=None)

    d = _make_dispatcher(db=mock_db)

    payload = TranslateBatchTaskPayload(target_lang="fr")

    mock_provider_fn = AsyncMock(return_value="translated")

    with (
        patch("src.services.provider_service.AgentProviderService") as mock_aps,
    ):
        aps_instance = mock_aps.return_value
        aps_instance._registry = {"default": object(), "real": mock_provider_fn}
        aps_instance.get_provider_callable.return_value = mock_provider_fn

        await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert "No more messages" in call[1].get("note", "")


@pytest.mark.asyncio
async def test_translate_batch_only_stub_provider():
    """Translate batch fails when only the stub default provider is available."""
    mock_db = MagicMock()
    mock_db.get_setting = AsyncMock(return_value=None)

    d = _make_dispatcher(db=mock_db)

    payload = TranslateBatchTaskPayload(target_lang="en")

    default_stub = AsyncMock(return_value="stub")

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        aps_instance = mock_aps.return_value
        aps_instance._registry = {"default": default_stub}
        aps_instance.get_provider_callable.return_value = default_stub

        await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED
    assert "stub" in call[1].get("error", "").lower()


@pytest.mark.asyncio
async def test_translate_batch_exception():
    """Translate batch handles exceptions during processing."""
    mock_db = MagicMock()
    mock_db.get_setting = AsyncMock(side_effect=RuntimeError("config error"))

    d = _make_dispatcher(db=mock_db)
    payload = TranslateBatchTaskPayload(target_lang="en")

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        mock_aps.return_value.get_provider_callable.return_value = AsyncMock()

        await d._handle_translate_batch(_task(CollectionTaskType.TRANSLATE_BATCH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED


# ---------------------------------------------------------------------------
# content_generate: auto-publish failure path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_content_generate_auto_publish_failure():
    """Auto-publish failure marks task as FAILED with descriptive error."""
    d = _make_dispatcher()

    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.llm_model = "gpt-4"
    mock_pipeline.publish_mode = MagicMock(value="auto")
    mock_pipeline.pipeline_json = None

    mock_pipeline_bundle = MagicMock()
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=mock_pipeline)

    mock_db = MagicMock()
    mock_db.repos.generation_runs.set_status = AsyncMock()

    d._pipeline_bundle = mock_pipeline_bundle
    d._search_engine = MagicMock()
    d._db = mock_db

    mock_run = MagicMock()
    mock_run.id = 42
    mock_run.metadata = {"effective_publish_mode": "auto"}

    payload = ContentGenerateTaskPayload(pipeline_id=1)

    with (
        patch("src.services.content_generation_service.ContentGenerationService") as mock_gen,
        patch("src.services.publish_service.PublishService") as mock_pub,
    ):
        gen_instance = mock_gen.return_value
        gen_instance.generate = AsyncMock(return_value=mock_run)

        pub_instance = mock_pub.return_value
        pub_instance.publish_run = AsyncMock(side_effect=RuntimeError("Telegram send failed"))

        await d._handle_content_generate(_task(CollectionTaskType.CONTENT_GENERATE, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED
    assert "publish failed" in call[1].get("error", "")


# ---------------------------------------------------------------------------
# content_publish: full flow with approved runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_content_publish_with_approved_runs():
    """Publish approved runs successfully."""
    d = _make_dispatcher()

    mock_db = MagicMock()

    # Simulate a row from generation_runs
    _row_data = {
        "id": 10,
        "pipeline_id": 1,
        "status": "completed",
        "prompt": "test",
        "generated_text": "content",
        "metadata": None,
        "image_url": None,
        "moderation_status": "approved",
        "quality_score": None,
        "quality_issues": None,
        "variants": None,
        "selected_variant": None,
        "published_at": None,
        "created_at": "2026-01-01T00:00:00",
        "updated_at": None,
    }
    mock_row = MagicMock()
    mock_row.keys = MagicMock(return_value=list(_row_data.keys()))
    mock_row.__getitem__ = lambda self, key: _row_data[key]

    async def mock_execute(query, params=()):
        result = MagicMock()
        result.fetchall = AsyncMock(return_value=[mock_row])
        return result

    mock_db.execute = mock_execute

    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.pipeline_json = None
    mock_pipeline_bundle = MagicMock()
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=mock_pipeline)

    d._db = mock_db
    d._pipeline_bundle = mock_pipeline_bundle
    d._client_pool = MagicMock()

    payload = ContentPublishTaskPayload(pipeline_id=None)

    with (
        patch("src.services.publish_service.PublishService") as mock_pub,
        patch("src.database.repositories.generation_runs.GenerationRunsRepository") as mock_repo,
    ):
        gen_run = MagicMock()
        gen_run.id = 10
        gen_run.pipeline_id = 1
        gen_run.status = "completed"
        gen_run.moderation_status = "approved"
        gen_run.generated_text = "content"

        mock_repo._to_generation_run = staticmethod(lambda row: gen_run)

        pub_instance = mock_pub.return_value
        pub_instance.publish_run = AsyncMock(return_value=[MagicMock(success=True)])

        await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert call[1].get("messages_collected") == 1


@pytest.mark.asyncio
async def test_content_publish_run_without_pipeline_id():
    """Runs with pipeline_id=None are skipped during publish."""
    d = _make_dispatcher()

    mock_db = MagicMock()

    _row_data = {
        "id": 10,
        "pipeline_id": None,
        "status": "completed",
        "prompt": "test",
        "generated_text": "content",
        "metadata": None,
        "image_url": None,
        "moderation_status": "approved",
        "quality_score": None,
        "quality_issues": None,
        "variants": None,
        "selected_variant": None,
        "published_at": None,
        "created_at": "2026-01-01T00:00:00",
        "updated_at": None,
    }
    mock_row = MagicMock()
    mock_row.keys = MagicMock(return_value=list(_row_data.keys()))
    mock_row.__getitem__ = lambda self, key: _row_data[key]

    async def mock_execute(query, params=()):
        result = MagicMock()
        result.fetchall = AsyncMock(return_value=[mock_row])
        return result

    mock_db.execute = mock_execute

    d._db = mock_db
    d._pipeline_bundle = MagicMock()
    d._client_pool = MagicMock()

    payload = ContentPublishTaskPayload(pipeline_id=None)

    with patch("src.database.repositories.generation_runs.GenerationRunsRepository") as mock_repo:
        gen_run = MagicMock()
        gen_run.id = 10
        gen_run.pipeline_id = None

        mock_repo._to_generation_run = staticmethod(lambda row: gen_run)

        await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert call[1].get("messages_collected") == 0


@pytest.mark.asyncio
async def test_content_publish_pipeline_not_found():
    """Runs whose pipeline is not found are skipped."""
    d = _make_dispatcher()

    mock_db = MagicMock()

    _row_data = {
        "id": 10,
        "pipeline_id": 999,
        "status": "completed",
        "prompt": "test",
        "generated_text": "content",
        "metadata": None,
        "image_url": None,
        "moderation_status": "approved",
        "quality_score": None,
        "quality_issues": None,
        "variants": None,
        "selected_variant": None,
        "published_at": None,
        "created_at": "2026-01-01T00:00:00",
        "updated_at": None,
    }
    mock_row = MagicMock()
    mock_row.keys = MagicMock(return_value=list(_row_data.keys()))
    mock_row.__getitem__ = lambda self, key: _row_data[key]

    async def mock_execute(query, params=()):
        result = MagicMock()
        result.fetchall = AsyncMock(return_value=[mock_row])
        return result

    mock_db.execute = mock_execute

    mock_pipeline_bundle = MagicMock()
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=None)

    d._db = mock_db
    d._pipeline_bundle = mock_pipeline_bundle
    d._client_pool = MagicMock()

    payload = ContentPublishTaskPayload(pipeline_id=None)

    with patch("src.database.repositories.generation_runs.GenerationRunsRepository") as mock_repo:
        gen_run = MagicMock()
        gen_run.id = 10
        gen_run.pipeline_id = 999

        mock_repo._to_generation_run = staticmethod(lambda row: gen_run)

        await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert call[1].get("messages_collected") == 0


@pytest.mark.asyncio
async def test_content_publish_exception():
    """Exception during publish marks task as FAILED."""
    d = _make_dispatcher()

    mock_db = MagicMock()

    async def mock_execute(query, params=()):
        raise RuntimeError("DB down")

    mock_db.execute = mock_execute

    d._db = mock_db
    d._pipeline_bundle = MagicMock()
    d._client_pool = MagicMock()

    payload = ContentPublishTaskPayload(pipeline_id=None)

    await d._handle_content_publish(_task(CollectionTaskType.CONTENT_PUBLISH, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED


# ---------------------------------------------------------------------------
# pipeline_run: outer exception (during service setup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_run_outer_exception_during_setup():
    """Exception during service setup is caught by outer try/except."""
    d = _make_dispatcher()

    mock_pipeline_bundle = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.llm_model = "gpt-4"
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=mock_pipeline)

    mock_db = MagicMock()
    mock_db.repos.generation_runs.set_status = AsyncMock()

    d._pipeline_bundle = mock_pipeline_bundle
    d._search_engine = MagicMock()
    d._db = mock_db
    d._notifier = None

    payload = PipelineRunTaskPayload(pipeline_id=1)

    with patch(
        "src.services.content_generation_service.ContentGenerationService",
        side_effect=ImportError("missing module"),
    ):
        await d._handle_pipeline_run(_task(CollectionTaskType.PIPELINE_RUN, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_pipeline_run_generation_fails_with_run_id():
    """Generation failure with run_id set marks both run and task as failed."""
    d = _make_dispatcher()

    mock_pipeline_bundle = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.llm_model = "gpt-4"
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=mock_pipeline)

    mock_db = MagicMock()
    mock_db.repos.generation_runs.set_status = AsyncMock()

    d._pipeline_bundle = mock_pipeline_bundle
    d._search_engine = MagicMock()
    d._db = mock_db
    d._notifier = None

    payload = PipelineRunTaskPayload(pipeline_id=1)

    with patch(
        "src.services.content_generation_service.ContentGenerationService"
    ) as mock_gen:
        gen_instance = mock_gen.return_value
        gen_instance.generate = AsyncMock(side_effect=RuntimeError("LLM timeout"))

        await d._handle_pipeline_run(_task(CollectionTaskType.PIPELINE_RUN, payload=payload))

    # The task should be marked as failed
    task_call = d._tasks.update_collection_task.call_args
    assert task_call[0][1] == CollectionTaskStatus.FAILED


# ---------------------------------------------------------------------------
# run_loop: exception recovery marks running task as failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_loop_marks_running_task_failed_on_exception():
    """When dispatch raises unexpectedly, running task should be marked failed."""
    d = _make_dispatcher(poll_interval_sec=0.01)

    task = _task(CollectionTaskType.STATS_ALL, payload=StatsAllTaskPayload(channel_ids=[], next_index=0))

    call_count = [0]

    async def claim_side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return task
        return None

    d._tasks.claim_next_due_generic_task = AsyncMock(side_effect=claim_side_effect)

    # Make the handler throw
    async def bad_update(*args, **kwargs):
        raise RuntimeError("DB corruption")

    d._tasks.update_collection_task = bad_update

    # Provide a fresh task for recovery
    fresh_task = MagicMock()
    fresh_task.id = 1
    fresh_task.status = CollectionTaskStatus.RUNNING
    d._tasks.get_collection_task = AsyncMock(return_value=fresh_task)

    # Override with a working update for the recovery path
    real_update = AsyncMock()
    d._tasks.update_collection_task = real_update

    # First call to update_collection_task is from _handle_stats_all -> raises
    # But actually the exception would be in _dispatch, not update_collection_task
    # Let's make _handle_stats_all raise

    async def broken_handler(t):
        raise RuntimeError("handler crashed")

    d._handle_stats_all = broken_handler

    await d.start()
    await asyncio.sleep(0.1)
    await d.stop()

    # Task should have been marked as failed in the exception recovery path
    assert real_update.call_count >= 1
    failed_calls = [
        c for c in real_update.await_args_list
        if len(c.args) >= 2 and c.args[1] == CollectionTaskStatus.FAILED
    ]
    assert len(failed_calls) >= 1, f"Expected FAILED status update, got: {real_update.await_args_list}"


# ---------------------------------------------------------------------------
# stats_all: stop_event interruption during batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stats_all_stop_event_interrupts_batch():
    """When stop_event is set during batch processing, handler exits early."""
    d = _make_dispatcher(poll_interval_sec=0.01)

    ch = MagicMock(channel_id=42)
    d._channel_bundle.get_by_channel_id = AsyncMock(return_value=ch)

    # Set stop_event before processing starts
    d._stop_event.set()

    payload = StatsAllTaskPayload(channel_ids=[42, 43])

    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload=payload))

    # stop_event was set before processing, so collect_channel_stats should not be called
    d._collector.collect_channel_stats.assert_not_awaited()


@pytest.mark.asyncio
async def test_stats_all_successful_completion():
    """Full batch processes all channels and completes."""
    d = _make_dispatcher(poll_interval_sec=0.01)

    ch1 = MagicMock(channel_id=100)
    ch2 = MagicMock(channel_id=101)
    d._channel_bundle.get_by_channel_id = AsyncMock(side_effect=[ch1, ch2])

    payload = StatsAllTaskPayload(channel_ids=[100, 101])

    await d._handle_stats_all(_task(CollectionTaskType.STATS_ALL, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert call[1].get("messages_collected") == 2


# ---------------------------------------------------------------------------
# sq_stats: exception during record_stat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sq_stats_exception_during_stat_recording():
    """Exception during stat recording marks task as failed."""
    d = _make_dispatcher()

    sq = MagicMock(query="test query")
    sq_bundle = MagicMock()
    sq_bundle.get_by_id = AsyncMock(return_value=sq)
    sq_bundle.get_fts_daily_stats_for_query = AsyncMock(return_value=[])
    sq_bundle.record_stat = AsyncMock(side_effect=RuntimeError("DB error"))

    d._sq_bundle = sq_bundle

    payload = SqStatsTaskPayload(sq_id=1)
    await d._handle_sq_stats(_task(CollectionTaskType.SQ_STATS, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.FAILED


# ---------------------------------------------------------------------------
# content_generate: no auto-publish when effective_mode is moderated
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_content_generate_moderated_mode_no_publish():
    """Content generate with moderated mode does not auto-publish."""
    d = _make_dispatcher()

    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.llm_model = "gpt-4"
    mock_pipeline.publish_mode = MagicMock(value="moderated")
    mock_pipeline.pipeline_json = None

    mock_pipeline_bundle = MagicMock()
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=mock_pipeline)

    mock_db = MagicMock()

    d._pipeline_bundle = mock_pipeline_bundle
    d._search_engine = MagicMock()
    d._db = mock_db

    mock_run = MagicMock()
    mock_run.id = 42
    mock_run.metadata = {"effective_publish_mode": "moderated"}

    payload = ContentGenerateTaskPayload(pipeline_id=1)

    with patch("src.services.content_generation_service.ContentGenerationService") as mock_gen:
        gen_instance = mock_gen.return_value
        gen_instance.generate = AsyncMock(return_value=mock_run)

        await d._handle_content_generate(_task(CollectionTaskType.CONTENT_GENERATE, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert call[1].get("messages_collected") == 1


# ---------------------------------------------------------------------------
# HANDLED_TYPES includes translate_batch
# ---------------------------------------------------------------------------


def test_handled_types_includes_translate_batch():
    assert "translate_batch" in HANDLED_TYPES


# ---------------------------------------------------------------------------
# _handler_map caching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_map_cached():
    """_handler_map returns the same dict on subsequent calls."""
    d = _make_dispatcher()
    map1 = d._handler_map()
    map2 = d._handler_map()
    assert map1 is map2


# ---------------------------------------------------------------------------
# start with recovered tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_recovered_tasks_logged():
    """start() recovers interrupted tasks and logs the count."""
    d = _make_dispatcher()
    d._tasks.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=5)

    await d.start()
    await asyncio.sleep(0.02)
    await d.stop()

    d._tasks.requeue_running_generic_tasks_on_startup.assert_called_once()


# ---------------------------------------------------------------------------
# content_generate: pipeline not found returns completed with note
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_content_generate_pipeline_not_found():
    """When pipeline not found, task is marked COMPLETED with note."""
    d = _make_dispatcher()

    mock_pipeline_bundle = MagicMock()
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=None)

    d._pipeline_bundle = mock_pipeline_bundle
    d._search_engine = MagicMock()
    d._db = MagicMock()

    payload = ContentGenerateTaskPayload(pipeline_id=999)

    with patch("src.services.content_generation_service.ContentGenerationService"):
        # The import inside the handler should not raise
        await d._handle_content_generate(_task(CollectionTaskType.CONTENT_GENERATE, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert "not found" in call[1].get("note", "")


# ---------------------------------------------------------------------------
# pipeline_run: pipeline not found with all deps set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_run_pipeline_not_found_with_deps():
    """Pipeline not found returns COMPLETED with note even with all deps set."""
    d = _make_dispatcher()

    mock_pipeline_bundle = MagicMock()
    mock_pipeline_bundle.get_by_id = AsyncMock(return_value=None)

    d._pipeline_bundle = mock_pipeline_bundle
    d._search_engine = MagicMock()
    d._db = MagicMock()
    d._notifier = None

    payload = PipelineRunTaskPayload(pipeline_id=999)

    await d._handle_pipeline_run(_task(CollectionTaskType.PIPELINE_RUN, payload=payload))

    call = d._tasks.update_collection_task.call_args
    assert call[0][1] == CollectionTaskStatus.COMPLETED
    assert "not found" in call[1].get("note", "")
