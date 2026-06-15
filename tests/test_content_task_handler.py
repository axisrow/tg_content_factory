"""Tests for ContentTaskHandler delivery-integrity (audit #835/1, #838/5).

AUTO-publish (handle_content_generate) and CONTENT_PUBLISH (handle_content_publish)
must surface delivery failures as FAILED tasks instead of silently reporting
COMPLETED. publish_run() never raises on real delivery errors — it returns
PublishResult(success=False) — so callers must inspect the result list.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
    PipelinePublishMode,
)
from src.services.publish_service import PublishResult
from src.services.task_handlers import ContentTaskHandler, TaskHandlerContext


def _context() -> TaskHandlerContext:
    tasks = MagicMock()
    tasks.update_collection_task = AsyncMock()
    return TaskHandlerContext(
        collector=MagicMock(),
        channel_bundle=MagicMock(),
        tasks=tasks,
        stop_event=MagicMock(),
        db=MagicMock(),
        search_engine=MagicMock(),
        pipeline_bundle=MagicMock(),
        client_pool=MagicMock(),
    )


def _last_status(tasks_mock: MagicMock) -> CollectionTaskStatus:
    return tasks_mock.update_collection_task.await_args.args[1]


def _make_pipeline(mode: PipelinePublishMode = PipelinePublishMode.AUTO) -> MagicMock:
    pipeline = MagicMock()
    pipeline.id = 1
    pipeline.llm_model = "test:model"
    pipeline.publish_mode = mode
    return pipeline


def _make_run() -> MagicMock:
    run = MagicMock()
    run.id = 5
    run.pipeline_id = 1
    run.metadata = {}
    return run


# ----------------------------------------------------------------------------
# 835#1 — AUTO-publish must FAIL the task when delivery fails
# ----------------------------------------------------------------------------


async def test_auto_publish_delivery_failure_marks_task_failed():
    ctx = _context()
    handler = ContentTaskHandler(ctx)
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.CONTENT_GENERATE,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentGenerateTaskPayload(pipeline_id=1),
    )

    pipeline = _make_pipeline(PipelinePublishMode.AUTO)
    run = _make_run()
    gen = MagicMock()
    gen.generate = AsyncMock(return_value=run)

    with (
        patch("src.services.pipeline_service.PipelineService") as pipe_cls,
        patch("src.services.publish_service.PublishService") as pub_cls,
        patch(
            "src.services.task_handlers.content.build_content_generation_service",
            AsyncMock(return_value=gen),
        ),
    ):
        pipe_cls.return_value.get = AsyncMock(return_value=pipeline)
        pub_cls.return_value.publish_run = AsyncMock(
            return_value=[PublishResult(success=False, error="No client for phone")]
        )

        await handler.handle_content_generate(task)

    assert _last_status(ctx.tasks) == CollectionTaskStatus.FAILED


async def test_auto_publish_success_marks_task_completed():
    ctx = _context()
    handler = ContentTaskHandler(ctx)
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.CONTENT_GENERATE,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentGenerateTaskPayload(pipeline_id=1),
    )

    pipeline = _make_pipeline(PipelinePublishMode.AUTO)
    run = _make_run()
    gen = MagicMock()
    gen.generate = AsyncMock(return_value=run)

    with (
        patch("src.services.pipeline_service.PipelineService") as pipe_cls,
        patch("src.services.publish_service.PublishService") as pub_cls,
        patch(
            "src.services.task_handlers.content.build_content_generation_service",
            AsyncMock(return_value=gen),
        ),
    ):
        pipe_cls.return_value.get = AsyncMock(return_value=pipeline)
        pub_cls.return_value.publish_run = AsyncMock(
            return_value=[PublishResult(success=True, message_id=10)]
        )

        await handler.handle_content_generate(task)

    assert _last_status(ctx.tasks) == CollectionTaskStatus.COMPLETED


# ----------------------------------------------------------------------------
# 838#5 — CONTENT_PUBLISH must not count partial delivery as success
# ----------------------------------------------------------------------------


def _publish_ctx_with_run(run: MagicMock) -> TaskHandlerContext:
    ctx = _context()
    cursor = MagicMock()
    cursor.fetchall = AsyncMock(return_value=[run])
    ctx.db.execute = AsyncMock(return_value=cursor)
    return ctx


async def test_content_publish_partial_delivery_marks_task_failed():
    run = _make_run()
    ctx = _publish_ctx_with_run(run)
    handler = ContentTaskHandler(ctx)
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.CONTENT_PUBLISH,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentPublishTaskPayload(pipeline_id=1),
    )

    with (
        patch("src.services.pipeline_service.PipelineService") as pipe_cls,
        patch("src.services.publish_service.PublishService") as pub_cls,
        patch(
            "src.database.repositories.generation_runs.GenerationRunsRepository._to_generation_run",
            staticmethod(lambda row: row),
        ),
    ):
        pipe_cls.return_value.get = AsyncMock(return_value=_make_pipeline())
        pub_cls.return_value.publish_run = AsyncMock(
            return_value=[
                PublishResult(success=True, message_id=1),
                PublishResult(success=False, error="flood"),
            ]
        )

        await handler.handle_content_publish(task)

    assert _last_status(ctx.tasks) == CollectionTaskStatus.FAILED


async def test_content_publish_full_success_marks_completed():
    run = _make_run()
    ctx = _publish_ctx_with_run(run)
    handler = ContentTaskHandler(ctx)
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.CONTENT_PUBLISH,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentPublishTaskPayload(pipeline_id=1),
    )

    with (
        patch("src.services.pipeline_service.PipelineService") as pipe_cls,
        patch("src.services.publish_service.PublishService") as pub_cls,
        patch(
            "src.database.repositories.generation_runs.GenerationRunsRepository._to_generation_run",
            staticmethod(lambda row: row),
        ),
    ):
        pipe_cls.return_value.get = AsyncMock(return_value=_make_pipeline())
        pub_cls.return_value.publish_run = AsyncMock(
            return_value=[PublishResult(success=True, message_id=1)]
        )

        await handler.handle_content_publish(task)

    assert _last_status(ctx.tasks) == CollectionTaskStatus.COMPLETED
    assert ctx.tasks.update_collection_task.await_args.kwargs.get("messages_collected") == 1
