from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType
from src.services.task_handlers import (
    ContentTaskHandler,
    PhotoTaskHandler,
    PipelineTaskHandler,
    StatsTaskHandler,
    TaskHandlerContext,
    TranslationTaskHandler,
)
from src.services.unified_dispatcher import HANDLED_TYPES, UnifiedDispatcher


def _context() -> TaskHandlerContext:
    collector = MagicMock()
    collector.is_running = False
    collector.delay_between_channels_sec = 0.0
    channel_bundle = MagicMock()
    tasks = MagicMock()
    tasks.update_collection_task = AsyncMock()
    return TaskHandlerContext(
        collector=collector,
        channel_bundle=channel_bundle,
        tasks=tasks,
        stop_event=MagicMock(),
    )


def test_handled_types_are_derived_from_task_handlers():
    expected = [
        task_type.value
        for handler_cls in (
            StatsTaskHandler,
            PhotoTaskHandler,
            PipelineTaskHandler,
            ContentTaskHandler,
            TranslationTaskHandler,
        )
        for task_type in handler_cls.task_types
    ]

    assert HANDLED_TYPES == expected


@pytest.mark.anyio
async def test_dispatcher_routes_each_handled_type_to_compatibility_shim():
    tasks = MagicMock()
    tasks.update_collection_task = AsyncMock()
    dispatcher = UnifiedDispatcher(
        collector=MagicMock(),
        channel_bundle=MagicMock(),
        tasks_repo=tasks,
    )

    called = []
    for task_type, handler_name in (
        (CollectionTaskType.STATS_ALL, "_handle_stats_all"),
        (CollectionTaskType.SQ_STATS, "_handle_sq_stats"),
        (CollectionTaskType.PHOTO_DUE, "_handle_photo_due"),
        (CollectionTaskType.PHOTO_AUTO, "_handle_photo_auto"),
        (CollectionTaskType.PIPELINE_RUN, "_handle_pipeline_run"),
        (CollectionTaskType.CONTENT_GENERATE, "_handle_content_generate"),
        (CollectionTaskType.CONTENT_PUBLISH, "_handle_content_publish"),
        (CollectionTaskType.TRANSLATE_BATCH, "_handle_translate_batch"),
    ):
        async def shim(task, task_type=task_type):
            called.append(task_type)

        setattr(dispatcher, handler_name, shim)

    for task_type in CollectionTaskType:
        if task_type.value not in HANDLED_TYPES:
            continue
        await dispatcher._dispatch(
            CollectionTask(id=1, task_type=task_type, status=CollectionTaskStatus.RUNNING)
        )

    assert called == [CollectionTaskType(value) for value in HANDLED_TYPES]


def test_task_handler_metadata_covers_only_supported_task_types():
    context = _context()
    handlers = [
        StatsTaskHandler(context),
        PhotoTaskHandler(context),
        PipelineTaskHandler(context),
        ContentTaskHandler(context),
        TranslationTaskHandler(context),
    ]

    handled = [task_type for handler in handlers for task_type in handler.task_types]

    assert handled == [CollectionTaskType(value) for value in HANDLED_TYPES]
    assert CollectionTaskType.CHANNEL_COLLECT not in handled
