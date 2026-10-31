"""Helpers shared by the core and web halves of a split test module (#1720 wave 3)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from tg_agent.database.bundles import PipelineBundle
from tg_agent.models import (
    Channel,
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPipeline,
    PipelinePublishMode,
    PipelineTarget,
    SearchResult,
)
from tg_agent.services.content_generation_service import ContentGenerationService
from tg_agent.services.draft_notification_service import DraftNotificationService
from tg_agent.services.task_handlers.base import TaskHandlerContext

GENERATED_TEXT = "FACTORY CONTENT через сквозной цикл"


def _build_real_generation_service(
    db,
    search_engine,
    notifier=None,
) -> ContentGenerationService:
    """Real ContentGenerationService over the real db; provider + notifier faked.

    The notifier is wrapped in a real DraftNotificationService so the MODERATED
    notification path is exercised as production code, not stubbed away.
    """
    provider_service = MagicMock()
    provider_service.get_provider_callable = MagicMock(return_value=_provider_callable())
    notification_service = (
        DraftNotificationService(db, notifier) if notifier is not None else None
    )
    return ContentGenerationService(
        db,
        search_engine,
        provider_service=provider_service,
        notification_service=notification_service,
    )


def _generate_task(task_id: int, pipeline_id: int) -> CollectionTask:
    return CollectionTask(
        id=task_id,
        task_type=CollectionTaskType.CONTENT_GENERATE,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )


def _make_context(db, pool) -> TaskHandlerContext:
    """A TaskHandlerContext wired to the real db, a real PipelineBundle, and the
    fake client pool. build_content_generation_service is overridden on the
    handler module so generation runs through our real-but-provider-faked service.

    Note: the draft notifier is NOT passed via the context here — the production
    handler never reads ``context.notifier`` directly, it reaches the notifier
    only through ``build_content_generation_service`` (base.py), which this test
    patches. So the notifier is injected at that patched seam (``_patch_generation``)
    rather than on the context, where it would be dead.
    """
    tasks = MagicMock()
    tasks.update_collection_task = AsyncMock()
    return TaskHandlerContext(
        collector=MagicMock(),
        channel_bundle=MagicMock(),
        tasks=tasks,
        stop_event=asyncio.Event(),
        search_engine=_make_search_engine(),
        pipeline_bundle=PipelineBundle.from_database(db),
        db=db,
        client_pool=pool,
    )


def _make_search_engine() -> MagicMock:
    """A search engine stub: the legacy RAG path awaits has_semantic_index and the
    search_* methods, so return empty results to run generation without an index."""
    search_engine = MagicMock()
    search_engine.has_semantic_index = AsyncMock(return_value=False)
    empty = SearchResult(messages=[], total=0, query="")
    search_engine.search_local = AsyncMock(return_value=empty)
    search_engine.search_hybrid = AsyncMock(return_value=empty)
    search_engine.search_semantic = AsyncMock(return_value=empty)
    return search_engine


def _patch_generation(monkeypatch, db, notifier=None) -> None:
    """Route the handler's build_content_generation_service to our real service.

    A new context is built per call so the search-engine stub matches; the
    provider + (optional) notifier are the only faked collaborators."""

    async def _builder(ctx):
        return _build_real_generation_service(db, ctx.search_engine, notifier=notifier)

    monkeypatch.setattr(
        "tg_agent.services.task_handlers.content.build_content_generation_service",
        _builder,
    )


def _provider_callable(text: str = GENERATED_TEXT):
    """A fake LLM provider callable: returns canned text, ignores the prompt."""

    async def fn(prompt=None, **kwargs):
        return text

    return fn


async def _seed_pipeline(
    db,
    *,
    publish_mode: PipelinePublishMode,
    targets: list[PipelineTarget],
    name: str = "Factory",
) -> int:
    """Persist a real pipeline + its targets so PipelineService.get and
    PublishService.list_targets read genuine rows (no list_targets mock)."""
    # generate_interval_minutes is passed explicitly: it has a pydantic Field
    # default, but mypy treats Field(...) defaults as required args, so naming it
    # keeps this file mypy-clean (no new baseline errors).
    pipeline = ContentPipeline(
        name=name,
        prompt_template="Summarize {source_messages}",
        llm_model="fake:model",
        publish_mode=publish_mode,
        generate_interval_minutes=60,
    )
    return await db.repos.content_pipelines.add(
        pipeline=pipeline,
        source_channel_ids=[1001],
        targets=targets,
    )


async def _seed_source_channel(db) -> None:
    """A source channel so the pipeline references a real row (defensive; the RAG
    path is stubbed, but keeps the fixture honest)."""
    await db.add_channel(Channel(channel_id=1001, title="Source A"))


def _target(phone: str = "+100", dialog_id: int = -1001) -> PipelineTarget:
    return PipelineTarget(
        pipeline_id=0,
        phone=phone,
        dialog_id=dialog_id,
        title="Target A",
        dialog_type="channel",
    )
