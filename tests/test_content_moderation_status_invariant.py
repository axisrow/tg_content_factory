"""End-to-end moderation_status invariants for the content cycle (issue #1036).

These tests pin the status flow against a *real* generation_runs repository
(:memory: DB via the ``db`` fixture) and the *real* ContentGenerationService /
ContentTaskHandler / PublishService code paths. Only the two genuine external
edges are stubbed: the LLM provider (returns canned text) and the Telegram
delivery (``PublishService._publish_to_target`` returns success without a
network call). Everything that decides moderation_status runs as production code
so the AUTO/MODERATED de-synchronisations are exercised on real SQL.

Invariants (issue #1036):
- AUTO: after handle_content_generate() the run is published and is NOT visible
  in list_pending_moderation(); it never lingers as 'pending'.
- MODERATED: the run stays 'pending' with no published_at and IS visible in the
  moderation queue until a human approves it.
- DAG publish: a graph pipeline run in AUTO mode reflects the publish in the
  generation_run (published_at + moderation_status='published'), not stranded
  'pending'.
- No run is ever both 'pending' and carries a published_at.
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
    ContentPipeline,
    PipelineGraph,
    PipelinePublishMode,
    PipelineTarget,
    SearchResult,
)
from src.services.content_generation_service import ContentGenerationService
from src.services.publish_service import PublishResult, PublishService
from src.services.task_handlers.base import TaskHandlerContext
from src.services.task_handlers.content import ContentTaskHandler


def _make_context(db) -> TaskHandlerContext:
    """Assemble a TaskHandlerContext wired to the real :memory: db.

    Only the fields the CONTENT_GENERATE path touches are populated; the rest
    use MagicMock so the dataclass is satisfied without standing up a Collector.
    """
    tasks = MagicMock()
    tasks.update_collection_task = AsyncMock()
    # A real RAG generate() awaits search_engine.has_semantic_index()/search_*;
    # stub them so the legacy (non-graph) path runs without a live index.
    search_engine = MagicMock()
    search_engine.has_semantic_index = AsyncMock(return_value=False)
    empty = SearchResult(messages=[], total=0, query="")
    search_engine.search_local = AsyncMock(return_value=empty)
    search_engine.search_hybrid = AsyncMock(return_value=empty)
    search_engine.search_semantic = AsyncMock(return_value=empty)
    return TaskHandlerContext(
        collector=MagicMock(),
        channel_bundle=MagicMock(),
        tasks=tasks,
        stop_event=asyncio.Event(),
        search_engine=search_engine,
        pipeline_bundle=MagicMock(),
        db=db,
        client_pool=MagicMock(),
    )


def _task(pipeline_id: int = 1) -> CollectionTask:
    return CollectionTask(
        id=1,
        task_type=CollectionTaskType.CONTENT_GENERATE,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )


def _pipeline(
    publish_mode: PipelinePublishMode,
    *,
    pipeline_id: int = 1,
    pipeline_json: PipelineGraph | None = None,
) -> ContentPipeline:
    return ContentPipeline(
        id=pipeline_id,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        publish_mode=publish_mode,
        pipeline_json=pipeline_json,
    )


def _provider(text: str = "generated content"):
    async def fn(prompt=None, **kwargs):
        return text

    return fn


def _build_real_service(ctx) -> ContentGenerationService:
    """Real ContentGenerationService over the real db, provider stubbed."""
    provider_service = MagicMock()
    provider_service.get_provider_callable = MagicMock(return_value=_provider())
    return ContentGenerationService(
        ctx.db,
        ctx.search_engine,
        provider_service=provider_service,
    )


async def _seed_targets(db, pipeline_id: int) -> None:
    """Insert one publish target so PublishService.publish_run can deliver."""
    db.repos.content_pipelines.list_targets = AsyncMock(
        return_value=[PipelineTarget(id=1, pipeline_id=pipeline_id, phone="+100", dialog_id=-1001)]
    )


def _patch_handler_collaborators(db, pipeline):
    """Patch only the real boundaries: pipeline lookup, service builder, and the
    single Telegram delivery call. Generation + publish bookkeeping stay live."""
    async def fake_deliver(self, run, target):
        return PublishResult(success=True, message_id=1, phone=target.phone, dialog_id=target.dialog_id)

    return (
        patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=pipeline)),
        patch(
            "src.services.task_handlers.content.build_content_generation_service",
            AsyncMock(side_effect=lambda c: _build_real_service(c)),
        ),
        patch.object(PublishService, "_publish_to_target", fake_deliver),
    )


@pytest.mark.anyio
async def test_auto_run_not_in_pending_moderation_queue(db):
    """AUTO: after handle_content_generate the run is published and absent from
    the moderation queue. A delivered AUTO run that still shows 'pending' would
    falsely surface in the moderator's list (issue #1036, gap A)."""
    pipeline = _pipeline(PipelinePublishMode.AUTO)
    await _seed_targets(db, pipeline.id)
    handler = ContentTaskHandler(_make_context(db))

    p_get, p_build, p_deliver = _patch_handler_collaborators(db, pipeline)
    with p_get, p_build, p_deliver:
        await handler.handle_content_generate(_task())

    pending = await db.repos.generation_runs.list_pending_moderation(pipeline.id)
    assert pending == [], "AUTO run must not appear in the moderation queue"

    rows = await db.repos.generation_runs.list_by_pipeline(pipeline.id)
    assert len(rows) == 1
    run = rows[0]
    assert run.moderation_status == "published"
    assert run.published_at is not None
    # The core invariant: no run is simultaneously pending and published.
    assert not (run.moderation_status == "pending" and run.published_at is not None)


@pytest.mark.anyio
async def test_auto_run_never_pending_even_before_publish(db):
    """AUTO: the run skips 'pending' the instant generation completes — proven by
    blocking delivery. Even with publish failing, the run must read 'approved'
    (publish-eligible), never 'pending' (issue #1036, gap A stranding case)."""
    pipeline = _pipeline(PipelinePublishMode.AUTO)
    await _seed_targets(db, pipeline.id)
    handler = ContentTaskHandler(_make_context(db))

    async def failing_deliver(self, run, target):
        return PublishResult(success=False, error="no client")

    with (
        patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=pipeline)),
        patch(
            "src.services.task_handlers.content.build_content_generation_service",
            AsyncMock(side_effect=lambda c: _build_real_service(c)),
        ),
        patch.object(PublishService, "_publish_to_target", failing_deliver),
    ):
        await handler.handle_content_generate(_task())

    rows = await db.repos.generation_runs.list_by_pipeline(pipeline.id)
    run = rows[0]
    # Delivery failed, so it is not yet 'published' — but the AUTO run must NOT
    # be stranded 'pending' (the core gap-A bug: a 'pending' AUTO run that never
    # gets a moderation decision). It rests at 'approved' (publish-eligible), so
    # a retry/manual re-publish can still deliver it. The forbidden state is
    # 'pending' on an AUTO run, never reachable now.
    assert run.moderation_status != "pending"
    assert run.moderation_status == "approved"
    # And it never carries a published_at while not 'published'.
    assert not (run.moderation_status == "pending" and run.published_at is not None)


@pytest.mark.anyio
async def test_moderated_run_stays_pending_in_queue(db):
    """MODERATED: the run stays 'pending' with no published_at and is visible in
    the moderation queue until approved."""
    pipeline = _pipeline(PipelinePublishMode.MODERATED)
    await _seed_targets(db, pipeline.id)
    handler = ContentTaskHandler(_make_context(db))

    deliver_calls = []

    async def tracking_deliver(self, run, target):
        deliver_calls.append(run.id)
        return PublishResult(success=True, message_id=1)

    with (
        patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=pipeline)),
        patch(
            "src.services.task_handlers.content.build_content_generation_service",
            AsyncMock(side_effect=lambda c: _build_real_service(c)),
        ),
        patch.object(PublishService, "_publish_to_target", tracking_deliver),
    ):
        await handler.handle_content_generate(_task())

    # MODERATED must NOT auto-publish.
    assert deliver_calls == []

    rows = await db.repos.generation_runs.list_by_pipeline(pipeline.id)
    assert len(rows) == 1
    run = rows[0]
    assert run.moderation_status == "pending"
    assert run.published_at is None

    pending = await db.repos.generation_runs.list_pending_moderation(pipeline.id)
    assert run.id in [r.id for r in pending], "MODERATED run must be in the moderation queue"


@pytest.mark.anyio
async def test_dag_auto_publish_updates_run(db):
    """DAG publish: a graph pipeline run in AUTO mode reflects the publish in the
    generation_run (published_at + moderation_status='published'), not stranded
    'pending' (issue #1036, gap B — regression guard).

    The graph executor only sets publish_mode in the context; actual delivery
    goes through PublishService.publish_run which updates the run. This guards
    against a regression where the DAG path bypasses that bookkeeping."""
    graph = PipelineGraph(nodes=[], edges=[])
    pipeline = _pipeline(PipelinePublishMode.AUTO, pipeline_json=graph)
    await _seed_targets(db, pipeline.id)
    handler = ContentTaskHandler(_make_context(db))

    delivered = []

    async def fake_deliver(self, run, target):
        delivered.append(run.id)
        return PublishResult(success=True, message_id=7, phone=target.phone, dialog_id=target.dialog_id)

    # The graph executor would normally need real nodes; stub it to return text
    # with an explicit AUTO effective mode (mirrors a publish node in AUTO).
    with (
        patch("src.services.pipeline_service.PipelineService.get", AsyncMock(return_value=pipeline)),
        patch(
            "src.services.task_handlers.content.build_content_generation_service",
            AsyncMock(side_effect=lambda c: _build_real_service(c)),
        ),
        patch(
            "src.services.pipeline_executor.PipelineExecutor.execute",
            AsyncMock(return_value={
                "generated_text": "graph content",
                "publish_mode": PipelinePublishMode.AUTO.value,
            }),
        ),
        patch.object(PublishService, "_publish_to_target", fake_deliver),
    ):
        await handler.handle_content_generate(_task())

    assert delivered, "DAG AUTO run must be routed through PublishService"
    rows = await db.repos.generation_runs.list_by_pipeline(pipeline.id)
    run = rows[0]
    assert run.published_at is not None
    assert run.moderation_status == "published"
    pending = await db.repos.generation_runs.list_pending_moderation(pipeline.id)
    assert run.id not in [r.id for r in pending]


@pytest.mark.anyio
async def test_no_run_is_pending_with_published_at(db):
    """Repo-level invariant guard: the (pending + published_at) combination is
    impossible. set_published_at moves moderation_status to 'published'
    atomically, so a run that has a published_at can never read back 'pending'."""
    repo = db.repos.generation_runs
    run_id = await repo.create_run(1, "prompt")
    await repo.save_result(run_id, "text", {})
    await repo.set_published_at(run_id)

    run = await repo.get(run_id)
    assert run is not None
    assert run.published_at is not None
    assert run.moderation_status == "published"
    assert run.moderation_status != "pending"

    pending = await repo.list_pending_moderation(1)
    assert run_id not in [r.id for r in pending]
