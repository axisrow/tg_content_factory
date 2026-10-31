"""End-to-end content-cycle integration test for the AI factory (issue #1038).

This is *the* through-test the epic (#1025, function 3) asks for: proof that the
content factory works as a *flow*, not a bag of separately-tested pieces. It
drives the whole MODERATED lifecycle on a real :memory: DB and the real
production services — ContentTaskHandler, ContentGenerationService,
PublishService, GenerationRunsRepository, PipelineService — stubbing only the two
genuine external edges:

  * the LLM provider (returns canned text, no network), and
  * Telegram delivery (a FakeClientPool whose FakeClient records sends instead of
    hitting MTProto).

Everything that *decides* the cycle — moderation_status transitions, the
status='completed'+'approved' publish gate (#1036), per-target delivery tracking
(#633), draft notifications (MODERATED only) — runs as real code against real
SQL, so the test fails if any seam in the flow breaks.

Covered trajectories:

  MODERATED full cycle (the headline):
    enqueue CONTENT_GENERATE
      → generate() → run completed + moderation_status='pending', published_at NULL
      → run IS visible in list_pending_moderation()
      → DraftNotificationService fired (MODERATED only)
      → approve_run (set_moderation_status 'approved')
      → enqueue CONTENT_PUBLISH → publish_run() delivers to every target
      → run published: moderation_status='published', published_at set
      → run NO LONGER pending; message actually delivered to the target

  reject branch:
    approve→reject decision rejects the draft; CONTENT_PUBLISH does NOT publish it;
    published_at stays NULL, nothing delivered.

  AUTO branch (diverges from MODERATED after #1036):
    generate(publish_mode=AUTO) → run published immediately, never 'pending',
    delivered without any human approval — a different trajectory from MODERATED.

  per-target delivery tracking:
    a 2-target pipeline where the first attempt only reaches one target stays
    'approved' (not 'published'), records the delivered target, and a retry
    reaches the second target without re-sending to the first.
"""
# Web-panel tests split out of tests/test_content_cycle_e2e.py (#1720 wave 3).


from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

# FakeClientPool is reused from the publish-service unit tests instead of being
# re-rolled here: it routes get_client_by_phone / release_client /
# resolve_dialog_entity to a FakeClient that records each send, so the real
# PublishService._publish_to_target runs end-to-end with no network.
from tests.test_publish_service import FakeClientPool
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
from tg_agent.services.task_handlers.content import ContentTaskHandler

# ---------------------------------------------------------------------------
# Fakes for the two external edges only: LLM provider and the draft notifier.
# ---------------------------------------------------------------------------


GENERATED_TEXT = "FACTORY CONTENT через сквозной цикл"


def _provider_callable(text: str = GENERATED_TEXT):
    """A fake LLM provider callable: returns canned text, ignores the prompt."""

    async def fn(prompt=None, **kwargs):
        return text

    return fn


# ---------------------------------------------------------------------------
# Real services wired to the real :memory: db; only provider + notifier faked.
# ---------------------------------------------------------------------------


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


def _generate_task(task_id: int, pipeline_id: int) -> CollectionTask:
    return CollectionTask(
        id=task_id,
        task_type=CollectionTaskType.CONTENT_GENERATE,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )


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


# ===========================================================================
# 5b. MODERATED cycle through the REAL production approve + publish entrypoints
# ===========================================================================


@pytest.mark.anyio
async def test_moderated_cycle_via_real_web_approve_and_publish_command(
    db, real_pool_harness_factory, tmp_path
):
    """The headline cycle again, but approval and publish are driven through the
    *production* moderation entrypoints rather than a direct repository write:

      * approve via the real web route POST /moderation/{run_id}/approve
        (exercises its 404 guard, the 'approved' status literal, and the redirect);
      * publish via the real TelegramCommandDispatcher._handle_moderation_publish_run
        — the exact command the web "Publish" button enqueues (moderation.publish_run).

    This closes the review gap that a `set_moderation_status(...,'approved')` setup
    shortcut leaves open: a regression in the approve route (wrong status, missing
    404 check) or in the publish command handler would now fail the test, not slip
    through. Only the LLM provider and Telegram delivery stay faked.
    """
    from webui_helpers import build_web_app, make_auth_client

    from tests.helpers import make_test_config
    from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher

    # A real web app over the SAME :memory: db the harness uses (CLAUDE.md: the app
    # and harness must share one Database instance, so pass `db` through).
    harness = real_pool_harness_factory()
    config = make_test_config(tmp_path)
    app, _ = await build_web_app(config, harness, db=db)

    await _seed_source_channel(db)
    pool = FakeClientPool(should_succeed=True)
    pipeline_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.MODERATED, targets=[_target()]
    )

    # --- generate a real MODERATED draft (handler path, provider faked) --------
    import pytest as _pytest

    mp = _pytest.MonkeyPatch()
    try:
        _patch_generation(mp, db)
        handler = ContentTaskHandler(_make_context(db, pool))
        gen_task_id = await db.repos.tasks.create_generic_task(
            CollectionTaskType.CONTENT_GENERATE,
            title="gen",
            payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
        )
        await handler.handle_content_generate(_generate_task(gen_task_id, pipeline_id))
    finally:
        mp.undo()

    run = (await db.repos.generation_runs.list_by_pipeline(pipeline_id))[0]
    assert run.moderation_status == "pending"

    async with make_auth_client(app) as client:
        # A non-existent run id is rejected by the route's own 404 guard …
        missing = await client.post("/moderation/999999/approve", follow_redirects=False)
        assert missing.status_code == 303
        assert "run_not_found" in missing.headers["location"]

        # … and approving the real run flips it to 'approved' through the route.
        approve = await client.post(
            f"/moderation/{run.id}/approve", follow_redirects=False
        )
        assert approve.status_code == 303
        assert "run_approved" in approve.headers["location"]

    approved = await db.repos.generation_runs.get(run.id)
    assert approved is not None and approved.moderation_status == "approved"

    # --- publish via the REAL command handler the web button enqueues ----------
    dispatcher = TelegramCommandDispatcher(db, pool)
    result = await dispatcher._handle_moderation_publish_run(
        {"run_id": run.id, "pipeline_id": pipeline_id}
    )
    assert result == {"run_id": run.id, "published": 1}

    published = await db.repos.generation_runs.get(run.id)
    assert published is not None
    assert published.moderation_status == "published"
    assert published.published_at is not None
    # delivered exactly once through the production publish command
    client_obj = pool._clients.get("+100")
    assert client_obj is not None and len(client_obj.sent_messages) == 1

    await app.state.collection_queue.shutdown()
