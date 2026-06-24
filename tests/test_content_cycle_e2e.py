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

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database.bundles import PipelineBundle
from src.models import (
    Channel,
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPipeline,
    ContentPublishTaskPayload,
    GenerationRun,
    PipelinePublishMode,
    PipelineTarget,
    SearchResult,
)
from src.services.content_generation_service import ContentGenerationService
from src.services.draft_notification_service import DraftNotificationService
from src.services.publish_service import PublishService
from src.services.task_handlers.base import TaskHandlerContext
from src.services.task_handlers.content import ContentTaskHandler

# FakeClientPool is reused from the publish-service unit tests instead of being
# re-rolled here: it routes get_client_by_phone / release_client /
# resolve_dialog_entity to a FakeClient that records each send, so the real
# PublishService._publish_to_target runs end-to-end with no network.
from tests.test_publish_service import FakeClientPool

# ---------------------------------------------------------------------------
# Fakes for the two external edges only: LLM provider and the draft notifier.
# ---------------------------------------------------------------------------


GENERATED_TEXT = "FACTORY CONTENT через сквозной цикл"


def _provider_callable(text: str = GENERATED_TEXT):
    """A fake LLM provider callable: returns canned text, ignores the prompt."""

    async def fn(prompt=None, **kwargs):
        return text

    return fn


class _RecordingNotifier:
    """Stand-in Notifier: records every message instead of pushing to Telegram.

    DraftNotificationService.notify_new_draft awaits ``notifier.notify(text)`` and
    treats a truthy return as success, so this is the whole surface it touches.
    """

    def __init__(self) -> None:
        self.messages: list[str] = []

    async def notify(self, text: str) -> bool:
        self.messages.append(text)
        return True


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


def _make_context(db, pool, notifier=None) -> TaskHandlerContext:
    """A TaskHandlerContext wired to the real db, a real PipelineBundle, and the
    fake client pool. build_content_generation_service is overridden on the
    handler module so generation runs through our real-but-provider-faked service.
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
        notifier=notifier,
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


def _publish_task(task_id: int, pipeline_id: int | None) -> CollectionTask:
    return CollectionTask(
        id=task_id,
        task_type=CollectionTaskType.CONTENT_PUBLISH,
        status=CollectionTaskStatus.RUNNING,
        payload=ContentPublishTaskPayload(pipeline_id=pipeline_id),
    )


def _patch_generation(monkeypatch, db, notifier=None) -> None:
    """Route the handler's build_content_generation_service to our real service.

    A new context is built per call so the search-engine stub matches; the
    provider + (optional) notifier are the only faked collaborators."""

    async def _builder(ctx):
        return _build_real_generation_service(db, ctx.search_engine, notifier=notifier)

    monkeypatch.setattr(
        "src.services.task_handlers.content.build_content_generation_service",
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
# 1. MODERATED full cycle: generate → pending → approve → publish → published
# ===========================================================================


@pytest.mark.anyio
async def test_moderated_full_cycle_generate_approve_publish(db, monkeypatch):
    await _seed_source_channel(db)
    pool = FakeClientPool(should_succeed=True)
    notifier = _RecordingNotifier()
    pipeline_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.MODERATED, targets=[_target()]
    )
    _patch_generation(monkeypatch, db, notifier=notifier)
    handler = ContentTaskHandler(_make_context(db, pool, notifier=notifier))

    # --- Step 1: enqueue + run CONTENT_GENERATE -----------------------------
    gen_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_GENERATE,
        title="gen",
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_generate(_generate_task(gen_task_id, pipeline_id))

    runs = await db.repos.generation_runs.list_by_pipeline(pipeline_id)
    assert len(runs) == 1, "CONTENT_GENERATE must create exactly one run"
    run = runs[0]

    # --- Step 2: run is a completed MODERATED draft, not yet published ------
    assert run.status == "completed"
    assert run.moderation_status == "pending"
    assert run.published_at is None
    assert GENERATED_TEXT in (run.generated_text or "")

    # --- Step 3: the draft is visible in the moderation queue --------------
    pending = await db.repos.generation_runs.list_pending_moderation(pipeline_id)
    assert run.id in [r.id for r in pending], "MODERATED draft must show in the queue"

    # --- Step 4: DraftNotificationService fired (MODERATED only) ------------
    assert notifier.messages, "MODERATED generation must notify a new draft"
    assert f"#{run.id}" in notifier.messages[0]

    # --- Step 5: a CONTENT_PUBLISH before approval delivers nothing --------
    early_publish_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_PUBLISH,
        title="pub-early",
        payload=ContentPublishTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_publish(_publish_task(early_publish_id, pipeline_id))
    not_yet = await db.repos.generation_runs.get(run.id)
    assert not_yet is not None and not_yet.published_at is None, (
        "an un-approved MODERATED run must not be publishable"
    )

    # --- Step 6: approve (the real moderation action) ----------------------
    await db.repos.generation_runs.set_moderation_status(run.id, "approved")
    approved = await db.repos.generation_runs.get(run.id)
    assert approved is not None and approved.moderation_status == "approved"

    # --- Step 7: enqueue + run CONTENT_PUBLISH -----------------------------
    publish_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_PUBLISH,
        title="pub",
        payload=ContentPublishTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_publish(_publish_task(publish_task_id, pipeline_id))

    # --- Step 8: the run is finally published and was delivered ------------
    published = await db.repos.generation_runs.get(run.id)
    assert published is not None
    assert published.moderation_status == "published"
    assert published.published_at is not None
    assert published.status == "completed"

    # delivered to the real target through the fake client (records the send)
    client = pool._clients.get("+100")
    assert client is not None, "publish must have acquired the target's client"
    assert len(client.sent_messages) == 1
    assert client.sent_messages[0]["text"] == published.generated_text

    # and it is gone from the moderation queue (no longer pending/approved)
    pending_after = await db.repos.generation_runs.list_pending_moderation(pipeline_id)
    assert run.id not in [r.id for r in pending_after]

    # the publish task itself completed
    last_status = handler._context.tasks.update_collection_task.await_args.args[1]
    assert last_status == CollectionTaskStatus.COMPLETED


# ===========================================================================
# 2. reject branch: a rejected draft is never published
# ===========================================================================


@pytest.mark.anyio
async def test_moderated_reject_branch_never_publishes(db, monkeypatch):
    await _seed_source_channel(db)
    pool = FakeClientPool(should_succeed=True)
    pipeline_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.MODERATED, targets=[_target()]
    )
    _patch_generation(monkeypatch, db)
    handler = ContentTaskHandler(_make_context(db, pool))

    gen_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_GENERATE,
        title="gen",
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_generate(_generate_task(gen_task_id, pipeline_id))
    run = (await db.repos.generation_runs.list_by_pipeline(pipeline_id))[0]
    assert run.moderation_status == "pending"

    # reject the draft (the real moderation action)
    await db.repos.generation_runs.set_moderation_status(run.id, "rejected")

    # rejected drafts drop out of the moderation queue immediately
    pending = await db.repos.generation_runs.list_pending_moderation(pipeline_id)
    assert run.id not in [r.id for r in pending]

    # a publish pass must NOT deliver a rejected run
    publish_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_PUBLISH,
        title="pub",
        payload=ContentPublishTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_publish(_publish_task(publish_task_id, pipeline_id))

    rejected = await db.repos.generation_runs.get(run.id)
    assert rejected is not None
    assert rejected.moderation_status == "rejected"
    assert rejected.published_at is None
    assert pool._clients == {}, "nothing must be delivered for a rejected run"

    # The handler's own selection must exclude the rejected run — proven by the
    # task completing with "no approved runs" rather than attempting a publish
    # that some downstream guard then blocks. This pins the CONTENT_PUBLISH SQL
    # filter (moderation_status='approved'), not just PublishService's gate.
    publish_call = handler._context.tasks.update_collection_task.await_args
    assert publish_call.args[1] == CollectionTaskStatus.COMPLETED
    assert "No approved runs" in (publish_call.kwargs.get("note") or "")


# ===========================================================================
# 3. AUTO branch: published immediately, never pending (diverges post-#1036)
# ===========================================================================


@pytest.mark.anyio
async def test_auto_branch_publishes_without_moderation(db, monkeypatch):
    await _seed_source_channel(db)
    pool = FakeClientPool(should_succeed=True)
    notifier = _RecordingNotifier()
    pipeline_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.AUTO, targets=[_target()]
    )
    _patch_generation(monkeypatch, db, notifier=notifier)
    handler = ContentTaskHandler(_make_context(db, pool, notifier=notifier))

    gen_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_GENERATE,
        title="gen",
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )
    # AUTO auto-publishes inside handle_content_generate — no approve, no
    # separate CONTENT_PUBLISH task.
    await handler.handle_content_generate(_generate_task(gen_task_id, pipeline_id))

    run = (await db.repos.generation_runs.list_by_pipeline(pipeline_id))[0]
    # The defining divergence from MODERATED: AUTO never rests at 'pending'.
    assert run.moderation_status == "published"
    assert run.published_at is not None
    assert run.status == "completed"

    # never surfaced in the moderation queue …
    pending = await db.repos.generation_runs.list_pending_moderation(pipeline_id)
    assert run.id not in [r.id for r in pending]
    # … and AUTO does not raise a moderation draft notification.
    assert notifier.messages == []

    # delivered to the target without any human action
    client = pool._clients.get("+100")
    assert client is not None
    assert len(client.sent_messages) == 1

    last_status = handler._context.tasks.update_collection_task.await_args.args[1]
    assert last_status == CollectionTaskStatus.COMPLETED


@pytest.mark.anyio
async def test_auto_run_skips_pending_even_when_delivery_fails(db, monkeypatch):
    """AUTO content must reach 'approved' the instant generation completes, BEFORE
    any delivery — so it never strands at 'pending' in the moderation queue
    (issue #1036 gap-A). Proven by failing delivery: with no usable client the run
    is not 'published', yet it must read 'approved' (publish-eligible for a retry),
    never 'pending'. This is the assertion that pins the AUTO→approved transition
    itself, independent of whether the subsequent publish succeeds.
    """
    await _seed_source_channel(db)
    # A pool that never yields a client → delivery fails for every target.
    pool = FakeClientPool(should_succeed=False)
    pipeline_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.AUTO, targets=[_target()]
    )
    _patch_generation(monkeypatch, db)
    handler = ContentTaskHandler(_make_context(db, pool))

    gen_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_GENERATE,
        title="gen",
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_generate(_generate_task(gen_task_id, pipeline_id))

    run = (await db.repos.generation_runs.list_by_pipeline(pipeline_id))[0]
    # Delivery failed, so it is not 'published' …
    assert run.moderation_status != "published"
    assert run.published_at is None
    # … but the AUTO run must NOT be stranded 'pending'; it rests 'approved'.
    assert run.moderation_status == "approved", (
        "AUTO must skip 'pending' even when publish fails (issue #1036 gap-A)"
    )
    # the forbidden combination is never reachable
    assert not (run.moderation_status == "pending" and run.published_at is not None)

    # the generate task surfaces the delivery failure rather than reporting success
    last_status = handler._context.tasks.update_collection_task.await_args.args[1]
    assert last_status == CollectionTaskStatus.FAILED


# ===========================================================================
# 4. per-target delivery tracking across a partial-failure retry (#633)
# ===========================================================================


@pytest.mark.anyio
async def test_per_target_delivery_tracking_on_partial_failure(db, monkeypatch):
    """A 2-target approved run whose second target is unreachable on the first
    pass stays 'approved' (not 'published'), records the delivered target, and a
    retry reaches the second target without re-sending to the first."""
    await _seed_source_channel(db)
    pipeline_id = await _seed_pipeline(
        db,
        publish_mode=PipelinePublishMode.MODERATED,
        targets=[_target("+100", -1001), _target("+200", -2002)],
    )
    pipeline = await db.repos.content_pipelines.get_by_id(pipeline_id)
    assert pipeline is not None

    # Generate a real completed run, then approve it.
    _patch_generation(monkeypatch, db)
    handler = ContentTaskHandler(_make_context(db, FakeClientPool()))
    gen_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.CONTENT_GENERATE,
        title="gen",
        payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
    )
    await handler.handle_content_generate(_generate_task(gen_task_id, pipeline_id))
    run = (await db.repos.generation_runs.list_by_pipeline(pipeline_id))[0]
    await db.repos.generation_runs.set_moderation_status(run.id, "approved")

    # --- Attempt 1: second target's phone has no client --------------------
    failing_pool = FakeClientPool(should_succeed=True, fail_phones={"+200"})
    run_for_publish = await db.repos.generation_runs.get(run.id)
    assert run_for_publish is not None
    results = await PublishService(db, failing_pool).publish_run(run_for_publish, pipeline)
    assert [r.success for r in results] == [True, False]

    after_partial = await db.repos.generation_runs.get(run.id)
    assert after_partial is not None
    # Partial delivery: NOT published, but the reached target is remembered.
    assert after_partial.moderation_status == "approved"
    assert after_partial.published_at is None
    assert (after_partial.metadata or {}).get("published_targets") == ["+100:-1001"]
    # the first target was actually sent to once
    assert len(failing_pool._clients["+100"].sent_messages) == 1

    # --- Attempt 2: retry with a fully-healthy pool ------------------------
    healthy_pool = FakeClientPool(should_succeed=True)
    run_retry = await db.repos.generation_runs.get(run.id)
    assert run_retry is not None
    results2 = await PublishService(db, healthy_pool).publish_run(run_retry, pipeline)
    assert all(r.success for r in results2)

    final = await db.repos.generation_runs.get(run.id)
    assert final is not None
    assert final.moderation_status == "published"
    assert final.published_at is not None
    assert sorted((final.metadata or {}).get("published_targets") or []) == [
        "+100:-1001",
        "+200:-2002",
    ]
    # The retry must NOT re-send to the already-delivered first target …
    assert "+100" not in healthy_pool._clients
    # … and must reach the previously-failed second target exactly once.
    assert len(healthy_pool._clients["+200"].sent_messages) == 1


# ===========================================================================
# 5. publish gate: a failed (status != completed) run is never delivered
# ===========================================================================


@pytest.mark.anyio
async def test_failed_run_is_not_publishable_even_if_approved(db):
    """fail-closed: a run that carries generated_text + moderation_status='approved'
    but ended at status='failed' must never be delivered (issue #1036 gate)."""
    await _seed_source_channel(db)
    pipeline_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.MODERATED, targets=[_target()]
    )
    pipeline = await db.repos.content_pipelines.get_by_id(pipeline_id)
    assert pipeline is not None

    # Craft a run that looks approved but is execution-failed.
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt")
    await db.repos.generation_runs.save_result(run_id, "leftover text", {})
    await db.repos.generation_runs.set_status(run_id, "failed")
    await db.repos.generation_runs.set_moderation_status(run_id, "approved")

    run = await db.repos.generation_runs.get(run_id)
    assert run is not None and run.status == "failed"

    pool = FakeClientPool(should_succeed=True)
    results = await PublishService(db, pool).publish_run(run, pipeline)

    assert results and results[0].success is False
    assert pool._clients == {}, "a failed run must never reach Telegram"
    after = await db.repos.generation_runs.get(run_id)
    assert after is not None and after.published_at is None


# ===========================================================================
# 6. AUTO vs MODERATED give *different* trajectories (the epic claim)
# ===========================================================================


@pytest.mark.anyio
async def test_auto_and_moderated_trajectories_diverge(db, monkeypatch):
    """Same generation, two publish modes → two distinct end states. This is the
    epic's load-bearing assertion: the factory routes content differently by mode
    (after #1036), not 'both end up published' by accident."""
    await _seed_source_channel(db)

    auto_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.AUTO, targets=[_target()], name="Auto"
    )
    mod_id = await _seed_pipeline(
        db, publish_mode=PipelinePublishMode.MODERATED, targets=[_target()], name="Mod"
    )
    _patch_generation(monkeypatch, db)

    for pid in (auto_id, mod_id):
        pool = FakeClientPool(should_succeed=True)
        handler = ContentTaskHandler(_make_context(db, pool))
        task_id = await db.repos.tasks.create_generic_task(
            CollectionTaskType.CONTENT_GENERATE,
            title="gen",
            payload=ContentGenerateTaskPayload(pipeline_id=pid),
        )
        await handler.handle_content_generate(_generate_task(task_id, pid))

    auto_run = (await db.repos.generation_runs.list_by_pipeline(auto_id))[0]
    mod_run = (await db.repos.generation_runs.list_by_pipeline(mod_id))[0]

    # AUTO: straight to published, no approval.
    assert auto_run.moderation_status == "published"
    assert auto_run.published_at is not None
    # MODERATED: parked pending, awaiting a human, not delivered.
    assert mod_run.moderation_status == "pending"
    assert mod_run.published_at is None

    # They are genuinely different end states.
    assert auto_run.moderation_status != mod_run.moderation_status


# A typing sanity check so the GenerationRun import is exercised even if the
# above ever short-circuits; also documents the model under test.
def test_generation_run_default_states_are_non_publishable():
    run = GenerationRun()
    assert run.status == "pending"
    assert run.moderation_status == "pending"
    assert run.published_at is None
