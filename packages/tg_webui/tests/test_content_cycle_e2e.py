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

import pytest

from tests.content_cycle_helpers import (
    _generate_task,
    _make_context,
    _patch_generation,
    _seed_pipeline,
    _seed_source_channel,
    _target,
)

# FakeClientPool is reused from the publish-service unit tests instead of being
# re-rolled here: it routes get_client_by_phone / release_client /
# resolve_dialog_entity to a FakeClient that records each send, so the real
# PublishService._publish_to_target runs end-to-end with no network.
from tests.test_publish_service import FakeClientPool
from tg_agent.models import (
    CollectionTaskType,
    ContentGenerateTaskPayload,
    PipelinePublishMode,
)
from tg_agent.services.task_handlers.content import ContentTaskHandler

# ---------------------------------------------------------------------------
# Fakes for the two external edges only: LLM provider and the draft notifier.
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# Real services wired to the real :memory: db; only provider + notifier faked.
# ---------------------------------------------------------------------------


















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
