"""Web mutations must enqueue a ``scheduler.reconcile`` command (#1236, #1247).

In web-mode ``app.state.scheduler`` is the read-only ``SnapshotSchedulerManager``
shim whose ``sync_search_query_jobs``/``sync_pipeline_jobs``/``update_interval`` are
no-ops. The only way a UI mutation reaches the live worker scheduler is by enqueuing
a ``scheduler.reconcile`` telegram command (handled by the worker's
``_handle_scheduler_reconcile``, which re-registers sq_/pipeline jobs and re-reads
the collect interval). These tests pin that the mutation routes do enqueue it — the
regression that #1236/#1247 fixed silently dropped the sync.
"""

from __future__ import annotations

import pytest

from src.models import Account, Channel, SearchQuery
from src.services.pipeline_service import PipelineService, PipelineTargetRef
from src.web.runtime_shims import SnapshotSchedulerManager
from tests.helpers import build_web_app, make_auth_client, make_test_config


@pytest.fixture
async def web_client(tmp_path, real_pool_harness_factory):
    """Authenticated web client running in web-mode (snapshot scheduler shim).

    Yields ``(client, db)`` so tests can drive routes and read back the
    ``telegram_commands`` table. The scheduler is forced to the shim so the test
    proves the reconcile command is enqueued even when the in-process
    ``sync_*``/``update_interval`` calls are no-ops.
    """
    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)
    # Force web-mode scheduler: behind the web container in production the scheduler
    # is always the snapshot shim, not the live SchedulerManager (#444).
    app.state.scheduler = SnapshotSchedulerManager(db, config.scheduler.collect_interval_minutes)

    await db.add_account(Account(phone="+100", session_string="sess"))
    await db.add_channel(Channel(channel_id=1001, title="Source A"))
    await db.repos.dialog_cache.replace_dialogs(
        "+100",
        [{"channel_id": 77, "title": "Target A", "username": "targeta", "channel_type": "channel"}],
    )

    async with make_auth_client(app) as client:
        yield client, db

    await app.state.collection_queue.shutdown()
    await db.close()


async def _reconcile_count(db) -> int:
    commands = await db.repos.telegram_commands.list_commands(command_type="scheduler.reconcile")
    return len(commands)


async def _seed_pipeline(db) -> int:
    return await PipelineService(db).add(
        name="Seed pipeline",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )


# ---------------------------------------------------------------------------
# #1236 — search-query mutations
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_add_search_query_enqueues_reconcile(web_client):
    client, db = web_client
    assert await _reconcile_count(db) == 0

    resp = await client.post("/search-queries/add", data={"query": "crypto", "interval_minutes": "30"})
    assert resp.status_code == 200
    assert await _reconcile_count(db) == 1


@pytest.mark.anyio
async def test_toggle_search_query_enqueues_reconcile(web_client):
    client, db = web_client
    sq_id = await db.repos.search_queries.add(SearchQuery(query="seed", interval_minutes=60))
    assert await _reconcile_count(db) == 0

    resp = await client.post(f"/search-queries/{sq_id}/toggle")
    assert resp.status_code == 200
    assert await _reconcile_count(db) == 1


@pytest.mark.anyio
async def test_delete_search_query_enqueues_reconcile(web_client):
    client, db = web_client
    sq_id = await db.repos.search_queries.add(SearchQuery(query="seed", interval_minutes=60))

    resp = await client.post(f"/search-queries/{sq_id}/delete")
    assert resp.status_code == 200
    assert await _reconcile_count(db) == 1


# ---------------------------------------------------------------------------
# #1236 — pipeline mutations
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_add_pipeline_enqueues_reconcile(web_client):
    client, db = web_client
    assert await _reconcile_count(db) == 0

    resp = await client.post(
        "/pipelines/add",
        data={
            "name": "News",
            "prompt_template": "Summarize {source_messages}",
            "source_channel_ids": ["1001"],
            "target_refs": ["+100|77"],
            "publish_mode": "moderated",
            "generation_backend": "chain",
            "generate_interval_minutes": "60",
        },
    )
    assert resp.status_code == 200
    assert await _reconcile_count(db) == 1


@pytest.mark.anyio
async def test_toggle_pipeline_enqueues_reconcile(web_client):
    client, db = web_client
    pid = await _seed_pipeline(db)
    assert await _reconcile_count(db) == 0

    resp = await client.post(f"/pipelines/{pid}/toggle")
    assert resp.status_code == 200
    assert await _reconcile_count(db) == 1


@pytest.mark.anyio
async def test_delete_pipeline_enqueues_reconcile(web_client):
    client, db = web_client
    pid = await _seed_pipeline(db)

    resp = await client.post(f"/pipelines/{pid}/delete")
    assert resp.status_code == 200
    assert await _reconcile_count(db) == 1


# ---------------------------------------------------------------------------
# #1247 — collect_interval saved from the Settings page
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_save_scheduler_settings_enqueues_reconcile(web_client):
    client, db = web_client
    assert await _reconcile_count(db) == 0

    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "15", "reaction_min_interval_sec": "10"},
    )
    assert resp.status_code == 200
    # The interval was persisted...
    assert await db.repos.settings.get_setting("collect_interval_minutes") == "15"
    # ...and a reconcile was enqueued so the worker rebuilds its IntervalTrigger.
    assert await _reconcile_count(db) == 1
