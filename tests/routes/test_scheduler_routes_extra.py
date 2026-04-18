"""Extra tests for scheduler routes targeting uncovered lines."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import CollectionTaskStatus, SearchQuery


@pytest.fixture
async def client(base_app):
    """Create test client with scheduler."""
    app, _, pool_mock = base_app

    async def _resolve_channel(identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    pool_mock.clients = {}
    pool_mock.resolve_channel = _resolve_channel

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c


# NOTE: pure-function tests for _format_retry_hint / _compute_load_level /
# _collector_health_recommendations / _job_label live in
# tests/test_web_scheduler_helpers.py (introduced in #456). Route-level tests
# that actually exercise HTTP behaviour stay here.

# ── cancel_task: line 179 (shutting_down) ──────────────────────────────


@pytest.mark.asyncio
async def test_cancel_task_shutting_down(client):
    """Test cancel task when shutting down (line 179)."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/tasks/1/cancel", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location
    client._transport.app.state.shutting_down = False


# ── clear_pending_collect_tasks: line 188 (shutting_down) ──────────────


@pytest.mark.asyncio
async def test_clear_pending_collect_shutting_down(client):
    """Test clear pending collect when shutting down (line 188)."""
    client._transport.app.state.shutting_down = True
    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location
    client._transport.app.state.shutting_down = False


# ── scheduler_page: lines 223, 256-258 ─────────────────────────────────


@pytest.mark.asyncio
async def test_scheduler_page_status_filter_completed(client):
    """Test scheduler page with completed status filter (line 271)."""
    resp = await client.get("/scheduler/?status=completed")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_status_filter_active(client):
    """Test scheduler page with active status filter."""
    resp = await client.get("/scheduler/?status=active")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_status_filter_invalid(client):
    """Test scheduler page with invalid status filter falls back to 'all'."""
    resp = await client.get("/scheduler/?status=invalid_status")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_with_pagination(client):
    """Test scheduler page with pagination parameters (line 256-258)."""
    resp = await client.get("/scheduler/?page=2&limit=10")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_page_exceeds_total(client):
    """Test scheduler page when page exceeds total pages (lines 282-286)."""
    db = client._transport.app.state.db
    # Create one task
    await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )

    # Request page 999, should clamp
    resp = await client.get("/scheduler/?page=999&limit=50")
    assert resp.status_code == 200


# ── toggle_scheduler_job: line 338 (shutting_down), 348 ────────────────


@pytest.mark.asyncio
async def test_toggle_scheduler_job_shutting_down(client):
    """Test toggle job when shutting down (line 338)."""
    client._transport.app.state.shutting_down = True
    resp = await client.post(
        "/scheduler/jobs/collect_all/toggle",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location
    client._transport.app.state.shutting_down = False


@pytest.mark.asyncio
async def test_toggle_scheduler_job_invalid(client):
    """Test toggle job with invalid job_id (line 339-340)."""
    resp = await client.post(
        "/scheduler/jobs/invalid_job_id/toggle",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=invalid_job" in location


@pytest.mark.asyncio
async def test_toggle_scheduler_job_syncs_running(client, base_app):
    """Test toggle job syncs when scheduler is running (line 348)."""
    app, db, _ = base_app
    scheduler = app.state.scheduler

    # Make scheduler appear running
    scheduler._scheduler = MagicMock()
    scheduler._scheduler.running = True
    scheduler._scheduler.get_jobs = MagicMock(return_value=[])

    resp = await client.post(
        "/scheduler/jobs/collect_all/toggle",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=job_toggled" in location


# ── set_job_interval: lines 355, 359, 361-369, 371-385 ────────────────


@pytest.mark.asyncio
async def test_set_job_interval_shutting_down(client):
    """Test set interval when shutting down (line 355)."""
    client._transport.app.state.shutting_down = True
    resp = await client.post(
        "/scheduler/jobs/collect_all/set-interval",
        data={"interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location
    client._transport.app.state.shutting_down = False


@pytest.mark.asyncio
async def test_set_job_interval_invalid_job(client):
    """Test set interval with invalid job_id (line 357-358)."""
    resp = await client.post(
        "/scheduler/jobs/invalid_id/set-interval",
        data={"interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=invalid_job" in location


@pytest.mark.asyncio
async def test_set_job_interval_photo_due_blocked(client):
    """Test set interval for photo_due is blocked (line 358-359)."""
    resp = await client.post(
        "/scheduler/jobs/photo_due/set-interval",
        data={"interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=invalid_job" in location


@pytest.mark.asyncio
async def test_set_job_interval_photo_auto_blocked(client):
    """Test set interval for photo_auto is blocked."""
    resp = await client.post(
        "/scheduler/jobs/photo_auto/set-interval",
        data={"interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=invalid_job" in location


@pytest.mark.asyncio
async def test_set_job_interval_missing_value(client):
    """Test set interval with missing value (line 364)."""
    resp = await client.post(
        "/scheduler/jobs/collect_all/set-interval",
        data={},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=invalid_interval" in location


@pytest.mark.asyncio
async def test_set_job_interval_collect_all(client, base_app):
    """Test set interval for collect_all (lines 368-370)."""
    app, db, _ = base_app

    resp = await client.post(
        "/scheduler/jobs/collect_all/set-interval",
        data={"interval_minutes": "45"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=interval_updated" in location

    saved = await db.get_setting("collect_interval_minutes")
    assert saved == "45"


@pytest.mark.asyncio
async def test_set_job_interval_search_query(client, base_app):
    """Test set interval for search query job (lines 371-377)."""
    app, db, _ = base_app

    sq_id = await db.repos.search_queries.add(SearchQuery(
        query="test_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))

    resp = await client.post(
        f"/scheduler/jobs/sq_{sq_id}/set-interval",
        data={"interval_minutes": "120"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=interval_updated" in location


@pytest.mark.asyncio
async def test_set_job_interval_pipeline(client, base_app):
    """Test set interval for pipeline_run job (lines 378-385)."""
    app, db, _ = base_app

    from src.models import ContentPipeline
    pipeline = ContentPipeline(name="Test Pipeline", target_channel_id=-1001234567890)
    pid = await db.repos.content_pipelines.add(pipeline, source_channel_ids=[], targets=[])

    resp = await client.post(
        f"/scheduler/jobs/pipeline_run_{pid}/set-interval",
        data={"interval_minutes": "60"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=interval_updated" in location


@pytest.mark.asyncio
async def test_set_job_interval_content_generate(client, base_app):
    """Test set interval for content_generate job (lines 378-385)."""
    app, db, _ = base_app

    from src.models import ContentPipeline
    pipeline = ContentPipeline(name="Test Pipeline2", target_channel_id=-1001234567890)
    pid = await db.repos.content_pipelines.add(pipeline, source_channel_ids=[], targets=[])

    resp = await client.post(
        f"/scheduler/jobs/content_generate_{pid}/set-interval",
        data={"interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=interval_updated" in location


# ── test_notification: lines 418, 427 ──────────────────────────────────


@pytest.mark.asyncio
async def test_test_notification_shutting_down(client):
    """Test test notification when shutting down (line 418)."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/test-notification", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location
    client._transport.app.state.shutting_down = False


@pytest.mark.asyncio
async def test_test_notification_bot_not_configured(client):
    """Test notification route now queues a worker command."""
    db = client._transport.app.state.db
    resp = await client.post(
        "/scheduler/test-notification",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=test_notification_queued" in location
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.asyncio
async def test_test_notification_with_search_query(client, base_app):
    """Test notification route queues command even when queries exist."""
    app, db, _ = base_app

    await db.repos.search_queries.add(SearchQuery(
        query="test_keyword", name="TestQuery",
        notify_on_collect=True, is_active=True, is_fts=False,
    ))
    resp = await client.post(
        "/scheduler/test-notification",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=test_notification_queued" in location
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.asyncio
async def test_test_notification_no_messages(client, base_app):
    """Test notification route queues command when there are no matches."""
    app, db, _ = base_app

    await db.repos.search_queries.add(SearchQuery(
        query="nonexistent_keyword_xyz", name="NoMatch",
        notify_on_collect=True, is_active=True, is_fts=False,
    ))
    resp = await client.post(
        "/scheduler/test-notification",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "msg=test_notification_queued" in location
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


# ── dry-run-notifications: lines 490-492 ────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_with_completed_task_and_matches(client, base_app):
    """Test dry-run with completed task and matching messages (lines 460-507)."""
    app, db, _ = base_app

    # Create a completed task
    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    # Add a search query
    await db.repos.search_queries.add(SearchQuery(
        query="test_keyword",
        name="TestQuery",
        notify_on_collect=True,
        is_active=True,
        is_fts=False,
    ))

    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dry_run_search_error(client, base_app, caplog):
    """Test dry-run handles search error for a query (lines 489-492)."""
    app, db, _ = base_app

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    await db.repos.search_queries.add(SearchQuery(
        query="error_query",
        name="ErrorQuery",
        notify_on_collect=True,
        is_active=True,
        is_fts=False,
    ))

    with patch.object(
        db, "search_messages_for_query_since",
        side_effect=Exception("Search engine down"),
    ):
        resp = await client.post("/scheduler/dry-run-notifications")
        assert resp.status_code == 200


# ── _build_collector_health_context: lines 103-109 ─────────────────────


@pytest.mark.asyncio
async def test_scheduler_health_with_naive_flood_wait(client, base_app):
    """Test health context handles flood_wait_until without tzinfo (lines 103-104)."""
    app, db, _ = base_app
    accounts = await db.get_accounts(active_only=False)
    # Set flood with naive datetime (no timezone)
    naive_future = datetime.now() + timedelta(hours=1)
    for acc in accounts:
        await db.update_account_flood(acc.phone, naive_future)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


# NOTE: _job_label() has full unit coverage in tests/test_web_scheduler_helpers.py (#456).


# ── _build_jobs_context: lines 202-244 ─────────────────────────────────


@pytest.mark.asyncio
async def test_scheduler_page_with_disabled_job(client, base_app):
    """Test scheduler page shows disabled job (lines 220-228)."""
    app, db, _ = base_app
    await db.repos.settings.set_setting("scheduler_job_disabled:collect_all", "1")

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_with_sq_job(client, base_app):
    """Test scheduler page shows search query job."""
    app, db, _ = base_app

    await db.repos.search_queries.add(SearchQuery(
        query="test_query",
        notify_on_collect=True,
        is_active=True,
        is_fts=False,
    ))

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


# ── trigger: line 410-412 ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trigger_collection_uses_bulk_enqueue_msg(client, base_app):
    """Test trigger collection uses bulk_enqueue_msg for flash message."""
    from src.services.collection_service import BulkEnqueueResult

    with patch(
        "src.web.routes.scheduler.deps.collection_service"
    ) as mock_svc, patch(
        "src.web.routes.scheduler.bulk_enqueue_msg"
    ) as mock_msg:
        mock_service = MagicMock()
        result = BulkEnqueueResult(queued_count=3, skipped_existing_count=0, total_candidates=3)
        mock_service.enqueue_all_channels = AsyncMock(return_value=result)
        mock_svc.return_value = mock_service
        mock_msg.return_value = "collect_all_queued"

        resp = await client.post("/scheduler/trigger", follow_redirects=False)
        assert resp.status_code == 303
        mock_msg.assert_called_once_with(result)


# ── _is_worker_alive / state=worker_down (fix for #457) ────────────────


@pytest.mark.asyncio
async def test_is_worker_alive_fresh_heartbeat(base_app):
    """Fresh heartbeat → worker is alive."""
    from src.web.routes.scheduler import _is_worker_alive

    _, db, _ = base_app
    # Default base_app fixture already stamps a fresh heartbeat.
    assert await _is_worker_alive(db) is True


@pytest.mark.asyncio
async def test_is_worker_alive_missing_heartbeat(tmp_path):
    """No heartbeat snapshot → worker is considered down.

    Uses a clean DB (not `base_app`) so no default heartbeat is stamped.
    """
    from src.database import Database
    from src.web.routes.scheduler import _is_worker_alive

    db = Database(str(tmp_path / "worker_down.db"))
    await db.initialize()
    try:
        assert await _is_worker_alive(db) is False
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_is_worker_alive_stale_heartbeat(base_app):
    """Heartbeat older than threshold → worker is considered down."""
    from src.models import RuntimeSnapshot
    from src.web.routes.scheduler import _is_worker_alive

    _, db, _ = base_app
    stale = datetime.now(timezone.utc) - timedelta(minutes=5)
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive"},
            updated_at=stale,
        )
    )
    assert await _is_worker_alive(db) is False


@pytest.mark.asyncio
async def test_is_worker_alive_naive_datetime(base_app):
    """Heartbeat with a naive datetime must still be comparable (UTC assumed)."""
    from src.models import RuntimeSnapshot
    from src.web.routes.scheduler import _is_worker_alive

    _, db, _ = base_app
    # Stamp a recent naive datetime — the helper must treat it as UTC.
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive"},
            updated_at=now_naive,
        )
    )
    assert await _is_worker_alive(db) is True


@pytest.mark.asyncio
async def test_scheduler_page_renders_worker_down_banner(client, base_app):
    """/scheduler/ surfaces the `worker_down` banner when the heartbeat is stale.

    Regression guard for #457: before the fix, web mode silently dropped all
    collection tasks into a PENDING pile without any UI signal that no worker
    was executing them.
    """
    from src.models import RuntimeSnapshot

    _, db, _ = base_app
    # Override the fresh heartbeat stamped by base_app with a stale one.
    stale = datetime.now(timezone.utc) - timedelta(minutes=5)
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive"},
            updated_at=stale,
        )
    )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    body = resp.text
    assert "Telegram-воркер не запущен" in body
    assert "python -m src.main worker" in body


@pytest.mark.asyncio
async def test_scheduler_page_no_worker_banner_when_heartbeat_fresh(client, base_app):
    """Fresh heartbeat must NOT render the worker_down banner."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert "Telegram-воркер не запущен" not in resp.text


# ── web_mode fixture sanity (#457 round 3) ─────────────────────────────


@pytest.mark.asyncio
async def test_web_mode_app_has_snapshot_shims(web_mode_app):
    """Sanity check — the new fixture must really produce the web-mode wiring."""
    from src.web.runtime_shims import (
        SnapshotClientPool,
        SnapshotCollector,
        SnapshotSchedulerManager,
    )
    app, container = web_mode_app
    assert app.state.runtime_mode == "web"
    assert app.state.collection_queue is None
    assert app.state.unified_dispatcher is None
    # Note: task_enqueuer is NOT None in web-mode (it only writes rows to
    # collection_tasks; no asyncio.Queue involved). So pipelines /run etc.
    # stay as silent no-ops rather than 500s — that's expected behaviour.
    assert app.state.task_enqueuer is not None
    assert isinstance(app.state.pool, SnapshotClientPool)
    assert isinstance(app.state.collector, SnapshotCollector)
    assert isinstance(app.state.scheduler, SnapshotSchedulerManager)
    # Production-parity: the real container agrees.
    assert isinstance(container.pool, SnapshotClientPool)


@pytest.mark.asyncio
async def test_web_mode_scheduler_page_renders(web_mode_client):
    """/scheduler/ must render a 200 OK under the real web-mode wiring, no 500."""
    resp = await web_mode_client.get("/scheduler/", follow_redirects=True)
    assert resp.status_code == 200


# ── web-mode fallback: collection_queue=None must not 500 (fix for #457 round 2) ─


@pytest.mark.asyncio
async def test_clear_pending_collect_web_mode_falls_back_to_db(client, base_app):
    """POST /scheduler/tasks/clear-pending-collect must NOT 500 in web-mode.

    Regression guard for the second half of #457: before the fix,
    `deps.get_queue()` returned None and the route crashed with
    `AttributeError: 'NoneType' object has no attribute 'clear_pending_tasks'`.
    """
    from src.models import Channel

    app, db, _ = base_app
    # Drop the live CollectionQueue so the route sees the same state as `serve`
    # running without a worker.
    app.state.collection_queue = None

    # Seed a pending task so the DELETE has something to count.
    channel_pk = await db.add_channel(Channel(channel_id=555, title="Pending Clear"))
    task_id = await db.repos.tasks.create_collection_task_if_not_active(
        channel_id=555,
        channel_title="Pending Clear",
    )
    assert task_id is not None
    assert channel_pk is not None

    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect", follow_redirects=False
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "pending_collect_tasks_deleted" in location or "pending_collect_tasks_empty" in location

    # The row must be gone from the DB.
    remaining = await db.repos.tasks.get_pending_channel_tasks()
    assert not any(t.id == task_id for t in remaining)


@pytest.mark.asyncio
async def test_cancel_task_web_mode_falls_back_to_db(client, base_app):
    """POST /scheduler/tasks/{id}/cancel must NOT 500 in web-mode.

    Same class of regression as clear-pending-collect: the route used to dereference
    a None queue. With the fix it delegates through CollectionService, which flips
    the DB row to CANCELLED without the in-memory queue.
    """
    from src.models import Channel, CollectionTaskStatus

    app, db, _ = base_app
    app.state.collection_queue = None

    await db.add_channel(Channel(channel_id=777, title="Cancel Target"))
    task_id = await db.repos.tasks.create_collection_task_if_not_active(
        channel_id=777,
        channel_title="Cancel Target",
    )
    assert task_id is not None

    resp = await client.post(
        f"/scheduler/tasks/{task_id}/cancel", follow_redirects=False
    )
    assert resp.status_code == 303
    assert "task_cancelled" in resp.headers.get("location", "")

    # DB status must reflect the cancellation even though no worker was listening.
    task = await db.repos.tasks.get_collection_task(task_id)
    assert task is not None
    assert task.status == CollectionTaskStatus.CANCELLED


# ── #457 round 3: filter preserved on redirect ────────────────────────────
#
# Before round 3, every POST-handler in scheduler.py redirected to
# `/scheduler?msg=...` and dropped the user's `?status=active&page=N&limit=M`
# query. Clicking "Запустить" on the "Активные" tab threw the user back to
# "Все" (default status=all) — on a big DB that looked like the button replaced
# the page with 143 pages of unrelated tasks. These tests lock that fixed
# behaviour in place for every POST route in scheduler.py.


@pytest.fixture
def _filter_qs() -> str:
    return "status=active&page=3&limit=25"


async def _assert_filter_preserved(resp, expected_msg_or_error: str) -> str:
    """Redirect must carry status/page/limit AND the msg/error code."""
    assert resp.status_code == 303, f"expected 303, got {resp.status_code} body={resp.text[:200]}"
    location = resp.headers.get("location", "")
    assert "status=active" in location, location
    assert "page=3" in location, location
    assert "limit=25" in location, location
    assert expected_msg_or_error in location, location
    return location


@pytest.mark.asyncio
async def test_trigger_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/trigger?{_filter_qs}")
    # On empty DB there are no active channels → bulk_enqueue_msg returns
    # a `collect_all_*` code; we don't care which, as long as the filter survives.
    assert resp.status_code == 303
    loc = resp.headers.get("location", "")
    assert "status=active" in loc
    assert "page=3" in loc
    assert "limit=25" in loc


@pytest.mark.asyncio
async def test_start_scheduler_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/start?{_filter_qs}")
    await _assert_filter_preserved(resp, "msg=scheduler_started")


@pytest.mark.asyncio
async def test_stop_scheduler_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/stop?{_filter_qs}")
    await _assert_filter_preserved(resp, "msg=scheduler_stopped")


@pytest.mark.asyncio
async def test_trigger_warm_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/trigger-warm?{_filter_qs}")
    await _assert_filter_preserved(resp, "msg=warm_dialogs_started")


@pytest.mark.asyncio
async def test_test_notification_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/test-notification?{_filter_qs}")
    await _assert_filter_preserved(resp, "msg=test_notification_queued")


@pytest.mark.asyncio
async def test_clear_pending_collect_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(
        f"/scheduler/tasks/clear-pending-collect?{_filter_qs}"
    )
    await _assert_filter_preserved(resp, "msg=pending_collect_tasks_empty")


@pytest.mark.asyncio
async def test_cancel_task_preserves_filter(web_mode_client, web_mode_app, _filter_qs):
    _, container = web_mode_app
    task_id = await container.db.repos.tasks.create_collection_task_if_not_active(
        channel_id=-1001, channel_title="Web Mode Test Channel",
    )
    resp = await web_mode_client.post(f"/scheduler/tasks/{task_id}/cancel?{_filter_qs}")
    await _assert_filter_preserved(resp, "msg=task_cancelled")


@pytest.mark.asyncio
async def test_toggle_scheduler_job_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/jobs/collect_all/toggle?{_filter_qs}")
    await _assert_filter_preserved(resp, "msg=job_toggled")


@pytest.mark.asyncio
async def test_set_job_interval_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(
        f"/scheduler/jobs/collect_all/set-interval?{_filter_qs}",
        data={"interval_minutes": "15"},
    )
    await _assert_filter_preserved(resp, "msg=interval_updated")


# Error branches also must keep the filter — otherwise an invalid_job bounces
# the user to status=all on top of the error, which was the actual bad UX.


@pytest.mark.asyncio
async def test_invalid_job_toggle_preserves_filter(web_mode_client, _filter_qs):
    resp = await web_mode_client.post(f"/scheduler/jobs/not-a-real-job/toggle?{_filter_qs}")
    await _assert_filter_preserved(resp, "error=invalid_job")


@pytest.mark.asyncio
async def test_shutting_down_preserves_filter(web_mode_app, web_mode_client, _filter_qs):
    app, _ = web_mode_app
    app.state.shutting_down = True
    try:
        resp = await web_mode_client.post(f"/scheduler/start?{_filter_qs}")
        await _assert_filter_preserved(resp, "error=shutting_down")
    finally:
        app.state.shutting_down = False


@pytest.mark.asyncio
async def test_default_filter_omitted_from_url(web_mode_client):
    """When user is already on default filters, the redirect URL stays clean."""
    resp = await web_mode_client.post("/scheduler/start")
    assert resp.status_code == 303
    loc = resp.headers.get("location", "")
    # Default tab (status=all, page=1, limit=50) — we *don't* want those echoed.
    assert "status=" not in loc
    assert "page=" not in loc
    assert "limit=" not in loc
    assert "msg=scheduler_started" in loc


# Regression guard: all POST routes listed here must return 2xx/3xx in web-mode,
# never 500. This catches the next #457-class bug (route dereferencing a None
# nullable service) automatically.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method, path, form",
    [
        ("POST", "/scheduler/start", None),
        ("POST", "/scheduler/stop", None),
        ("POST", "/scheduler/trigger", None),
        ("POST", "/scheduler/trigger-warm", None),
        ("POST", "/scheduler/test-notification", None),
        ("POST", "/scheduler/tasks/clear-pending-collect", None),
        ("POST", "/scheduler/jobs/collect_all/toggle", None),
        ("POST", "/scheduler/jobs/collect_all/set-interval", {"interval_minutes": "15"}),
    ],
)
async def test_scheduler_post_routes_never_500_in_web_mode(web_mode_client, method, path, form):
    resp = await web_mode_client.request(method, path, data=form)
    assert resp.status_code < 500, (
        f"{method} {path} returned {resp.status_code} "
        f"(body: {resp.text[:300]}) — web-mode must never 500"
    )
