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


# ── _format_retry_hint: line 38 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_format_retry_hint_with_value():
    """Test _format_retry_hint with a datetime value (line 38)."""
    from src.web.routes.scheduler import _format_retry_hint

    dt = datetime(2025, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
    result = _format_retry_hint(dt)
    assert "2025-06-15" in result
    assert "12:30:00" in result


@pytest.mark.asyncio
async def test_format_retry_hint_none():
    """Test _format_retry_hint with None (line 37)."""
    from src.web.routes.scheduler import _format_retry_hint

    result = _format_retry_hint(None)
    assert result == ""


# ── _compute_load_level: lines 53, 55, 57 ──────────────────────────────


@pytest.mark.asyncio
async def test_compute_load_level_overload_short_interval():
    """Test load level overload with short interval and high pressure (line 53)."""
    from src.web.routes.scheduler import _compute_load_level

    result = _compute_load_level(
        interval_minutes=15,
        active_unfiltered_channels=120,
        available_accounts_now=2,
        state="healthy",
    )
    assert result == "overload"


@pytest.mark.asyncio
async def test_compute_load_level_high_medium_interval():
    """Test load level high with medium interval and moderate pressure (line 55)."""
    from src.web.routes.scheduler import _compute_load_level

    result = _compute_load_level(
        interval_minutes=30,
        active_unfiltered_channels=80,
        available_accounts_now=2,
        state="healthy",
    )
    assert result == "high"


@pytest.mark.asyncio
async def test_compute_load_level_high_very_high_pressure():
    """Test load level high with very high pressure regardless of interval (line 57)."""
    from src.web.routes.scheduler import _compute_load_level

    result = _compute_load_level(
        interval_minutes=60,
        active_unfiltered_channels=150,
        available_accounts_now=2,
        state="healthy",
    )
    assert result == "high"


@pytest.mark.asyncio
async def test_compute_load_level_ok():
    """Test load level ok with normal parameters."""
    from src.web.routes.scheduler import _compute_load_level

    result = _compute_load_level(
        interval_minutes=60,
        active_unfiltered_channels=10,
        available_accounts_now=2,
        state="healthy",
    )
    assert result == "ok"


@pytest.mark.asyncio
async def test_compute_load_level_overload_all_flooded():
    """Test load level overload when all flooded (line 48)."""
    from src.web.routes.scheduler import _compute_load_level

    result = _compute_load_level(
        interval_minutes=60,
        active_unfiltered_channels=10,
        available_accounts_now=2,
        state="all_flooded",
    )
    assert result == "overload"


# ── _collector_health_recommendations: line 71 ─────────────────────────


@pytest.mark.asyncio
async def test_collector_health_recommendations_no_clients():
    """Test recommendations for no_clients state (line 73)."""
    from src.web.routes.scheduler import _collector_health_recommendations

    recs = _collector_health_recommendations(
        state="no_clients",
        load_level="ok",
        interval_minutes=60,
        active_unfiltered_channels=10,
        available_accounts_now=0,
    )
    assert any("переподключить" in r.lower() for r in recs)


@pytest.mark.asyncio
async def test_collector_health_recommendations_single_account():
    """Test recommendations for single account (line 83)."""
    from src.web.routes.scheduler import _collector_health_recommendations

    recs = _collector_health_recommendations(
        state="healthy",
        load_level="ok",
        interval_minutes=60,
        active_unfiltered_channels=10,
        available_accounts_now=1,
    )
    assert any("Добавить ещё Telegram-аккаунт" in r for r in recs)


@pytest.mark.asyncio
async def test_collector_health_recommendations_high_load():
    """Test recommendations for high load (lines 78-81)."""
    from src.web.routes.scheduler import _collector_health_recommendations

    recs = _collector_health_recommendations(
        state="healthy",
        load_level="high",
        interval_minutes=30,
        active_unfiltered_channels=100,
        available_accounts_now=2,
    )
    assert len(recs) >= 2
    assert any("интервал" in r.lower() for r in recs)
    assert any("Сократить" in r for r in recs)


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


# ── _job_label: various job IDs ────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_label_search_query():
    """Test _job_label for search query job."""
    from src.web.routes.scheduler import _job_label

    result = _job_label("sq_42")
    assert "42" in result


@pytest.mark.asyncio
async def test_job_label_pipeline_run():
    """Test _job_label for pipeline run job."""
    from src.web.routes.scheduler import _job_label

    result = _job_label("pipeline_run_7")
    assert "7" in result


@pytest.mark.asyncio
async def test_job_label_content_generate():
    """Test _job_label for content generate job."""
    from src.web.routes.scheduler import _job_label

    result = _job_label("content_generate_3")
    assert "3" in result


@pytest.mark.asyncio
async def test_job_label_unknown():
    """Test _job_label for unknown job."""
    from src.web.routes.scheduler import _job_label

    result = _job_label("custom_job")
    assert result == "custom_job"


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
