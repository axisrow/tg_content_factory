"""Tests for scheduler routes."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import Channel, CollectionTask, CollectionTaskStatus, SearchQuery, StatsAllTaskPayload
from src.web.scheduler.context import format_task_result


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


@pytest.mark.anyio
async def test_scheduler_page(client):
    """Test scheduler page renders."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_shows_status(client):
    """Test scheduler page shows scheduler status."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Page should contain scheduler info
    assert "scheduler" in resp.text.lower() or "планировщик" in resp.text.lower()


@pytest.mark.anyio
async def test_scheduler_page_with_message(client):
    """Test scheduler page with message query param."""
    resp = await client.get("/scheduler/?msg=test_message")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_with_error(client):
    """Test scheduler page with error query param."""
    resp = await client.get("/scheduler/?error=shutting_down")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_legacy_tasks_fragment_redirects_to_jobs_with_filters(client):
    resp = await client.get(
        "/scheduler/fragments/tasks?status=active&page=3&limit=25&ignored=value",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert resp.headers["location"] == (
        "/jobs?source=collection_task&status=active&page=3&limit=25"
    )


@pytest.mark.anyio
async def test_legacy_scheduler_page_bookmark_redirects_to_jobs(client):
    resp = await client.get(
        "/scheduler/?status=completed&page=2&limit=50",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert resp.headers["location"] == (
        "/jobs?source=collection_task&status=completed&page=2&limit=50"
    )


@pytest.mark.anyio
async def test_start_scheduler_redirect(client):
    """Test start scheduler redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.start = AsyncMock()
        resp = await client.post("/scheduler/start", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_start_scheduler_shutting_down(client):
    """Test start scheduler when shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/start", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location


@pytest.mark.anyio
async def test_stop_scheduler_redirect(client):
    """Test stop scheduler redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.stop = AsyncMock()
        resp = await client.post("/scheduler/stop", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_trigger_collection_redirect(client):
    """Test trigger collection redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.trigger_collection = AsyncMock()
        resp = await client.post("/scheduler/trigger", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_trigger_collection_shutting_down(client):
    """Test trigger collection when shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location


@pytest.mark.anyio
async def test_trigger_collection_enqueues(client):
    """Test trigger collection enqueues channels."""
    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "/scheduler" in location


@pytest.mark.anyio
async def test_cancel_task_redirect(client):
    """Test cancel task redirects."""
    # Create a task first
    db = client._transport.app.state.db
    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test Channel",
    )

    resp = await client.post(f"/scheduler/tasks/{task_id}/cancel", follow_redirects=False)
    assert resp.status_code == 303
    assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_cancel_task_nonexistent(client):
    """Test cancel nonexistent task."""
    resp = await client.post("/scheduler/tasks/999999/cancel", follow_redirects=False)
    # Should redirect even if task doesn't exist
    assert resp.status_code == 303


def test_format_task_result_channel_collect_with_valid_total():
    task = CollectionTask(
        task_type="channel_collect",
        messages_collected=1000,
        payload={"messages_total": 5000},
    )

    assert format_task_result(task) == "1000/5000"


def test_format_task_result_channel_collect_hides_invalid_total():
    task = CollectionTask(
        task_type="channel_collect",
        messages_collected=32878,
        payload={"messages_total": 5000},
    )

    assert format_task_result(task) == "32878"


def test_format_task_result_channel_collect_without_total():
    task = CollectionTask(task_type="channel_collect", messages_collected=32878)

    assert format_task_result(task) == "32878"


@pytest.mark.anyio
async def test_scheduler_page_shows_search_log(client):
    """Test scheduler page shows search log."""
    db = client._transport.app.state.db

    # Log a search
    await db.log_search("+1234567890", "test query", 10)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_shows_collector_status(client):
    """Test scheduler page shows collector running status."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Should show collector status somewhere
    text_lower = resp.text.lower()
    assert "running" in text_lower or "остановлен" in text_lower or "запущен" in text_lower


@pytest.mark.anyio
async def test_scheduler_shows_collector_health_card_when_all_accounts_flooded(client):
    db = client._transport.app.state.db
    accounts = await db.get_accounts(active_only=False)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/fragments/health")
    assert resp.status_code == 200
    assert "Здоровье коллектора" in resp.text
    assert "Flood Wait" in resp.text
    assert "Что делать" in resp.text


@pytest.mark.anyio
async def test_scheduler_overload_running_is_warning_not_flood_blocker(client):
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    collector = client._transport.app.state.collector
    scheduler = client._transport.app.state.scheduler
    pool.clients = {"+1234567890": MagicMock()}
    collector._running = True
    scheduler.update_interval(15)
    for i in range(101, 317):
        await db.add_channel(Channel(channel_id=i, title=f"Channel {i}"))
    task_id = await db.create_collection_task(channel_id=100, channel_title="Running Channel")
    await db.update_collection_task(task_id, CollectionTaskStatus.RUNNING, messages_collected=12)
    old_note = "Flood Wait: account +1234567890 unavailable"
    for i in range(2):
        old_task_id = await db.create_collection_task(channel_id=200 + i, channel_title=f"Old {i}")
        await db.update_collection_task(old_task_id, CollectionTaskStatus.COMPLETED, note=old_note)

    resp = await client.get("/scheduler/fragments/health")

    assert resp.status_code == 200
    assert "Риск перегрузки" in resp.text
    assert "border-warning" in resp.text
    assert "border-danger" not in resp.text
    assert "Сейчас собирается:" in resp.text
    assert "Running Channel" in resp.text
    assert "собрано 12 сообщений" in resp.text
    assert "Недавние события недоступности" in resp.text
    assert "×2" in resp.text
    active_flood_reason = (
        "Почему сейчас не собираем:</strong> доступных аккаунтов нет или их недостаточно. Flood Wait активен"
    )
    assert active_flood_reason not in resp.text


@pytest.mark.anyio
async def test_scheduler_all_flooded_keeps_danger_current_reason(client):
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    pool.clients = {"+1234567890": MagicMock()}
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    for acc in await db.get_accounts(active_only=False):
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/fragments/health")

    assert resp.status_code == 200
    assert "border-danger" in resp.text
    assert "Все аккаунты во Flood Wait" in resp.text
    active_flood_reason = (
        "Почему сейчас не собираем:</strong> доступных аккаунтов нет или их недостаточно. Flood Wait активен"
    )
    assert active_flood_reason in resp.text


@pytest.mark.anyio
async def test_scheduler_shows_interval(client):
    """Test scheduler page shows collection interval."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_empty_tasks(client):
    """Test scheduler page with no tasks."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_empty_search_log(client):
    """Test scheduler page with no search log."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_has_active_tasks_flag(client):
    """Test scheduler page has active tasks detection."""
    db = client._transport.app.state.db

    # Create pending task
    await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_run_display(client):
    """Test scheduler displays last run time."""
    # Set last_run on scheduler
    client._transport.app.state.scheduler._last_run = datetime.now(timezone.utc)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_stats_display(client):
    """Test scheduler displays last stats."""
    client._transport.app.state.scheduler._last_stats = {"collected": 100}

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_search_run_display(client):
    """Test scheduler displays last search run time."""
    client._transport.app.state.scheduler._last_search_run = datetime.now(timezone.utc)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_search_stats_display(client):
    """Test scheduler displays last search stats."""
    client._transport.app.state.scheduler._last_search_stats = {"queries": 5}

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_interval_minutes(client):
    """Test scheduler displays interval minutes."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_search_interval_minutes(client):
    """Test scheduler displays search interval minutes."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_start_scheduler_calls_service(client):
    """Test start scheduler enqueues reconcile command."""
    db = client._transport.app.state.db
    await client.post("/scheduler/start")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "scheduler.reconcile"


@pytest.mark.anyio
async def test_stop_scheduler_calls_service(client):
    """Test stop scheduler enqueues reconcile command."""
    db = client._transport.app.state.db
    await client.post("/scheduler/stop")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "scheduler.reconcile"


@pytest.mark.anyio
async def test_start_scheduler_sets_autostart_flag(client):
    """POST /scheduler/start persists scheduler_autostart=1 to DB."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/start", follow_redirects=False)
    assert resp.status_code == 303
    value = await db.get_setting("scheduler_autostart")
    assert value == "1"


@pytest.mark.anyio
async def test_stop_scheduler_clears_autostart_flag(client):
    """POST /scheduler/stop persists scheduler_autostart=0 to DB."""
    db = client._transport.app.state.db
    await db.set_setting("scheduler_autostart", "1")
    resp = await client.post("/scheduler/stop", follow_redirects=False)
    assert resp.status_code == 303
    value = await db.get_setting("scheduler_autostart")
    assert value == "0"


@pytest.mark.anyio
async def test_trigger_collection_calls_service(client):
    """Test trigger collection calls collection service."""
    mock_service = MagicMock()
    mock_result = MagicMock()
    mock_result.queued_count = 0
    mock_result.skipped_existing_count = 0
    mock_result.total_candidates = 0
    mock_service.enqueue_all_channels = AsyncMock(return_value=mock_result)

    with patch(
        "src.web.routes.scheduler.deps.collection_service",
        return_value=mock_service,
    ):
        await client.post("/scheduler/trigger")
        mock_service.enqueue_all_channels.assert_called_once()


@pytest.mark.anyio
async def test_cancel_task_calls_queue(client):
    """Test cancel task calls queue cancel."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )

    resp = await client.post(f"/scheduler/tasks/{task_id}/cancel")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_clear_pending_collect_tasks_redirects_and_deletes_only_pending_channel_tasks(client):
    db = client._transport.app.state.db

    pending_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Pending Channel",
    )
    running_id = await db.create_collection_task(
        channel_id=-1001234567891,
        channel_title="Running Channel",
    )
    await db.update_collection_task(running_id, CollectionTaskStatus.RUNNING)
    await db.create_stats_task(StatsAllTaskPayload(channel_ids=[-1001234567890]))

    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=pending_collect_tasks_deleted" in resp.headers["location"]
    assert await db.get_collection_task(pending_id) is None
    assert (await db.get_collection_task(running_id)).status == CollectionTaskStatus.RUNNING


@pytest.mark.anyio
async def test_clear_pending_collect_tasks_empty_queue_redirects(client):
    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=pending_collect_tasks_empty" in resp.headers["location"]


@pytest.mark.anyio
async def test_scheduler_page_hides_clear_button_when_no_pending(client):
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert 'action="/scheduler/tasks/clear-pending-collect"' not in resp.text


@pytest.mark.anyio
async def test_trigger_then_get_scheduler_with_pending_tasks(client):
    """Trigger collect-all then follow redirect to scheduler page with pending tasks.

    Reproduces the 500 'No response returned' scenario where the scheduler
    page fails to render when pending tasks exist after triggering collection.
    """
    db = client._transport.app.state.db

    # Seed several channels so enqueue_all_channels creates tasks
    for i in range(3):
        await db.add_channel(Channel(
            channel_id=-(1001000000 + i),
            title=f"Channel {i}",
            username=f"ch{i}",
            channel_type="channel",
        ))

    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert location.startswith("/scheduler")
    assert "msg=collect_all_queued" in location

    redirected = await client.get(location)
    assert redirected.status_code == 200
    assert "Планировщик" in redirected.text


# ── Dry-run notification tests ──────────────────────────────────────


@pytest.mark.anyio
async def test_dry_run_no_queries(client):
    """Dry-run with no notification queries shows empty state."""
    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "Нет активных запросов" in resp.text


@pytest.mark.anyio
async def test_dry_run_excludes_inactive_queries(client):
    """Dry-run excludes queries with is_active=False."""
    db = client._transport.app.state.db
    await db.repos.search_queries.add(SearchQuery(
        query="active_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))
    await db.repos.search_queries.add(SearchQuery(
        query="inactive_query", notify_on_collect=True, is_active=False, is_fts=False,
    ))
    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "inactive_query" not in resp.text


@pytest.mark.anyio
async def test_dry_run_excludes_disabled_scheduler_job(client):
    """Dry-run excludes queries whose scheduler job is disabled."""
    db = client._transport.app.state.db
    # Create a completed collection task so dry-run has a time window
    task_id = await db.create_collection_task(channel_id=-1001234567890, channel_title="Test")
    await db.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    await db.repos.search_queries.add(SearchQuery(
        query="enabled_job_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))
    disabled_id = await db.repos.search_queries.add(SearchQuery(
        query="disabled_job_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))
    # Disable the scheduler job for the second query
    await db.repos.settings.set_setting(f"scheduler_job_disabled:sq_{disabled_id}", "1")

    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "enabled_job_query" in resp.text
    assert "disabled_job_query" not in resp.text


@pytest.mark.anyio
async def test_scheduler_shows_flood_wait_countdown(client):
    """Test that scheduler page shows flood wait countdown in hours and minutes."""
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    accounts = await db.get_accounts(active_only=False)

    # Put account in pool so it appears in connected_active_accounts
    pool.clients = {acc.phone: MagicMock() for acc in accounts}

    # Set flood wait for 3 hours in future (round number avoids second-boundary races)
    future = datetime.now(timezone.utc) + timedelta(hours=3)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/fragments/health")
    assert resp.status_code == 200
    # Check that countdown is displayed — "3 ч 0 мин" or similar
    assert " ч " in resp.text or " мин)" in resp.text


@pytest.mark.anyio
async def test_scheduler_hides_countdown_if_too_short(client):
    """Test that countdown is hidden if less than 60 seconds remain."""
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    accounts = await db.get_accounts(active_only=False)

    # Put account in pool so it appears in connected_active_accounts
    pool.clients = {acc.phone: MagicMock() for acc in accounts}

    # Set flood wait for only 30 seconds (below the 60s threshold, shouldn't show countdown)
    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Countdown should not appear for very short waits
    assert "(0 мин)" not in resp.text
    assert "Все аккаунты во Flood Wait" not in resp.text
    assert "Частичная деградация" not in resp.text


@pytest.mark.anyio
async def test_pause_queue_sets_setting_and_enqueues(client):
    """POST /scheduler/pause persists collection_queue_paused=1 and enqueues collection.pause."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/pause", follow_redirects=False)
    assert resp.status_code == 303
    assert await db.get_setting("collection_queue_paused") == "1"
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "collection.pause"


@pytest.mark.anyio
async def test_resume_queue_sets_setting_and_enqueues(client):
    """POST /scheduler/resume persists collection_queue_paused=0 and enqueues collection.resume."""
    db = client._transport.app.state.db
    await db.set_setting("collection_queue_paused", "1")
    resp = await client.post("/scheduler/resume", follow_redirects=False)
    assert resp.status_code == 303
    assert await db.get_setting("collection_queue_paused") == "0"
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "collection.resume"


@pytest.mark.anyio
async def test_pause_queue_shutting_down(client):
    """POST /scheduler/pause is rejected while shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/pause", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=shutting_down" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_scheduler_page_shows_pause_button_when_running(client):
    """The page offers 'Пауза очереди' when not paused, 'Продолжить очередь' when paused."""
    db = client._transport.app.state.db
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert "Пауза очереди" in resp.text
    assert "Очередь на паузе" not in resp.text

    await db.set_setting("collection_queue_paused", "1")
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert "Продолжить очередь" in resp.text
    assert "Очередь на паузе" in resp.text


@pytest.mark.anyio
async def test_scheduler_page_lazy_loads_fragments(client):
    """The scheduler shell only lazy-loads health and periodic scheduler jobs."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert 'hx-get="/scheduler/fragments/health"' in resp.text
    assert 'hx-get="/scheduler/fragments/jobs"' in resp.text
    assert 'hx-get="/scheduler/fragments/tasks' not in resp.text
    assert 'id="tasks-fragment"' not in resp.text
    assert 'hx-trigger="load"' in resp.text
    # Controls stay in the skeleton.
    assert 'id="dry-run-btn"' in resp.text


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path",
    [
        "/scheduler/fragments/health",
        "/scheduler/fragments/jobs",
    ],
)
async def test_scheduler_fragments_return_partial_html(client, path):
    """Scheduler fragment endpoints return bare partials, not a full page."""
    resp = await client.get(path)
    assert resp.status_code == 200
    assert "<html" not in resp.text.lower()
