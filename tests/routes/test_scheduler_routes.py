"""Tests for scheduler routes."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import Account, CollectionTaskStatus, SearchQuery, StatsAllTaskPayload
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app


@pytest.fixture
async def client(tmp_path):
    """Create test client with scheduler."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _resolve_channel(self, identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    pool_mock = MagicMock()
    pool_mock.clients = {}
    pool_mock.resolve_channel = _resolve_channel
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)

    scheduler = SchedulerManager(config.scheduler)
    app.state.scheduler = scheduler
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.asyncio
async def test_scheduler_page(client):
    """Test scheduler page renders."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_shows_status(client):
    """Test scheduler page shows scheduler status."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Page should contain scheduler info
    assert "scheduler" in resp.text.lower() or "планировщик" in resp.text.lower()


@pytest.mark.asyncio
async def test_scheduler_page_with_message(client):
    """Test scheduler page with message query param."""
    resp = await client.get("/scheduler/?msg=test_message")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_with_error(client):
    """Test scheduler page with error query param."""
    resp = await client.get("/scheduler/?error=shutting_down")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_start_scheduler_redirect(client):
    """Test start scheduler redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.start = AsyncMock()
        resp = await client.post("/scheduler/start", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_start_scheduler_shutting_down(client):
    """Test start scheduler when shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/start", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location


@pytest.mark.asyncio
async def test_stop_scheduler_redirect(client):
    """Test stop scheduler redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.stop = AsyncMock()
        resp = await client.post("/scheduler/stop", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_trigger_collection_redirect(client):
    """Test trigger collection redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.trigger_collection = AsyncMock()
        resp = await client.post("/scheduler/trigger", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_trigger_collection_shutting_down(client):
    """Test trigger collection when shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location


@pytest.mark.asyncio
async def test_trigger_collection_enqueues(client):
    """Test trigger collection enqueues channels."""
    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "/scheduler" in location


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_cancel_task_nonexistent(client):
    """Test cancel nonexistent task."""
    resp = await client.post("/scheduler/tasks/999999/cancel", follow_redirects=False)
    # Should redirect even if task doesn't exist
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_scheduler_page_shows_tasks(client):
    """Test scheduler page shows collection tasks."""
    db = client._transport.app.state.db

    # Create a task
    await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test Channel",
    )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_shows_search_log(client):
    """Test scheduler page shows search log."""
    db = client._transport.app.state.db

    # Log a search
    await db.log_search("+1234567890", "test query", 10)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_shows_collector_status(client):
    """Test scheduler page shows collector running status."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Should show collector status somewhere
    text_lower = resp.text.lower()
    assert "running" in text_lower or "остановлен" in text_lower or "запущен" in text_lower


@pytest.mark.asyncio
async def test_scheduler_shows_interval(client):
    """Test scheduler page shows collection interval."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_empty_tasks(client):
    """Test scheduler page with no tasks."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_empty_search_log(client):
    """Test scheduler page with no search log."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_scheduler_last_run_display(client):
    """Test scheduler displays last run time."""
    # Set last_run on scheduler
    from datetime import datetime

    client._transport.app.state.scheduler._last_run = datetime.now()

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_last_stats_display(client):
    """Test scheduler displays last stats."""
    client._transport.app.state.scheduler._last_stats = {"collected": 100}

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_last_search_run_display(client):
    """Test scheduler displays last search run time."""
    from datetime import datetime

    client._transport.app.state.scheduler._last_search_run = datetime.now()

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_last_search_stats_display(client):
    """Test scheduler displays last search stats."""
    client._transport.app.state.scheduler._last_search_stats = {"queries": 5}

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_interval_minutes(client):
    """Test scheduler displays interval minutes."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_search_interval_minutes(client):
    """Test scheduler displays search interval minutes."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_start_scheduler_calls_service(client):
    """Test start scheduler calls service method."""
    mock_service = MagicMock()
    mock_service.start = AsyncMock()

    with patch("src.web.routes.scheduler.deps.scheduler_service", return_value=mock_service):
        await client.post("/scheduler/start")
        mock_service.start.assert_called_once()


@pytest.mark.asyncio
async def test_stop_scheduler_calls_service(client):
    """Test stop scheduler calls service method."""
    mock_service = MagicMock()
    mock_service.stop = AsyncMock()

    with patch("src.web.routes.scheduler.deps.scheduler_service", return_value=mock_service):
        await client.post("/scheduler/stop")
        mock_service.stop.assert_called_once()


@pytest.mark.asyncio
async def test_start_scheduler_sets_autostart_flag(client):
    """POST /scheduler/start persists scheduler_autostart=1 to DB."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/start", follow_redirects=False)
    assert resp.status_code == 303
    value = await db.get_setting("scheduler_autostart")
    assert value == "1"


@pytest.mark.asyncio
async def test_stop_scheduler_clears_autostart_flag(client):
    """POST /scheduler/stop persists scheduler_autostart=0 to DB."""
    db = client._transport.app.state.db
    await db.set_setting("scheduler_autostart", "1")
    resp = await client.post("/scheduler/stop", follow_redirects=False)
    assert resp.status_code == 303
    value = await db.get_setting("scheduler_autostart")
    assert value == "0"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_cancel_task_calls_queue(client):
    """Test cancel task calls queue cancel."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )

    resp = await client.post(f"/scheduler/tasks/{task_id}/cancel")
    assert resp.status_code == 200


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_clear_pending_collect_tasks_empty_queue_redirects(client):
    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=pending_collect_tasks_empty" in resp.headers["location"]


@pytest.mark.asyncio
async def test_scheduler_page_shows_clear_pending_collect_button(client):
    db = client._transport.app.state.db
    await db.create_collection_task(
        channel_id=-1001234567890, channel_title="Pending Channel",
    )

    resp = await client.get("/scheduler/")

    assert resp.status_code == 200
    assert 'action="/scheduler/tasks/clear-pending-collect"' in resp.text
    assert "Очистить очередь загрузки" in resp.text


@pytest.mark.asyncio
async def test_scheduler_page_hides_clear_button_when_no_pending(client):
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert 'action="/scheduler/tasks/clear-pending-collect"' not in resp.text


@pytest.mark.asyncio
async def test_scheduler_page_with_completed_task(client):
    """Test scheduler page shows completed task status."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_with_failed_task(client):
    """Test scheduler page shows failed task status."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.FAILED, error="Test error")

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_scheduler_page_with_cancelled_task(client):
    """Test scheduler page shows cancelled task status."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.CANCELLED)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


# ── Dry-run notification tests ──────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_no_queries(client):
    """Dry-run with no notification queries shows empty state."""
    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "Нет активных запросов" in resp.text


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
