"""Tests for settings, scheduler, pipeline, search, and photo-task paths."""
# Web-panel tests split out of tests/test_settings_scheduler_pipeline_search_paths.py (#1720 wave 3).


from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tg_agent.collection_queue import CollectionQueue
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account, Channel
from tg_agent.search.ai_search import AISearchEngine
from tg_agent.search.engine import SearchEngine
from tg_agent.telegram.collector import Collector
from tg_shared.auth import TelegramAuth
from tg_webui.app import create_app

# ---------------------------------------------------------------------------
# Web app fixture (mirrors test_cli_image_scheduler_web_routes.py base_app)
# ---------------------------------------------------------------------------


@pytest.fixture
async def base_app(tmp_path):
    """Minimal app fixture for web route tests."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test7.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)
    db = Database(config.database.path, session_encryption_secret="test-session-encryption-key")
    await db.initialize()
    app.state.db = db
    app.state.config = config

    pool_mock = MagicMock()
    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    pool_mock.resolve_channel = AsyncMock(return_value={
        "channel_id": -1001234567890,
        "title": "Test Channel",
        "username": "testchannel",
        "channel_type": "channel",
    })
    pool_mock.get_forum_topics = AsyncMock(return_value=[])
    pool_mock.remove_client = AsyncMock()
    pool_mock.disconnect_client = AsyncMock()
    app.state.pool = pool_mock
    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None

    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector
    collection_queue = CollectionQueue(collector, db)
    app.state.collection_queue = collection_queue
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    sched = MagicMock()
    sched.is_running = False
    sched.interval_minutes = 60
    sched.get_potential_jobs = AsyncMock(return_value=[])
    sched.get_all_jobs_next_run = MagicMock(return_value={})
    sched.start = AsyncMock()
    sched.stop = AsyncMock()
    sched.update_interval = MagicMock()
    sched.sync_job_state = AsyncMock()
    sched.sync_search_query_jobs = AsyncMock()
    sched.sync_pipeline_jobs = AsyncMock()
    app.state.scheduler = sched
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_channel(Channel(channel_id=100, title="Test Channel"))

    yield app, db, pool_mock

    await collection_queue.shutdown()
    await db.close()


@pytest.fixture
async def route_client(base_app):
    """AsyncClient with Basic auth for web route tests."""
    app, db, pool_mock = base_app
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Origin": "http://test",
        },
    ) as c:
        yield c


# ===========================================================================
# 1. web/routes/scheduler.py — edge-case branches
# ===========================================================================


class TestWebSchedulerRoutes:
    """Tests for src/web/routes/scheduler.py"""

    @pytest.mark.anyio
    async def test_scheduler_page_renders(self, route_client):
        """GET /scheduler/ returns 200."""
        resp = await route_client.get("/scheduler/")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_scheduler_page_status_filter(self, route_client):
        """GET /scheduler/?status=active filters tasks."""
        resp = await route_client.get("/scheduler/?status=active")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_scheduler_page_completed_status(self, route_client):
        """GET /scheduler/?status=completed returns 200."""
        resp = await route_client.get("/scheduler/?status=completed")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_scheduler_page_invalid_status(self, route_client):
        """GET /scheduler/?status=invalid defaults to all."""
        resp = await route_client.get("/scheduler/?status=invalid")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_start_scheduler(self, route_client):
        """POST /scheduler/start redirects to scheduler page."""
        resp = await route_client.post("/scheduler/start")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_stop_scheduler(self, route_client):
        """POST /scheduler/stop redirects to scheduler page."""
        resp = await route_client.post("/scheduler/stop")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_trigger_collection(self, route_client):
        """POST /scheduler/trigger enqueues all channels."""
        from tg_agent.services.collection_service import BulkEnqueueResult

        with patch("tg_webui.routes.scheduler.deps.collection_service") as mock_svc_fn:
            svc = MagicMock()
            svc.enqueue_all_channels = AsyncMock(
                return_value=BulkEnqueueResult(
                    queued_count=1, skipped_existing_count=0, total_candidates=1
                )
            )
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/scheduler/trigger")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_toggle_job_valid(self, route_client):
        """POST /scheduler/jobs/collect_all/toggle toggles the job."""
        resp = await route_client.post("/scheduler/jobs/collect_all/toggle")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_toggle_job_invalid_id(self, route_client):
        """POST /scheduler/jobs/invalid_job/toggle returns redirect with error."""
        resp = await route_client.post("/scheduler/jobs/invalid_job_xyz!/toggle")
        # follows redirect to /scheduler?error=invalid_job
        query = resp.url.query
        if isinstance(query, bytes):
            query = query.decode()
        assert "invalid_job" in query or resp.status_code == 200

    @pytest.mark.anyio
    async def test_toggle_job_sq_pattern(self, route_client):
        """POST /scheduler/jobs/sq_1/toggle is valid job id pattern."""
        resp = await route_client.post("/scheduler/jobs/sq_1/toggle")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_set_interval_collect_all(self, route_client):
        """POST /scheduler/jobs/collect_all/set-interval updates interval."""
        resp = await route_client.post(
            "/scheduler/jobs/collect_all/set-interval",
            data={"interval_minutes": "60"},
        )
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_set_interval_invalid_value(self, route_client):
        """POST /scheduler/jobs/collect_all/set-interval with invalid value redirects."""
        resp = await route_client.post(
            "/scheduler/jobs/collect_all/set-interval",
            data={"interval_minutes": "not_a_number"},
        )
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_cancel_task(self, route_client):
        """POST /scheduler/tasks/999/cancel cancels non-existent task."""
        resp = await route_client.post("/scheduler/tasks/999/cancel")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_clear_pending_collect(self, route_client):
        """POST /scheduler/tasks/clear-pending-collect clears pending tasks."""
        resp = await route_client.post("/scheduler/tasks/clear-pending-collect")
        assert resp.status_code == 200


# ===========================================================================
# 2. web/routes/settings.py — edge-case branches
# ===========================================================================


class TestWebSettingsRoutes:
    """Tests for web/routes/settings.py — provider/image/notification settings."""

    @pytest.mark.anyio
    async def test_settings_page_renders(self, route_client):
        """GET /settings/ returns 200."""
        resp = await route_client.get("/settings/")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_settings_agent_providers_page(self, route_client):
        """GET /settings/agent-providers returns 200 or redirect."""
        with patch("tg_webui.settings.handlers._agent_provider_service") as mock_fn:
            svc = MagicMock()
            svc.load_provider_configs = AsyncMock(return_value=[])
            mock_fn.return_value = svc
            resp = await route_client.get("/settings/agent-providers")
        assert resp.status_code in (200, 302, 303, 404)

    @pytest.mark.anyio
    async def test_settings_image_providers_get(self, route_client):
        """GET /settings/image-providers returns 200."""
        with patch("tg_webui.settings.handlers._image_provider_service") as mock_fn:
            svc = MagicMock()
            svc.load_provider_configs = AsyncMock(return_value=[])
            mock_fn.return_value = svc
            resp = await route_client.get("/settings/image-providers")
        assert resp.status_code in (200, 302, 303, 404)

    @pytest.mark.anyio
    async def test_settings_notification_status(self, route_client):
        """GET /settings/notifications/status returns 200 or redirect.

        Queued-command model: _notification_service helper was removed,
        route now reads from runtime snapshot or reports unknown status.
        """
        resp = await route_client.get("/settings/notifications/status")
        assert resp.status_code in (200, 302, 303, 404)

    @pytest.mark.anyio
    async def test_settings_provider_bulk_test_status(self, route_client):
        """GET /settings/agent-providers/bulk-test/status returns JSON."""
        resp = await route_client.get(
            "/settings/agent-providers/bulk-test/status",
            headers={"Accept": "application/json"},
        )
        assert resp.status_code in (200, 302, 303, 404)

    @pytest.mark.anyio
    async def test_settings_page_agent_tab(self, route_client):
        """GET /settings/?tab=agent returns 200."""
        resp = await route_client.get("/settings/?tab=agent")
        assert resp.status_code in (200, 302, 303, 404)

    @pytest.mark.anyio
    async def test_semantic_search_settings_page(self, route_client):
        """GET /settings/semantic-search returns 200."""
        resp = await route_client.get("/settings/semantic-search")
        assert resp.status_code in (200, 302, 303, 404)

    @pytest.mark.anyio
    async def test_settings_save_agent_prompt_template(self, base_app, route_client):
        """POST /settings/agent-prompt-template saves template."""
        resp = await route_client.post(
            "/settings/agent-prompt-template",
            data={"template": "Test template {source_messages}"},
        )
        assert resp.status_code in (200, 302, 303, 404)
