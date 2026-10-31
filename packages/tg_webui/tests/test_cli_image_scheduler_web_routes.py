"""Tests for CLI image, settings, analytics, scheduler, and related web routes.

Covers:
- CLI: image (generate/models/providers)
- CLI: settings (get/set/info)
- CLI: analytics (summary/pipeline-stats/daily/trending-topics/trending-channels/velocity/peak-hours/calendar)
- CLI: scheduler (status/stop/job-toggle/set-interval/task-cancel/clear-pending)
- Web: /images routes (page/generate/models/search)
- Web: /accounts routes (flood-status/flood-clear/toggle/delete)
- Web: /calendar routes (page/api/calendar/api/upcoming/api/stats)
"""
# Web-panel tests split out of tests/test_cli_image_scheduler_web_routes.py (#1720 wave 3).


from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tg_agent.collection_queue import CollectionQueue
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account, Channel
from tg_agent.scheduler.service import SchedulerManager
from tg_agent.search.ai_search import AISearchEngine
from tg_agent.search.engine import SearchEngine
from tg_agent.telegram.collector import Collector
from tg_shared.auth import TelegramAuth
from tg_webui.app import create_app

# ---------------------------------------------------------------------------
# Web fixtures (scoped to this file — mirrors tests/routes/conftest.py)
# ---------------------------------------------------------------------------


@pytest.fixture
async def base_app():
    """Minimal app with one account and channel for web route tests.

    Uses an in-memory database (``:memory:``) rather than a file under
    ``tmp_path``. These tests drive the FastAPI app entirely in-process over
    ASGITransport — there is no CLI subprocess that would need to share the DB
    on disk — so the file-backed DB bought nothing but I/O. Under ``-n auto``
    with many xdist workers the file-backed WAL connection (write + 4 read-pool
    connections per fixture instance) hit busy-contention on disk and raised
    ``DatabaseBusyError``; ``:memory:`` removes the disk entirely, killing the
    flake at its root (#1049, closes #1017). For ``:memory:`` the read pool
    shares the single write connection, so the app sees one consistent DB.
    """
    config = AppConfig()
    config.database.path = ":memory:"
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)
    db = Database(config.database.path)
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
    app.state.scheduler = SchedulerManager(config.scheduler)
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


# ---------------------------------------------------------------------------
# Web: /images routes
# ---------------------------------------------------------------------------


class TestWebImagesRoutes:
    """Tests for src/web/routes/images.py"""

    @pytest.mark.anyio
    async def test_images_page_renders(self, route_client):
        """GET /images/ returns 200."""
        with patch("tg_webui.images.handlers._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.adapter_names = []
            mock_svc_fn.return_value = svc
            resp = await route_client.get("/images/")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_generate_no_prompt(self, route_client):
        """POST /images/generate without prompt returns 400."""
        with patch("tg_webui.images.handlers._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/images/generate", data={"prompt": ""})
        assert resp.status_code == 400
        data = resp.json()
        assert data["ok"] is False

    @pytest.mark.anyio
    async def test_generate_no_providers(self, route_client):
        """POST /images/generate when no providers returns 409."""
        with patch("tg_webui.images.handlers._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=False)
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/images/generate", data={"prompt": "a cat"})
        assert resp.status_code == 409

    @pytest.mark.anyio
    async def test_generate_success(self, route_client):
        """POST /images/generate with valid prompt and provider returns 200 with url."""
        with patch("tg_webui.images.handlers._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value="https://example.com/img.png")
            mock_svc_fn.return_value = svc
            resp = await route_client.post(
                "/images/generate",
                data={"prompt": "a cat", "model": "replicate:flux"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "url" in data

    @pytest.mark.anyio
    async def test_generate_provider_failure(self, route_client):
        """POST /images/generate when generation fails returns 500."""
        with patch("tg_webui.images.handlers._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value=None)
            svc.last_failure = None
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/images/generate", data={"prompt": "a cat"})
        assert resp.status_code == 500

    @pytest.mark.anyio
    async def test_models_search_no_provider(self, route_client):
        """GET /images/models/search without provider returns 400."""
        resp = await route_client.get("/images/models/search")
        assert resp.status_code == 400

    @pytest.mark.anyio
    async def test_models_search_with_provider(self, route_client):
        """GET /images/models/search with provider returns model list."""
        with (
            patch("tg_webui.images.handlers._get_provider_api_key", new_callable=AsyncMock, return_value="key"),
            patch("tg_webui.images.handlers.ImageGenerationService") as mock_svc_cls,
        ):
            svc = MagicMock()
            svc.search_models = AsyncMock(return_value=[])
            mock_svc_cls.return_value = svc
            resp = await route_client.get("/images/models/search?provider=replicate&q=flux")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "models" in data


# ---------------------------------------------------------------------------
# Web: /accounts routes
# ---------------------------------------------------------------------------


class TestWebAccountsRoutes:
    """Tests for src/web/routes/accounts.py"""

    @pytest.mark.anyio
    async def test_flood_status_empty(self, route_client, base_app):
        """GET /settings/flood-status returns list of accounts."""
        resp = await route_client.get("/settings/flood-status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        assert "phone" in data[0]
        assert "flood_wait_until" in data[0]

    @pytest.mark.anyio
    async def test_flood_status_ok_field(self, route_client, base_app):
        """GET /settings/flood-status returns ok status for accounts with no flood."""
        resp = await route_client.get("/settings/flood-status")
        data = resp.json()
        acc = next((a for a in data if a["phone"] == "+1234567890"), None)
        assert acc is not None
        assert acc["flood_wait_until"] == "ok"
        assert acc["remaining_seconds"] == 0

    @pytest.mark.anyio
    async def test_flood_clear_not_found(self, route_client):
        """POST /settings/999/flood-clear with non-existent account redirects with error."""
        resp = await route_client.post("/settings/999/flood-clear", follow_redirects=False)
        assert resp.status_code in (303, 200)

    @pytest.mark.anyio
    async def test_toggle_account(self, route_client, base_app):
        """POST /settings/{id}/toggle redirects to settings."""
        _, db, _ = base_app
        accounts = await db.get_accounts()
        acc = accounts[0]
        resp = await route_client.post(f"/settings/{acc.id}/toggle", follow_redirects=False)
        assert resp.status_code == 303
        assert "settings" in resp.headers["location"]

    @pytest.mark.anyio
    async def test_delete_account(self, route_client, base_app):
        """POST /settings/{id}/delete redirects to settings."""
        _, db, _ = base_app
        accounts = await db.get_accounts()
        acc = accounts[0]
        resp = await route_client.post(f"/settings/{acc.id}/delete", follow_redirects=False)
        assert resp.status_code == 303
        assert "settings" in resp.headers["location"]

    @pytest.mark.anyio
    async def test_set_primary_account(self, route_client, base_app):
        """POST /settings/{id}/set-primary promotes the account and demotes the old primary."""
        _, db, _ = base_app
        await db.add_account(Account(phone="+1888888888", session_string="primary_session", is_primary=True))
        await db.add_account(Account(phone="+1999999999", session_string="second_session"))
        accounts = await db.get_account_summaries(active_only=False)
        old_primary = next(a for a in accounts if a.is_primary)
        target = next(a for a in accounts if not a.is_primary and a.id != old_primary.id)

        resp = await route_client.post(f"/settings/{target.id}/set-primary", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=account_set_primary" in resp.headers["location"]

        after = {a.id: a.is_primary for a in await db.get_account_summaries(active_only=False)}
        assert after[target.id] is True
        assert after[old_primary.id] is False
        assert sum(1 for v in after.values() if v) == 1

    @pytest.mark.anyio
    async def test_set_primary_account_not_found(self, route_client):
        """POST /settings/999/set-primary for a missing account redirects with an error."""
        resp = await route_client.post("/settings/999/set-primary", follow_redirects=False)
        assert resp.status_code == 303
        assert "error=invalid_account" in resp.headers["location"]


# ---------------------------------------------------------------------------
# Web: /calendar routes
# ---------------------------------------------------------------------------


class TestWebCalendarRoutes:
    """Tests for src/web/routes/calendar.py"""

    @pytest.mark.anyio
    async def test_calendar_page_renders(self, route_client):
        """GET /calendar/ returns 200."""
        resp = await route_client.get("/calendar/")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_calendar_page_with_days_param(self, route_client):
        """GET /calendar/?days=14 returns 200."""
        resp = await route_client.get("/calendar/?days=14")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_calendar_page_invalid_days(self, route_client):
        """GET /calendar/?days=0 returns 422 (out of range)."""
        resp = await route_client.get("/calendar/?days=0")
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_api_calendar_returns_json(self, route_client):
        """GET /calendar/api/calendar returns JSON list."""
        resp = await route_client.get("/calendar/api/calendar")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.anyio
    async def test_api_calendar_with_days(self, route_client):
        """GET /calendar/api/calendar?days=3 returns JSON."""
        resp = await route_client.get("/calendar/api/calendar?days=3")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.anyio
    async def test_api_upcoming_returns_json(self, route_client):
        """GET /calendar/api/upcoming returns JSON list."""
        resp = await route_client.get("/calendar/api/upcoming")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.anyio
    async def test_api_upcoming_with_limit(self, route_client):
        """GET /calendar/api/upcoming?limit=5 returns list."""
        resp = await route_client.get("/calendar/api/upcoming?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.anyio
    async def test_api_stats_returns_json(self, route_client):
        """GET /calendar/api/stats returns JSON dict."""
        resp = await route_client.get("/calendar/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    @pytest.mark.anyio
    async def test_api_calendar_pipeline_filter(self, route_client):
        """GET /calendar/api/calendar?pipeline_id=999 returns empty list."""
        resp = await route_client.get("/calendar/api/calendar?pipeline_id=999")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


# ---------------------------------------------------------------------------
# Regression: web-route fixture isolation (#1049, closes #1017)
# ---------------------------------------------------------------------------


class TestBaseAppDbIsolation:
    """Guards the fix for the ``DatabaseBusyError`` flake under ``-n auto`` (#1049).

    The ``base_app`` fixture used to back the app with a file-backed SQLite DB
    under ``tmp_path``. Each fixture instance opens a WAL write connection plus a
    read pool; under many xdist workers the on-disk WAL hit busy-contention and
    raised ``DatabaseBusyError`` (root cause diagnosed in #1017). These web-route
    tests run the FastAPI app purely in-process over ASGITransport — no CLI
    subprocess needs the DB on disk — so an in-memory DB removes the disk I/O that
    caused the contention while behaving identically (for ``:memory:`` the read
    pool shares the single write connection). This test pins that invariant so a
    future edit can't silently reintroduce the file-backed DB and the flake.
    """

    @pytest.mark.anyio
    async def test_base_app_uses_in_memory_db(self, base_app, tmp_path):
        """``base_app`` must back the app with ``:memory:``, never a disk file."""
        app, db, _pool = base_app

        # The DB the app actually serves is in-memory — no disk contention path.
        assert db._db_path == ":memory:"
        assert app.state.db is db

        # And nothing wrote a SQLite file into tmp_path (no stray on-disk DB).
        assert not list(tmp_path.glob("*.db")), (
            "base_app must not create a file-backed SQLite DB (would reintroduce "
            "the -n auto DatabaseBusyError flake; #1049/#1017)"
        )
