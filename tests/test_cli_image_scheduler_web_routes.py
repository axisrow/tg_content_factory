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

from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app
from tests.helpers import cli_ns as _ns

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_ns(**kwargs):
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return _ns(**defaults)


# ---------------------------------------------------------------------------
# CLI: image command
# ---------------------------------------------------------------------------


class TestCLIImageCommand:
    """Tests for src/cli/commands/image.py"""

    def test_providers_none_configured(self, capsys):
        """providers action with no adapters prints help message."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.adapter_names = []
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="providers"))
            out = capsys.readouterr().out
            assert "No providers configured" in out

    def test_providers_lists_names(self, capsys):
        """providers action prints each adapter name."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.adapter_names = ["replicate", "together"]
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="providers"))
            out = capsys.readouterr().out
            assert "replicate" in out
            assert "together" in out

    def test_generate_no_providers(self, capsys):
        """generate action when no providers configured prints warning."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=False)
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model=None, prompt="a cat"))
            out = capsys.readouterr().out
            assert "No image providers configured" in out

    def test_generate_success(self, capsys):
        """generate action prints result URL on success."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value="https://example.com/img.png")
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model="replicate:flux", prompt="a cat"))
            out = capsys.readouterr().out
            assert "https://example.com/img.png" in out

    def test_generate_failure(self, capsys):
        """generate action prints failure message when result is None."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value=None)
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model=None, prompt="a cat"))
            out = capsys.readouterr().out
            assert "Generation failed" in out

    def test_models_no_results(self, capsys):
        """models action with no results prints message."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.search_models = AsyncMock(return_value=[])
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="models", provider="replicate", query=""))
            out = capsys.readouterr().out
            assert "No models found" in out

    def test_models_with_results(self, capsys):
        """models action prints model strings and descriptions."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.search_models = AsyncMock(return_value=[
                {"model_string": "replicate:flux", "run_count": 1000, "description": "Fast model"},
                {"model_string": "replicate:sdxl", "run_count": None, "description": ""},
            ])
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="models", provider="replicate", query="flux"))
            out = capsys.readouterr().out
            assert "replicate:flux" in out
            assert "Fast model" in out

    def test_unknown_action(self, capsys):
        """Unknown action prints usage help."""
        from src.cli.commands.image import run

        with patch("src.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="unknown"))
            out = capsys.readouterr().out
            assert "Usage" in out


# ---------------------------------------------------------------------------
# CLI: settings command
# ---------------------------------------------------------------------------


class TestCLISettingsCommand:
    """Tests for src/cli/commands/settings.py"""

    def test_get_all_empty(self, cli_env, capsys):
        """get action with no key lists whatever settings exist (at least header)."""
        from src.cli.commands.settings import run

        run(_ns(settings_action="get", key=None))
        out = capsys.readouterr().out
        # Either shows "No settings found" or a table header — either way no crash
        assert out.strip() != ""

    def test_get_all_with_data(self, cli_env, capsys):
        """get action with no key lists all settings."""
        asyncio.run(cli_env.set_setting("test_key", "test_value"))

        from src.cli.commands.settings import run

        run(_ns(settings_action="get", key=None))
        out = capsys.readouterr().out
        assert "test_key" in out
        assert "test_value" in out

    def test_get_specific_key(self, cli_env, capsys):
        """get action with specific key prints value."""
        asyncio.run(cli_env.set_setting("my_key", "my_value"))

        from src.cli.commands.settings import run

        run(_ns(settings_action="get", key="my_key"))
        out = capsys.readouterr().out
        assert "my_key" in out
        assert "my_value" in out

    def test_get_missing_key(self, cli_env, capsys):
        """get action with missing key prints '(not set)'."""
        from src.cli.commands.settings import run

        run(_ns(settings_action="get", key="nonexistent_key"))
        out = capsys.readouterr().out
        assert "(not set)" in out

    def test_set_setting(self, cli_env, capsys):
        """set action prints confirmation (DB access happens inside the CLI run)."""
        from src.cli.commands.settings import run

        run(_ns(settings_action="set", key="foo", value="bar"))
        out = capsys.readouterr().out
        assert "Set foo = bar" in out

    def test_info_action(self, cli_env, capsys):
        """info action prints system information."""
        from src.cli.commands.settings import run

        run(_ns(settings_action="info"))
        out = capsys.readouterr().out
        assert "System information" in out


# ---------------------------------------------------------------------------
# CLI: analytics extended commands
# ---------------------------------------------------------------------------


class TestCLIAnalyticsExtended:
    """Tests for analytics sub-commands not covered by test_cli_analytics.py"""

    def test_summary_empty(self, cli_env, capsys):
        """summary action works with empty DB."""
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="summary", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "Total generations" in out

    def test_pipeline_stats_empty(self, cli_env, capsys):
        """pipeline-stats action with no data prints message."""
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="pipeline-stats", date_from=None, date_to=None, pipeline_id=None))
        out = capsys.readouterr().out
        assert "No pipeline stats found" in out

    def test_daily_empty(self, cli_env, capsys):
        """daily action with no data prints 'No data'."""
        from src.cli.commands.analytics import run

        _target = "src.services.content_analytics_service.ContentAnalyticsService.get_daily_stats"
        with patch(_target, new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="daily", date_from=None, date_to=None, days=30, pipeline_id=None))
        out = capsys.readouterr().out
        assert "No data" in out

    def test_trending_topics_empty(self, cli_env, capsys):
        """trending-topics action with no data prints message."""
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="trending-topics", date_from=None, date_to=None, days=7, limit=20))
        out = capsys.readouterr().out
        assert "No trending topics found" in out

    def test_trending_channels_empty(self, cli_env, capsys):
        """trending-channels action with no data prints message."""
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="trending-channels", date_from=None, date_to=None, days=7, limit=20))
        out = capsys.readouterr().out
        assert "No channel data found" in out

    def test_velocity_empty(self, cli_env, capsys):
        """velocity action with no data prints message."""
        from src.cli.commands.analytics import run

        with patch("src.services.trend_service.TrendService.get_message_velocity", new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="velocity", date_from=None, date_to=None, days=30))
        out = capsys.readouterr().out
        assert "No velocity data found" in out

    def test_peak_hours_empty(self, cli_env, capsys):
        """peak-hours action with no data prints message."""
        from src.cli.commands.analytics import run

        with patch("src.services.trend_service.TrendService.get_peak_hours", new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="peak-hours", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "No peak hours data found" in out

    def test_calendar_empty(self, cli_env, capsys):
        """calendar action with no upcoming publications prints message."""
        from src.cli.commands.analytics import run

        run(_ns(analytics_action="calendar", date_from=None, date_to=None, limit=20, pipeline_id=None))
        out = capsys.readouterr().out
        assert "No upcoming publications" in out


# ---------------------------------------------------------------------------
# CLI: scheduler command (actions that don't need a real pool)
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_env_scheduler(cli_env):
    """Patch runtime.init_pool to return an empty fake pool."""
    fake_pool = MagicMock()
    fake_pool.clients = {}
    fake_pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), fake_pool

    with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield cli_env, fake_pool


class TestCLISchedulerCommand:
    """Tests for scheduler sub-commands (status/stop/job-toggle/set-interval/task-cancel/clear-pending)."""

    def test_status_prints_interval(self, cli_env_scheduler, capsys):
        """status action prints interval and autostart."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="status"))
        out = capsys.readouterr().out
        assert "Interval" in out

    def test_stop_disables_autostart(self, cli_env_scheduler, capsys):
        """stop action prints autostart-disabled message."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="stop"))
        out = capsys.readouterr().out
        assert "Scheduler autostart disabled" in out

    def test_job_toggle_disables(self, cli_env_scheduler, capsys):
        """job-toggle disables an enabled job."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="job-toggle", job_id="collect_all"))
        out = capsys.readouterr().out
        assert "collect_all" in out

    def test_job_toggle_enables(self, cli_env_scheduler, capsys):
        """job-toggle re-enables a disabled job."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}
        asyncio.run(cli_env.repos.settings.set_setting("scheduler_job_disabled:collect_all", "1"))

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="job-toggle", job_id="collect_all"))
        out = capsys.readouterr().out
        assert "enabled" in out

    def test_set_interval_collect_all(self, cli_env_scheduler, capsys):
        """set-interval for collect_all prints confirmation."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="set-interval", job_id="collect_all", minutes=45))
        out = capsys.readouterr().out
        assert "45" in out

    def test_set_interval_clamps_min(self, cli_env_scheduler, capsys):
        """set-interval clamps below-minimum value to 1."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="set-interval", job_id="some_job", minutes=0))
        out = capsys.readouterr().out
        assert "1 min" in out

    def test_task_cancel_not_found(self, cli_env_scheduler, capsys):
        """task-cancel on missing task prints not-found message."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="task-cancel", task_id=99999))
        out = capsys.readouterr().out
        assert "not found" in out or "Task 99999" in out

    def test_clear_pending(self, cli_env_scheduler, capsys):
        """clear-pending prints deleted count."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="clear-pending"))
        out = capsys.readouterr().out
        assert "Cleared" in out


# ---------------------------------------------------------------------------
# Web fixtures (scoped to this file — mirrors tests/routes/conftest.py)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="function")
async def base_app(tmp_path):
    """Minimal app with one account and channel for web route tests."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
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


@pytest_asyncio.fixture(loop_scope="function")
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

    @pytest.mark.asyncio
    async def test_images_page_renders(self, route_client):
        """GET /images/ returns 200."""
        with patch("src.web.routes.images._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.adapter_names = []
            mock_svc_fn.return_value = svc
            resp = await route_client.get("/images/")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_generate_no_prompt(self, route_client):
        """POST /images/generate without prompt returns 400."""
        with patch("src.web.routes.images._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/images/generate", data={"prompt": ""})
        assert resp.status_code == 400
        data = resp.json()
        assert data["ok"] is False

    @pytest.mark.asyncio
    async def test_generate_no_providers(self, route_client):
        """POST /images/generate when no providers returns 409."""
        with patch("src.web.routes.images._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=False)
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/images/generate", data={"prompt": "a cat"})
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_generate_success(self, route_client):
        """POST /images/generate with valid prompt and provider returns 200 with url."""
        with patch("src.web.routes.images._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
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

    @pytest.mark.asyncio
    async def test_generate_provider_failure(self, route_client):
        """POST /images/generate when generation fails returns 500."""
        with patch("src.web.routes.images._get_image_service", new_callable=AsyncMock) as mock_svc_fn:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value=None)
            mock_svc_fn.return_value = svc
            resp = await route_client.post("/images/generate", data={"prompt": "a cat"})
        assert resp.status_code == 500

    @pytest.mark.asyncio
    async def test_models_search_no_provider(self, route_client):
        """GET /images/models/search without provider returns 400."""
        resp = await route_client.get("/images/models/search")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_models_search_with_provider(self, route_client):
        """GET /images/models/search with provider returns model list."""
        with (
            patch("src.web.routes.images._get_provider_api_key", new_callable=AsyncMock, return_value="key"),
            patch("src.web.routes.images.ImageGenerationService") as mock_svc_cls,
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

    @pytest.mark.asyncio
    async def test_flood_status_empty(self, route_client, base_app):
        """GET /settings/flood-status returns list of accounts."""
        resp = await route_client.get("/settings/flood-status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        assert "phone" in data[0]
        assert "flood_wait_until" in data[0]

    @pytest.mark.asyncio
    async def test_flood_status_ok_field(self, route_client, base_app):
        """GET /settings/flood-status returns ok status for accounts with no flood."""
        resp = await route_client.get("/settings/flood-status")
        data = resp.json()
        acc = next((a for a in data if a["phone"] == "+1234567890"), None)
        assert acc is not None
        assert acc["flood_wait_until"] == "ok"
        assert acc["remaining_seconds"] == 0

    @pytest.mark.asyncio
    async def test_flood_clear_not_found(self, route_client):
        """POST /settings/999/flood-clear with non-existent account redirects with error."""
        resp = await route_client.post("/settings/999/flood-clear", follow_redirects=False)
        assert resp.status_code in (303, 200)

    @pytest.mark.asyncio
    async def test_toggle_account(self, route_client, base_app):
        """POST /settings/{id}/toggle redirects to settings."""
        _, db, _ = base_app
        accounts = await db.get_accounts()
        acc = accounts[0]
        resp = await route_client.post(f"/settings/{acc.id}/toggle", follow_redirects=False)
        assert resp.status_code == 303
        assert "settings" in resp.headers["location"]

    @pytest.mark.asyncio
    async def test_delete_account(self, route_client, base_app):
        """POST /settings/{id}/delete redirects to settings."""
        _, db, _ = base_app
        accounts = await db.get_accounts()
        acc = accounts[0]
        resp = await route_client.post(f"/settings/{acc.id}/delete", follow_redirects=False)
        assert resp.status_code == 303
        assert "settings" in resp.headers["location"]


# ---------------------------------------------------------------------------
# Web: /calendar routes
# ---------------------------------------------------------------------------


class TestWebCalendarRoutes:
    """Tests for src/web/routes/calendar.py"""

    @pytest.mark.asyncio
    async def test_calendar_page_renders(self, route_client):
        """GET /calendar/ returns 200."""
        resp = await route_client.get("/calendar/")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_calendar_page_with_days_param(self, route_client):
        """GET /calendar/?days=14 returns 200."""
        resp = await route_client.get("/calendar/?days=14")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_calendar_page_invalid_days(self, route_client):
        """GET /calendar/?days=0 returns 422 (out of range)."""
        resp = await route_client.get("/calendar/?days=0")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_api_calendar_returns_json(self, route_client):
        """GET /calendar/api/calendar returns JSON list."""
        resp = await route_client.get("/calendar/api/calendar")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_api_calendar_with_days(self, route_client):
        """GET /calendar/api/calendar?days=3 returns JSON."""
        resp = await route_client.get("/calendar/api/calendar?days=3")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_api_upcoming_returns_json(self, route_client):
        """GET /calendar/api/upcoming returns JSON list."""
        resp = await route_client.get("/calendar/api/upcoming")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_api_upcoming_with_limit(self, route_client):
        """GET /calendar/api/upcoming?limit=5 returns list."""
        resp = await route_client.get("/calendar/api/upcoming?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_api_stats_returns_json(self, route_client):
        """GET /calendar/api/stats returns JSON dict."""
        resp = await route_client.get("/calendar/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    @pytest.mark.asyncio
    async def test_api_calendar_pipeline_filter(self, route_client):
        """GET /calendar/api/calendar?pipeline_id=999 returns empty list."""
        resp = await route_client.get("/calendar/api/calendar?pipeline_id=999")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
