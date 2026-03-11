"""Integration tests for the TG Post Search application.

Tests cover full request cycles through the web layer including
auth, CRUD for channels/keywords, search, scheduler, and collector logic.
"""

from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.cli.runtime import init_pool
from src.config import AppConfig, SchedulerConfig, load_config
from src.database import Database
from src.models import Account, Channel, Message, SearchQuery
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.web.app import create_app
from tests.helpers import make_mock_pool as _make_pool

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def app_with_db(tmp_path):
    """Create a full app with initialized DB, yielding (app, db)."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    async def _no_users(self):
        return []

    async def _resolve_channel(self, identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Resolved Channel",
            "username": identifier.lstrip("@"),
        }

    async def _get_dialogs(self):
        return []

    async def _get_dialogs_for_phone(self, phone, include_dm=False, mode="channels_only"):
        return []

    pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _resolve_channel,
            "get_dialogs": _get_dialogs,
            "get_dialogs_for_phone": _get_dialogs_for_phone,
        },
    )()
    app.state.pool = pool
    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None

    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(collector, config.scheduler)

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))

    yield app, db

    await db.close()


@pytest.fixture
async def auth_client(app_with_db):
    """Authenticated HTTP client."""
    app, _ = app_with_db
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c


@pytest.fixture
async def noauth_client(app_with_db):
    """Unauthenticated HTTP client (shares the same app)."""
    app, _ = app_with_db
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
    ) as c:
        yield c


@pytest.fixture
async def test_db(app_with_db):
    """Shortcut to the test database."""
    _, db = app_with_db
    return db


# ===================================================================
# Priority 1 — Critical
# ===================================================================


class TestAuthFlow:
    """Full authentication cycle: no auth → 401, with auth → 200, health → 200."""

    @pytest.mark.asyncio
    async def test_no_auth_returns_401(self, noauth_client):
        resp = await noauth_client.get("/", follow_redirects=False)
        assert resp.status_code == 401
        assert "WWW-Authenticate" in resp.headers

    @pytest.mark.asyncio
    async def test_with_auth_returns_200(self, auth_client):
        resp = await auth_client.get("/")
        assert resp.status_code == 200
        assert resp.url.path == "/agent"

    @pytest.mark.asyncio
    async def test_health_no_auth_returns_200(self, noauth_client):
        resp = await noauth_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("healthy", "degraded")
        assert "db" in data

    @pytest.mark.asyncio
    async def test_wrong_credentials_returns_401(self, app_with_db):
        app, _ = app_with_db
        transport = ASGITransport(app=app)
        bad_auth = base64.b64encode(b"wrong:creds").decode()
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Basic {bad_auth}"},
        ) as c:
            resp = await c.get("/")
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_all_protected_pages_require_auth(self, noauth_client):
        for path in [
            "/settings/", "/channels/", "/dashboard/",
            "/scheduler/", "/search-queries/", "/my-telegram/", "/debug/",
        ]:
            resp = await noauth_client.get(path, follow_redirects=False)
            assert resp.status_code == 401, f"{path} should require auth"


class TestChannelCRUD:
    """Add channel → list → toggle active → delete."""

    @pytest.mark.asyncio
    async def test_full_channel_lifecycle(self, auth_client, test_db):
        # Add channel via identifier (resolve_channel is mocked in fixture)
        resp = await auth_client.post(
            "/channels/add",
            data={"identifier": "@test_channel"},
        )
        assert resp.status_code == 200  # redirected + followed

        # Verify in DB
        channels = await test_db.get_channels()
        assert len(channels) == 1
        ch = channels[0]
        assert ch.channel_id == -1001234567890
        assert ch.title == "Resolved Channel"
        assert ch.is_active is True

        # Toggle active (deactivate)
        resp = await auth_client.post(f"/channels/{ch.id}/toggle")
        assert resp.status_code == 200
        channels = await test_db.get_channels()
        assert channels[0].is_active is False

        # Toggle again (reactivate)
        resp = await auth_client.post(f"/channels/{ch.id}/toggle")
        assert resp.status_code == 200
        channels = await test_db.get_channels()
        assert channels[0].is_active is True

        # Delete
        resp = await auth_client.post(f"/channels/{ch.id}/delete")
        assert resp.status_code == 200
        channels = await test_db.get_channels()
        assert len(channels) == 0

    @pytest.mark.asyncio
    async def test_add_channel_resolve_fail(self, auth_client, app_with_db, test_db):
        app, _ = app_with_db

        async def _fail_resolve(self, identifier):
            raise ValueError("not found")

        original = app.state.pool.resolve_channel
        app.state.pool.resolve_channel = _fail_resolve

        resp = await auth_client.post(
            "/channels/add",
            data={"identifier": "@nonexistent"},
        )
        assert resp.status_code == 200
        assert "error=resolve" in str(resp.url) or "Не удалось найти" in resp.text

        channels = await test_db.get_channels()
        assert len(channels) == 0

        app.state.pool.resolve_channel = original

    @pytest.mark.asyncio
    async def test_channels_page_lists_channels(self, auth_client, test_db):
        ch = Channel(channel_id=-100999, title="Visible Channel")
        await test_db.add_channel(ch)

        resp = await auth_client.get("/channels/")
        assert resp.status_code == 200
        assert "Visible Channel" in resp.text


class TestSearchQueryCRUD:
    """Add search query → list → delete."""

    @pytest.mark.asyncio
    async def test_add_plain_query(self, auth_client, test_db):
        resp = await auth_client.post(
            "/search-queries/add",
            data={
                "query": "bitcoin",
                "interval_minutes": "60",
                "track_stats": "true",
            },
        )
        assert resp.status_code == 200

        repo = test_db.repos.search_queries
        queries = await repo.get_all()
        assert len(queries) == 1
        assert queries[0].query == "bitcoin"
        assert queries[0].is_regex is False

    @pytest.mark.asyncio
    async def test_add_regex_query(self, auth_client, test_db):
        resp = await auth_client.post(
            "/search-queries/add",
            data={
                "query": r"BTC|ETH",
                "interval_minutes": "60",
                "is_regex": "true",
                "track_stats": "true",
            },
        )
        assert resp.status_code == 200

        repo = test_db.repos.search_queries
        queries = await repo.get_all()
        assert len(queries) == 1
        assert queries[0].is_regex is True

    @pytest.mark.asyncio
    async def test_delete_query(self, auth_client, test_db):
        repo = test_db.repos.search_queries
        sq = SearchQuery(query="delete_me")
        sq_id = await repo.add(sq)

        resp = await auth_client.post(f"/search-queries/{sq_id}/delete")
        assert resp.status_code == 200

        queries = await repo.get_all()
        assert len(queries) == 0

    @pytest.mark.asyncio
    async def test_query_appears_on_page(self, auth_client, test_db):
        repo = test_db.repos.search_queries
        await repo.add(SearchQuery(query="ethereum"))

        resp = await auth_client.get("/search-queries/")
        assert "ethereum" in resp.text


class TestSearchModes:
    """Search local with results and without."""

    @pytest.mark.asyncio
    async def test_search_local_no_results(self, auth_client):
        resp = await auth_client.get("/search?q=nonexistent&mode=local")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_search_local_with_results(self, auth_client, test_db):
        msgs = [
            Message(
                channel_id=-100123,
                message_id=1,
                text="Bitcoin price hits 100k",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=2,
                text="Weather forecast sunny",
                date=datetime.now(timezone.utc),
            ),
        ]
        await test_db.insert_messages_batch(msgs)

        resp = await auth_client.get("/search?q=Bitcoin&mode=local")
        assert resp.status_code == 200
        assert "Bitcoin" in resp.text

    @pytest.mark.asyncio
    async def test_search_empty_query_shows_form(self, auth_client):
        resp = await auth_client.get("/search")
        assert resp.status_code == 200
        assert "Поиск" in resp.text


# ===================================================================
# Priority 2 — Important
# ===================================================================


class TestConfigEnvChain:
    """Full chain: .env → config.yaml → AppConfig."""

    def test_env_substitution_full_chain(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TG_API_ID", "99999")
        monkeypatch.setenv("TG_API_HASH", "abc123hash")
        monkeypatch.setenv("WEB_PASS", "secret123")

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
            "web:\n"
            "  password: ${WEB_PASS}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 99999
        assert config.telegram.api_hash == "abc123hash"
        assert config.web.password == "secret123"

    def test_empty_env_drops_keys_uses_defaults(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TG_API_ID", raising=False)
        monkeypatch.delenv("TG_API_HASH", raising=False)
        monkeypatch.delenv("WEB_PASS", raising=False)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
            "web:\n"
            "  password: ${WEB_PASS}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 0
        assert config.telegram.api_hash == ""
        assert config.web.password == ""

    def test_partial_env_mix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TG_API_ID", "11111")
        monkeypatch.delenv("TG_API_HASH", raising=False)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "telegram:\n"
            "  api_id: ${TG_API_ID}\n"
            "  api_hash: ${TG_API_HASH}\n"
        )
        config = load_config(config_file)

        assert config.telegram.api_id == 11111
        assert config.telegram.api_hash == ""


class TestSchedulerStartStop:
    """Start/stop scheduler through web routes."""

    @pytest.mark.asyncio
    async def test_scheduler_page_shows_status(self, auth_client):
        resp = await auth_client.get("/scheduler/")
        assert resp.status_code == 200
        assert "Остановлен" in resp.text

    @pytest.mark.asyncio
    async def test_start_and_stop_scheduler(self, auth_client, app_with_db):
        app, _ = app_with_db
        sched = app.state.scheduler

        assert sched.is_running is False

        # Start
        resp = await auth_client.post("/scheduler/start")
        assert resp.status_code == 200
        assert sched.is_running is True

        # Stop
        resp = await auth_client.post("/scheduler/stop")
        assert resp.status_code == 200
        assert sched.is_running is False

    @pytest.mark.asyncio
    async def test_double_start_is_safe(self, auth_client, app_with_db):
        app, _ = app_with_db
        sched = app.state.scheduler

        await auth_client.post("/scheduler/start")
        await auth_client.post("/scheduler/start")  # should not crash
        assert sched.is_running is True

        await auth_client.post("/scheduler/stop")

    @pytest.mark.asyncio
    async def test_trigger_returns_immediately(self, app_with_db):
        """POST /scheduler/trigger returns redirect with ?msg=triggered instantly."""
        app, _ = app_with_db
        scheduler = None

        class BlockingCollector:
            def __init__(self):
                self.started_event = asyncio.Event()
                self.release_event = asyncio.Event()
                self._running = False

            @property
            def is_running(self):
                return self._running

            async def collect_all_channels(self):
                self._running = True
                self.started_event.set()
                try:
                    await self.release_event.wait()
                    return {"channels": 0, "messages": 0, "errors": 0}
                finally:
                    self._running = False

        collector = BlockingCollector()
        scheduler = SchedulerManager(collector, SchedulerConfig())
        app.state.collector = collector
        app.state.scheduler = scheduler

        transport = ASGITransport(app=app)
        auth_header = base64.b64encode(b":testpass").decode()
        try:
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
                follow_redirects=False,
                headers={"Authorization": f"Basic {auth_header}"},
            ) as c:
                resp = await c.post("/scheduler/trigger")
                assert resp.status_code == 303
                assert "msg=triggered" in resp.headers["location"]

            await asyncio.wait_for(collector.started_event.wait(), timeout=1.0)
            assert scheduler._bg_task is not None
            assert not scheduler._bg_task.done()
            assert collector.is_running is True
        finally:
            collector.release_event.set()
            if scheduler is not None:
                await scheduler.stop()

        assert scheduler._bg_task is None
        assert collector.is_running is False

    @pytest.mark.asyncio
    async def test_trigger_while_collecting(self, app_with_db):
        """If collection is already running, redirect with ?msg=already_running."""
        app, _ = app_with_db
        collector = app.state.collector

        transport = ASGITransport(app=app)
        auth_header = base64.b64encode(b":testpass").decode()
        with patch.object(
            type(collector), "is_running",
            new_callable=PropertyMock, return_value=True,
        ):
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
                follow_redirects=False,
                headers={"Authorization": f"Basic {auth_header}"},
            ) as c:
                resp = await c.post("/scheduler/trigger")
                assert resp.status_code == 303
                assert "msg=already_running" in resp.headers["location"]


class TestCollectorIncremental:
    """Collector passes min_id from DB to iter_messages on incremental run."""

    @pytest.mark.asyncio
    async def test_incremental_passes_min_id(self, test_db):
        from tests.helpers import AsyncIterEmpty as _AsyncIterEmpty

        ch = Channel(
            channel_id=-100500, title="Incremental Test",
            username="inc_test",
        )
        await test_db.add_channel(ch)
        await test_db.update_channel_last_id(-100500, 42)

        mock_client = AsyncMock()
        mock_client.get_dialogs = AsyncMock(return_value=[])
        mock_client.get_entity = AsyncMock(return_value=MagicMock())
        mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

        pool = _make_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

        config = SchedulerConfig(delay_between_requests_sec=0)
        collector = Collector(pool, test_db, config)
        await collector.collect_all_channels()

        call_kwargs = mock_client.iter_messages.call_args[1]
        assert call_kwargs["min_id"] == 42


class TestSearchPagination:
    """Search with pagination (page=1, page=2)."""

    @pytest.mark.asyncio
    async def test_paginated_search(self, auth_client, test_db):
        # Insert 60 messages containing "crypto"
        msgs = [
            Message(
                channel_id=-100123,
                message_id=i,
                text=f"Crypto news item {i}",
                date=datetime.now(timezone.utc),
            )
            for i in range(1, 61)
        ]
        await test_db.insert_messages_batch(msgs)

        # Page 1 (limit=50 by default)
        resp1 = await auth_client.get("/search?q=Crypto&mode=local&page=1")
        assert resp1.status_code == 200
        assert "Crypto" in resp1.text

        # Page 2
        resp2 = await auth_client.get("/search?q=Crypto&mode=local&page=2")
        assert resp2.status_code == 200

    @pytest.mark.asyncio
    async def test_search_engine_pagination_offsets(self, test_db):
        msgs = [
            Message(
                channel_id=-100123,
                message_id=i,
                text=f"Result {i}",
                date=datetime.now(timezone.utc),
            )
            for i in range(1, 21)
        ]
        await test_db.insert_messages_batch(msgs)

        engine = SearchEngine(test_db)

        page1 = await engine.search_local("Result", limit=5, offset=0)
        page2 = await engine.search_local("Result", limit=5, offset=5)

        assert len(page1.messages) == 5
        assert len(page2.messages) == 5
        assert page1.total == 20
        assert page2.total == 20

        # Pages should contain different messages
        ids_page1 = {m.message_id for m in page1.messages}
        ids_page2 = {m.message_id for m in page2.messages}
        assert ids_page1.isdisjoint(ids_page2)


# ===================================================================
# Priority 3 — Edge Cases
# ===================================================================


class TestFloodWaitRotation:
    """ClientPool switches to another account on FloodWaitError."""

    @pytest.mark.asyncio
    async def test_report_flood_marks_account(self, test_db):
        from src.models import Account

        acc1 = Account(phone="+70001111111", session_string="s1", is_primary=True)
        acc2 = Account(phone="+70002222222", session_string="s2")
        await test_db.add_account(acc1)
        await test_db.add_account(acc2)

        auth = MagicMock()
        pool = ClientPool(auth, test_db)
        await pool.report_flood("+70001111111", 120)

        accounts = await test_db.get_accounts()
        flooded = [a for a in accounts if a.phone == "+70001111111"]
        assert flooded[0].flood_wait_until is not None

        healthy = [a for a in accounts if a.phone == "+70002222222"]
        assert healthy[0].flood_wait_until is None

    @pytest.mark.asyncio
    async def test_collector_handles_flood_with_rotation(self, test_db):
        """FloodWaitError on client1 → report_flood → retry with client2."""
        from telethon.errors import FloodWaitError

        from tests.helpers import AsyncIterEmpty as _AsyncIterEmpty

        ch = Channel(channel_id=-100777, title="Flood Test", username="flood_test")
        await test_db.add_channel(ch)

        flood_err = FloodWaitError(request=None, capture=0)
        flood_err.seconds = 60  # ≤ max_flood_wait_sec=300 → retry branch

        client1 = AsyncMock()
        client1.get_entity = AsyncMock(side_effect=flood_err)

        client2 = AsyncMock()
        client2.get_entity = AsyncMock(return_value=MagicMock())
        client2.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

        pool = _make_pool(
            get_available_client=AsyncMock(
                side_effect=[
                    (client1, "+70001111111"),  # prefetch dialogs
                    (client1, "+70001111111"),  # first _collect_channel call
                    (client2, "+70002222222"),  # retry after flood
                ]
            ),
        )

        config = SchedulerConfig(max_flood_wait_sec=300, delay_between_requests_sec=0)
        collector = Collector(pool, test_db, config)
        await collector.collect_all_channels()

        pool.report_flood.assert_awaited_with("+70001111111", 60)
        client2.get_entity.assert_awaited()


class TestNotificationQueryMatch:
    """Notification query patterns match correctly during collection check."""

    @pytest.mark.asyncio
    async def test_plain_query_case_insensitive(self, test_db):
        repo = test_db.repos.search_queries
        await repo.add(SearchQuery(
            name="bitcoin", query="bitcoin", notify_on_collect=True,
        ))

        notifier = AsyncMock()
        notifier.notify = AsyncMock()

        pool = _make_pool()
        config = SchedulerConfig()
        collector = Collector(pool, test_db, config, notifier)

        msgs = [
            Message(
                channel_id=-100123,
                message_id=1,
                text="BITCOIN price is rising fast!",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=2,
                text="Weather is nice today",
                date=datetime.now(timezone.utc),
            ),
        ]
        await collector._check_notification_queries(msgs)

        # Batched: one notification per query per run
        assert notifier.notify.await_count == 1
        call_text = notifier.notify.call_args[0][0]
        assert "BITCOIN" in call_text

    @pytest.mark.asyncio
    async def test_regex_query_matches(self, test_db):
        repo = test_db.repos.search_queries
        await repo.add(SearchQuery(
            name="crypto", query=r"\b(BTC|ETH)\b",
            is_regex=True, notify_on_collect=True,
        ))

        notifier = AsyncMock()
        notifier.notify = AsyncMock()

        pool = _make_pool()
        config = SchedulerConfig()
        collector = Collector(pool, test_db, config, notifier)

        msgs = [
            Message(
                channel_id=-100123,
                message_id=1,
                text="BTC just hit 100k",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=2,
                text="ETH staking rewards update",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=3,
                text="Nice weather outside",
                date=datetime.now(timezone.utc),
            ),
        ]
        await collector._check_notification_queries(msgs)

        # Batched: one notification per query (2 matches → single notification with count)
        assert notifier.notify.await_count == 1
        call_text = notifier.notify.call_args[0][0]
        assert "2 times" in call_text

    @pytest.mark.asyncio
    async def test_invalid_regex_does_not_crash(self, test_db):
        repo = test_db.repos.search_queries
        await repo.add(SearchQuery(
            name="bad", query=r"[invalid(",
            is_regex=True, notify_on_collect=True,
        ))

        notifier = AsyncMock()
        notifier.notify = AsyncMock()

        pool = _make_pool()
        config = SchedulerConfig()
        collector = Collector(pool, test_db, config, notifier)

        msgs = [
            Message(
                channel_id=-100123,
                message_id=1,
                text="Some text",
                date=datetime.now(timezone.utc),
            ),
        ]
        await collector._check_notification_queries(msgs)
        assert notifier.notify.await_count == 0


class TestGracefulShutdown:
    """SchedulerManager cancels background task on stop."""

    @pytest.mark.asyncio
    async def test_trigger_task_cancelled_on_stop(self, test_db):
        """trigger_background -> stop -> task is cancelled, collector not running."""
        await test_db.add_channel(
            Channel(channel_id=-100765, title="Blocking Channel", username="blocking_channel")
        )
        pool = _make_pool(get_available_client=AsyncMock(return_value=None))

        config = SchedulerConfig(delay_between_requests_sec=0)
        collector = Collector(pool, test_db, config)
        manager = SchedulerManager(collector, config)

        started_event = asyncio.Event()
        release_event = asyncio.Event()
        cancelled_event = asyncio.Event()

        async def _blocking_collect(_channel, **_kwargs):
            started_event.set()
            try:
                await release_event.wait()
                return 0
            except asyncio.CancelledError:
                cancelled_event.set()
                raise

        collector._collect_channel = AsyncMock(side_effect=_blocking_collect)

        try:
            await manager.trigger_background()
            assert manager._bg_task is not None
            await asyncio.wait_for(started_event.wait(), timeout=1.0)
            assert not manager._bg_task.done()
            assert collector.is_running is True

            await manager.stop()
            await asyncio.wait_for(cancelled_event.wait(), timeout=1.0)
        finally:
            release_event.set()

        assert manager._bg_task is None
        assert collector.is_running is False
        assert cancelled_event.is_set()

    @pytest.mark.asyncio
    async def test_stop_without_bg_task_is_safe(self, test_db):
        """stop() with no background task does not raise."""
        pool = _make_pool()
        config = SchedulerConfig()
        collector = Collector(pool, test_db, config)
        manager = SchedulerManager(collector, config)

        # Should not raise
        await manager.stop()
        assert manager._bg_task is None

    @pytest.mark.asyncio
    async def test_trigger_background_deduplicates_racing_calls(self, test_db):
        await test_db.add_channel(
            Channel(channel_id=-100876, title="Race Channel", username="race_channel")
        )
        pool = _make_pool(get_available_client=AsyncMock(return_value=None))
        config = SchedulerConfig(delay_between_requests_sec=0)
        collector = Collector(pool, test_db, config)
        manager = SchedulerManager(collector, config)

        started_event = asyncio.Event()
        release_event = asyncio.Event()
        call_count = 0

        async def _blocking_collect(_channel, **_kwargs):
            nonlocal call_count
            call_count += 1
            started_event.set()
            await release_event.wait()
            return 0

        collector._collect_channel = AsyncMock(side_effect=_blocking_collect)

        try:
            await asyncio.gather(manager.trigger_background(), manager.trigger_background())
            await asyncio.wait_for(started_event.wait(), timeout=1.0)
            assert manager._bg_task is not None
            assert call_count == 1
        finally:
            release_event.set()
            await manager.stop()


class TestAPICredentialsFallback:
    """When config has no API creds, lifespan loads them from DB settings."""

    @pytest.mark.asyncio
    async def test_api_credentials_fallback_via_lifespan(self, tmp_path):
        from src.web.app import lifespan

        # Pre-seed DB with credentials
        db_path = str(tmp_path / "test.db")
        db = Database(db_path)
        await db.initialize()
        await db.set_setting("tg_api_id", "99999")
        await db.set_setting("tg_api_hash", "hash_from_db")
        await db.close()

        # Config with empty credentials — should trigger fallback
        config = AppConfig()
        config.database.path = db_path
        assert config.telegram.api_id == 0
        assert config.telegram.api_hash == ""

        with patch.object(ClientPool, "initialize", new_callable=AsyncMock):
            app = create_app(config)
            async with lifespan(app):
                assert app.state.auth.is_configured is True
                assert app.state.auth._api_id == 99999
                assert app.state.auth._api_hash == "hash_from_db"

    @pytest.mark.asyncio
    async def test_invalid_api_id_in_db_does_not_crash_lifespan(self, tmp_path):
        from src.web.app import lifespan

        db_path = str(tmp_path / "bad-web.db")
        db = Database(db_path)
        await db.initialize()
        await db.set_setting("tg_api_id", "not-a-number")
        await db.set_setting("tg_api_hash", "hash_from_db")
        await db.close()

        config = AppConfig()
        config.database.path = db_path

        with patch.object(ClientPool, "initialize", new_callable=AsyncMock) as initialize:
            app = create_app(config)
            async with lifespan(app):
                assert app.state.auth.is_configured is False
                initialize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_api_id_in_db_does_not_crash_cli_pool_init(self, tmp_path):
        db_path = str(tmp_path / "bad-cli.db")
        db = Database(db_path)
        await db.initialize()
        await db.set_setting("tg_api_id", "not-a-number")
        await db.set_setting("tg_api_hash", "hash_from_db")

        config = AppConfig()
        config.database.path = db_path

        with patch.object(ClientPool, "initialize", new_callable=AsyncMock) as initialize:
            auth, pool = await init_pool(config, db)

        assert auth.is_configured is False
        assert isinstance(pool, ClientPool)
        initialize.assert_awaited_once()
        await db.close()
