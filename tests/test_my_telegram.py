from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import Account
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.channel_service import ChannelService
from src.telegram.collector import Collector
from src.web.app import create_app

_FAKE_DIALOGS = [
    {"channel_id": -100111, "title": "My Channel", "username": "mychan", "channel_type": "channel", "deactivate": False},
    {"channel_id": -100222, "title": "My Group", "username": None, "channel_type": "supergroup", "deactivate": False},
    {"channel_id": 999, "title": "Some User", "username": "someuser", "channel_type": "dm", "deactivate": False},
]


@pytest.fixture
async def client(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _get_dialogs_for_phone(self, phone, include_dm=False):
        return _FAKE_DIALOGS

    async def _get_dialogs(self):
        return []

    async def _no_users(self):
        return []

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+1234567890": MagicMock()},
            "get_users_info": _no_users,
            "get_dialogs": _get_dialogs,
            "get_dialogs_for_phone": _get_dialogs_for_phone,
        },
    )()

    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(collector, config.scheduler)
    app.state.session_secret = "test_secret_key"

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.asyncio
async def test_my_telegram_page_renders(client):
    resp = await client.get("/my-telegram/")
    assert resp.status_code == 200
    assert "Мой Телеграм" in resp.text


@pytest.mark.asyncio
async def test_my_telegram_page_shows_dialogs(client):
    resp = await client.get("/my-telegram/?phone=%2B1234567890")
    assert resp.status_code == 200
    assert "My Channel" in resp.text
    assert "My Group" in resp.text


@pytest.mark.asyncio
async def test_my_telegram_page_requires_auth(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    app.state.pool = type("Pool", (), {"clients": {}})()
    app.state.session_secret = "test_secret_key"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=False) as c:
        resp = await c.get("/my-telegram/")
    assert resp.status_code == 401
    await db.close()


@pytest.mark.asyncio
async def test_get_my_dialogs_enriches_already_added(db):
    """get_my_dialogs() marks dialogs already in the channel DB."""
    from src.models import Channel
    await db.add_channel(Channel(
        channel_id=-100111,
        title="My Channel",
        username="mychan",
        channel_type="channel",
        is_active=True,
    ))

    pool = MagicMock()
    pool.get_dialogs_for_phone = AsyncMock(return_value=list(_FAKE_DIALOGS))
    queue = MagicMock()

    service = ChannelService(db, pool, queue)
    dialogs = await service.get_my_dialogs("+1234567890")

    pool.get_dialogs_for_phone.assert_awaited_once_with("+1234567890", include_dm=True)
    by_id = {d["channel_id"]: d for d in dialogs}
    assert by_id[-100111]["already_added"] is True
    assert by_id[-100222]["already_added"] is False
    assert by_id[999]["already_added"] is False
