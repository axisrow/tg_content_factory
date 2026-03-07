from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock, patch

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
    {"channel_id": 888, "title": "My Bot", "username": "mybot", "channel_type": "bot", "deactivate": False},
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

    async def _leave_channels(self, phone, channel_ids):
        return {cid: True for cid in channel_ids}

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {"+1234567890": MagicMock()},
            "get_users_info": _no_users,
            "get_dialogs": _get_dialogs,
            "get_dialogs_for_phone": _get_dialogs_for_phone,
            "leave_channels": _leave_channels,
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
    assert "Some User" in resp.text
    assert "My Bot" in resp.text
    # All 4 tabs present
    assert "tab-channels" in resp.text
    assert "tab-groups" in resp.text
    assert "tab-dms" in resp.text
    assert "tab-bots" in resp.text


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
async def test_leave_channels_success():
    """All channel_ids → True when delete_dialog succeeds."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    mock_client.get_entity = AsyncMock(return_value=MagicMock())
    mock_client.delete_dialog = AsyncMock()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()

    with patch("src.telegram.client_pool.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", [-100111, -100222])

    assert result == {-100111: True, -100222: True}
    assert mock_client.delete_dialog.await_count == 2


@pytest.mark.asyncio
async def test_leave_channels_partial_failure():
    """One delete_dialog raises RuntimeError → that id is False, others True."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()

    call_count = 0

    async def _get_entity(peer):
        return MagicMock()

    async def _delete_dialog(entity):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("some error")

    mock_client.get_entity = _get_entity
    mock_client.delete_dialog = _delete_dialog

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()

    with patch("src.telegram.client_pool.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", [-100111, -100222])

    assert result[-100111] is False
    assert result[-100222] is True


@pytest.mark.asyncio
async def test_leave_channels_flood_breaks_loop():
    """FloodWaitError → reports flood, marks all remaining ids as False, stops loop."""
    from telethon.errors import FloodWaitError

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()

    async def _get_entity(peer):
        return MagicMock()

    async def _delete_dialog(entity):
        err = FloodWaitError(request=None)
        err.seconds = 60
        raise err

    mock_client.get_entity = _get_entity
    mock_client.delete_dialog = _delete_dialog

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()

    with patch("src.telegram.client_pool.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", [-100111, -100222])

    assert result[-100111] is False
    assert result[-100222] is False
    pool.report_flood.assert_awaited_once_with("+1234567890", 60)


@pytest.mark.asyncio
async def test_leave_dialogs_post(client):
    """POST /my-telegram/leave redirects with left/failed counts."""
    resp = await client.post(
        "/my-telegram/leave",
        data={"phone": "+1234567890", "channel_ids": ["-100111", "-100222"]},
    )
    assert resp.status_code == 200  # follow_redirects=True → final GET
    assert "left=2" in str(resp.url) or "Отписались" in resp.text


@pytest.mark.asyncio
async def test_leave_dialogs_flash_message(client):
    """GET with left/failed params shows flash banner."""
    resp = await client.get("/my-telegram/?phone=%2B1234567890&left=2&failed=1")
    assert resp.status_code == 200
    assert "Отписались" in resp.text
    assert "<strong>2</strong>" in resp.text
    assert "<strong>1</strong>" in resp.text


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
    assert by_id[888]["already_added"] is False


@pytest.mark.asyncio
async def test_get_my_dialogs_bot_type():
    """entity with bot=True → channel_type='bot'."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)

    bot_entity = MagicMock()
    bot_entity.id = 777
    bot_entity.username = "testbot"
    bot_entity.bot = True

    bot_dialog = MagicMock()
    bot_dialog.entity = bot_entity
    bot_dialog.title = "Test Bot"
    bot_dialog.is_channel = False
    bot_dialog.is_group = False

    async def _fake_iter_dialogs():
        yield bot_dialog

    mock_client = MagicMock()
    mock_client.iter_dialogs.return_value = _fake_iter_dialogs()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()

    # Call the real method
    result = await ClientPool.get_dialogs_for_phone(pool, "+1234567890", include_dm=True)

    assert len(result) == 1
    assert result[0]["channel_type"] == "bot"
    assert result[0]["channel_id"] == 777


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_partial_on_timeout():
    """When iter_dialogs times out, partial accumulated results are returned."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)

    async def _slow_iter_dialogs():
        # Yield one dialog then hang indefinitely
        chan_entity = MagicMock()
        chan_entity.id = -100999
        chan_entity.username = "fastchan"
        chan_entity.megagroup = False
        chan_entity.broadcast = True
        chan_entity.gigagroup = False
        chan_entity.forum = False
        chan_entity.scam = False
        chan_entity.fake = False
        chan_entity.restricted = False

        dialog = MagicMock()
        dialog.entity = chan_entity
        dialog.title = "Fast Channel"
        dialog.is_channel = True
        dialog.is_group = False
        yield dialog

        await asyncio.sleep(120)

    mock_client = MagicMock()
    mock_client.iter_dialogs.return_value = _slow_iter_dialogs()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))

    # Patch wait_for to use a tiny timeout so we don't wait 60 s in tests
    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.client_pool.asyncio.wait_for", fast_wait_for):
        result = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert len(result) == 1
    assert result[0]["channel_id"] == -100999
