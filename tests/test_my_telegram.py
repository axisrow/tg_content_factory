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
    {"channel_id": -100111, "title": "My Channel", "username": "mychan",
     "channel_type": "channel", "deactivate": False, "is_own": False},
    {"channel_id": -100222, "title": "My Group", "username": None,
     "channel_type": "supergroup", "deactivate": False, "is_own": False},
    {"channel_id": 999, "title": "Some User", "username": "someuser",
     "channel_type": "dm", "deactivate": False, "is_own": False},
    {"channel_id": 888, "title": "My Bot", "username": "mybot",
     "channel_type": "bot", "deactivate": False, "is_own": False},
]


def _bind_dialog_cache_methods(pool):
    from src.telegram.client_pool import ClientPool

    pool._dialogs_cache = {}
    pool._dialogs_cache_ttl_sec = 60.0
    pool.invalidate_dialogs_cache = ClientPool.invalidate_dialogs_cache.__get__(pool, ClientPool)
    pool._get_cached_dialogs = ClientPool._get_cached_dialogs.__get__(pool, ClientPool)
    pool._get_db_cached_dialogs = ClientPool._get_db_cached_dialogs.__get__(pool, ClientPool)
    pool._store_cached_dialogs = ClientPool._store_cached_dialogs.__get__(pool, ClientPool)


def _make_channel_dialog(
    channel_id: int,
    title: str = "Cached Channel",
    username: str = "cachedchan",
):
    channel_entity = MagicMock()
    channel_entity.id = channel_id
    channel_entity.username = username
    channel_entity.creator = False

    dialog = MagicMock()
    dialog.entity = channel_entity
    dialog.title = title
    dialog.is_channel = True
    dialog.is_group = False
    return dialog


def _strip_extra_dialog_fields(dialogs: list[dict]) -> list[dict]:
    return [
        {key: value for key, value in dialog.items() if key != "already_added"}
        for dialog in dialogs
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

    async def _get_dialogs_for_phone(
        self,
        phone,
        include_dm=False,
        mode="channels_only",
        refresh=False,
    ):
        return _FAKE_DIALOGS

    async def _get_dialogs(self):
        return []

    async def _no_users(self):
        return []

    async def _leave_channels(self, phone, dialogs):
        return {cid: True for cid, _ in dialogs}

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
    assert 'href="/my-telegram/photos"' in resp.text
    assert "Если аккаунт не выбран, откроется первый доступный профиль." in resp.text
    assert "Выберите аккаунт" in resp.text
    assert "загрузить список диалогов" in resp.text


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
    assert 'action="/my-telegram/refresh"' in resp.text
    assert "Обновить диалоги" in resp.text
    assert "Показан сохранённый список диалогов" in resp.text


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
    async with AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=False
    ) as c:
        resp = await c.get("/my-telegram/")
    assert resp.status_code == 401
    await db.close()


@pytest.mark.asyncio
async def test_leave_channels_success():
    """All dialogs → True; PeerChannel for channels/groups, PeerUser for dm/bot."""
    from telethon.tl.types import PeerChannel, PeerUser

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    received_peers = []

    async def _get_entity(peer):
        received_peers.append(peer)
        return MagicMock()

    mock_client.get_entity = _get_entity
    mock_client.delete_dialog = AsyncMock()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (999, "dm")]
    with patch("src.telegram.client_pool.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result == {-100111: True, 999: True}
    assert mock_client.delete_dialog.await_count == 2
    assert isinstance(received_peers[0], PeerChannel)
    assert received_peers[0].channel_id == 100111
    assert isinstance(received_peers[1], PeerUser)
    assert received_peers[1].user_id == 999
    pool.invalidate_dialogs_cache.assert_called_once_with("+1234567890")
    pool._db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1234567890")


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
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (-100222, "supergroup")]
    with patch("src.telegram.client_pool.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result[-100111] is False
    assert result[-100222] is True
    pool._db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1234567890")


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
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (-100222, "supergroup")]
    with patch("src.telegram.client_pool.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result[-100111] is False
    assert result[-100222] is False
    pool.report_flood.assert_awaited_once_with("+1234567890", 60)
    pool._db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1234567890")


@pytest.mark.asyncio
async def test_leave_dialogs_post(client):
    """POST /my-telegram/leave redirects with left/failed counts."""
    resp = await client.post(
        "/my-telegram/leave",
        data={"phone": "+1234567890", "channel_ids": ["-100111:channel", "-100222:supergroup"]},
    )
    assert resp.status_code == 200  # follow_redirects=True → final GET
    assert "left=2" in str(resp.url) or "Отписались" in resp.text


@pytest.mark.asyncio
async def test_refresh_dialogs_post_warms_cache(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.get_users_info = AsyncMock(return_value=[])
    pool.get_dialogs = AsyncMock(return_value=[])
    pool.get_dialogs_for_phone = AsyncMock(return_value=list(_FAKE_DIALOGS))
    pool.leave_channels = AsyncMock(return_value={})
    app.state.pool = pool

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
        resp = await c.post("/my-telegram/refresh", data={"phone": "+1234567890"})

    assert resp.status_code == 200
    assert "My Channel" in resp.text
    assert pool.get_dialogs_for_phone.await_count == 2
    first_call = pool.get_dialogs_for_phone.await_args_list[0]
    second_call = pool.get_dialogs_for_phone.await_args_list[1]
    assert first_call.args == ("+1234567890",)
    assert first_call.kwargs == {"include_dm": True, "mode": "full", "refresh": True}
    assert second_call.args == ("+1234567890",)
    assert second_call.kwargs == {"include_dm": True, "mode": "full", "refresh": False}

    await app.state.collection_queue.shutdown()
    await db.close()


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

    pool.get_dialogs_for_phone.assert_awaited_once_with(
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=False,
    )
    by_id = {d["channel_id"]: d for d in dialogs}
    assert by_id[-100111]["already_added"] is True
    assert by_id[-100222]["already_added"] is False
    assert by_id[999]["already_added"] is False
    assert by_id[888]["already_added"] is False


@pytest.mark.asyncio
async def test_get_my_dialogs_passes_refresh_flag(db):
    pool = MagicMock()
    pool.get_dialogs_for_phone = AsyncMock(return_value=list(_FAKE_DIALOGS))
    queue = MagicMock()

    service = ChannelService(db, pool, queue)
    await service.get_my_dialogs("+1234567890", refresh=True)

    pool.get_dialogs_for_phone.assert_awaited_once_with(
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )


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
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    # Call the real method
    result = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
    )

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
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    # Patch wait_for to use a tiny timeout so we don't wait 60 s in tests
    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.client_pool.asyncio.wait_for", fast_wait_for):
        result = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert len(result) == 1
    assert result[0]["channel_id"] == -100999


@pytest.mark.asyncio
async def test_my_telegram_page_without_phone_does_not_fetch_dialogs(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.get_users_info = AsyncMock(return_value=[])
    pool.get_dialogs = AsyncMock(return_value=[])
    pool.get_dialogs_for_phone = AsyncMock(return_value=list(_FAKE_DIALOGS))
    pool.leave_channels = AsyncMock(return_value={})
    app.state.pool = pool

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
        resp = await c.get("/my-telegram/")

    assert resp.status_code == 200
    assert "Выберите аккаунт" in resp.text
    assert 'href="/my-telegram/photos"' in resp.text
    pool.get_dialogs_for_phone.assert_not_awaited()

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.asyncio
async def test_my_telegram_page_without_accounts_shows_disabled_photo_loader(tmp_path):
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

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        resp = await c.get("/my-telegram/")

    assert resp.status_code == 200
    assert "Сначала добавьте Telegram-аккаунт в настройках." in resp.text
    assert 'href="/my-telegram/photos"' not in resp.text
    assert 'aria-disabled="true"' in resp.text

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_uses_manual_cache():
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100123)

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.return_value = _fake_iter_dialogs()
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    result1 = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")
    result2 = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert result1 == result2
    assert mock_client.iter_dialogs.call_count == 1


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_refresh_bypasses_cache():
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100123)

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.side_effect = [_fake_iter_dialogs(), _fake_iter_dialogs()]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    await ClientPool.get_dialogs_for_phone(pool, "+1234567890")
    await ClientPool.get_dialogs_for_phone(pool, "+1234567890", refresh=True)

    assert mock_client.iter_dialogs.call_count == 2


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_refetches_after_ttl_expiry():
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100123)

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.side_effect = [_fake_iter_dialogs(), _fake_iter_dialogs()]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)
    pool._dialogs_cache_ttl_sec = -1.0

    await ClientPool.get_dialogs_for_phone(pool, "+1234567890")
    await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert mock_client.iter_dialogs.call_count == 2


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_uses_db_cache_across_pool_instances(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool.get_client_by_phone = AsyncMock()
    pool.release_client = AsyncMock()
    _bind_dialog_cache_methods(pool)

    dialogs = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
    )

    assert _strip_extra_dialog_fields(dialogs) == _strip_extra_dialog_fields(_FAKE_DIALOGS)
    pool.get_client_by_phone.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_refresh_replaces_db_cache(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [dict(_FAKE_DIALOGS[0], title="Stale Title")],
    )

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100111, title="Fresh Title", username="freshchan")

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.return_value = _fake_iter_dialogs()
    pool._db = db
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    _bind_dialog_cache_methods(pool)

    dialogs = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )

    assert dialogs[0]["title"] == "Fresh Title"
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert cached[0]["title"] == "Fresh Title"


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_failed_refresh_keeps_existing_db_cache(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    _bind_dialog_cache_methods(pool)

    dialogs = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )

    assert dialogs == []
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert _strip_extra_dialog_fields(cached) == _strip_extra_dialog_fields(_FAKE_DIALOGS)


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_partial_timeout_keeps_existing_db_cache(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock(spec=ClientPool)

    mock_client = MagicMock()
    partial_dialog = _make_channel_dialog(-100999, title="Partial Channel", username="partial")

    async def _iter():
        yield partial_dialog
        await asyncio.sleep(120)

    mock_client.iter_dialogs.return_value = _iter()
    pool._db = db
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    _bind_dialog_cache_methods(pool)

    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.client_pool.asyncio.wait_for", fast_wait_for):
        result = await ClientPool.get_dialogs_for_phone(
            pool,
            "+1234567890",
            include_dm=True,
            mode="full",
            refresh=True,
        )

    assert result[0]["title"] == "Partial Channel"
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert _strip_extra_dialog_fields(cached) == _strip_extra_dialog_fields(_FAKE_DIALOGS)
