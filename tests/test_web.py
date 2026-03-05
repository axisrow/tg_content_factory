import base64

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.database import Database
from src.models import Account
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.collector import Collector
from src.web.app import create_app
from src.web.session import COOKIE_NAME, create_session_token


@pytest.fixture
async def client(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    # Manually initialize state (lifespan doesn't run with ASGITransport)
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
            "channel_type": "channel",
        }

    async def _get_dialogs(self):
        return [
            {
                "channel_id": -100111, "title": "Dialog Chan 1",
                "username": "chan1", "channel_type": "channel",
            },
            {
                "channel_id": -100222, "title": "Dialog Chan 2",
                "username": None, "channel_type": "group",
            },
        ]

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _resolve_channel,
            "get_dialogs": _get_dialogs,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
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
        yield c

    await db.close()


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] in ("healthy", "degraded")
    assert "db" in data
    assert "accounts_connected" in data


@pytest.mark.asyncio
async def test_dashboard(client):
    resp = await client.get("/dashboard/")
    assert resp.status_code == 200
    assert "Панель" in resp.text


@pytest.mark.asyncio
async def test_login_page(client):
    resp = await client.get("/auth/login")
    assert resp.status_code == 200
    assert "/settings" in resp.text


@pytest.mark.asyncio
async def test_settings_page(client):
    resp = await client.get("/settings/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_channels_page(client):
    resp = await client.get("/channels/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_page(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "Поиск" in resp.text


@pytest.mark.asyncio
async def test_scheduler_page(client):
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_with_query(client):
    resp = await client.get("/?q=test&mode=local")
    assert resp.status_code == 200
    assert "test" in resp.text


@pytest.mark.asyncio
async def test_search_with_invalid_channel_id_returns_error(client):
    resp = await client.get("/?q=test&mode=channel&channel_id=abc")
    assert resp.status_code == 200
    assert "Некорректный ID канала: abc" in resp.text


@pytest.fixture
async def unauth_client(client):
    """Client without auth headers, reusing the same app from client fixture."""
    transport = client._transport
    async with AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=True
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_no_auth_returns_401(unauth_client):
    resp = await unauth_client.get("/")
    assert resp.status_code == 401
    assert "WWW-Authenticate" in resp.headers


@pytest.mark.asyncio
async def test_health_no_auth(unauth_client):
    resp = await unauth_client.get("/health")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_basic_auth_sets_cookie(client):
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 200
    assert COOKIE_NAME in resp.cookies


@pytest.mark.asyncio
async def test_cookie_auth_without_basic(client):
    token = create_session_token("admin", "test_secret_key")
    transport = client._transport
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={COOKIE_NAME: token},
    ) as c:
        resp = await c.get("/")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_logout_clears_cookie(client):
    resp = await client.get("/logout", follow_redirects=False)
    assert resp.status_code == 401
    assert COOKIE_NAME in resp.headers.get("set-cookie", "")
    cookie_header = resp.headers.get("set-cookie", "")
    assert 'Max-Age=0' in cookie_header or 'max-age=0' in cookie_header


@pytest.mark.asyncio
async def test_cookie_not_secure_on_http(client):
    resp = await client.get("/", follow_redirects=False)
    cookie_header = resp.headers.get("set-cookie", "")
    assert "Secure" not in cookie_header


@pytest.mark.asyncio
async def test_cookie_secure_on_https(client):
    transport = client._transport
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="https://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        resp = await c.get("/")
        cookie_header = resp.headers.get("set-cookie", "")
        assert "Secure" in cookie_header


@pytest.mark.asyncio
async def test_invalid_cookie_falls_back(unauth_client):
    unauth_client.cookies.set(COOKIE_NAME, "fake.token")
    resp = await unauth_client.get("/")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_logout_no_auth_required(unauth_client):
    resp = await unauth_client.get("/logout", follow_redirects=False)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_settings_shows_accounts(tmp_path):
    """Settings page displays accounts from DB."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    account = Account(phone="+79991234567", session_string="test_session", is_primary=True)
    await db.add_account(account)

    app.state.pool = type("Pool", (), {"clients": {"+79991234567": object()}})()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
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
        resp = await c.get("/settings/")
        assert resp.status_code == 200
        assert "+79991234567" in resp.text
        assert "Добавьте первый аккаунт" not in resp.text

    await db.close()


@pytest.mark.asyncio
async def test_settings_no_accounts(client):
    """Settings page shows 'no accounts' message when DB has no accounts."""
    resp = await client.get("/settings/")
    assert resp.status_code == 200
    assert "Добавьте первый аккаунт" in resp.text
    assert "/auth/login" in resp.text


@pytest.mark.asyncio
async def test_resolve_channel_success(client):
    """Adding a channel via identifier resolves and saves it."""
    resp = await client.post("/channels/add", data={"identifier": "@testchan"})
    assert resp.status_code == 200
    assert "Resolved Channel" in resp.text


@pytest.mark.asyncio
async def test_csrf_blocks_cross_origin_post(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={"Origin": "https://evil.example"},
        follow_redirects=False,
    )
    assert resp.status_code == 403
    assert "CSRF validation failed" in resp.text


@pytest.mark.asyncio
async def test_csrf_blocks_null_origin(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={"Origin": "null"},
        follow_redirects=False,
    )
    assert resp.status_code == 403
    assert "CSRF validation failed" in resp.text


@pytest.mark.asyncio
async def test_csrf_allows_post_without_origin_or_referer(client):
    """POST without Origin/Referer headers is allowed (matches Django behavior)."""
    transport = client._transport
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        resp = await c.post(
            "/channels/add",
            data={"identifier": "@testchan"},
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.asyncio
async def test_csrf_allows_same_origin_post(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={"Origin": "http://test"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_resolve_channel_fail(tmp_path):
    """Failed resolve redirects with error query param."""
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

    async def _fail_resolve(self, identifier):
        raise ValueError("not found")

    async def _get_dialogs(self):
        return []

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _fail_resolve,
            "get_dialogs": _get_dialogs,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
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
        resp = await c.post("/channels/add", data={"identifier": "@nonexistent"})
        assert resp.status_code == 200
        assert "Не удалось найти канал" in resp.text

    await db.close()


@pytest.mark.asyncio
async def test_dialogs_endpoint(client):
    """GET /channels/dialogs returns JSON list of channels."""
    resp = await client.get("/channels/dialogs")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["title"] == "Dialog Chan 1"
    assert data[0]["channel_id"] == -100111
    assert "already_added" in data[0]


@pytest.mark.asyncio
async def test_add_bulk(client):
    """POST /channels/add-bulk adds selected channels from dialogs."""
    resp = await client.post(
        "/channels/add-bulk",
        data={"channel_ids": ["-100111", "-100222"]},
    )
    assert resp.status_code == 200
    # Verify channels page shows added channels
    resp = await client.get("/channels/")
    assert "Dialog Chan 1" in resp.text
    assert "Dialog Chan 2" in resp.text


@pytest.mark.asyncio
async def test_add_channel_redirect_has_msg(client):
    """Adding a channel redirects with ?msg=channel_added."""
    resp = await client.post(
        "/channels/add", data={"identifier": "@testchan"}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "msg=channel_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_channel_type_displayed(client):
    """Channel type column is shown on channels page after adding a channel."""
    await client.post("/channels/add", data={"identifier": "@testchan"})
    resp = await client.get("/channels/")
    assert resp.status_code == 200
    assert "Канал" in resp.text
    assert "Тип" in resp.text


@pytest.mark.asyncio
async def test_search_results_have_tg_links(client):
    """Search results contain links to original Telegram messages."""
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = client._transport.app.state.db
    ch = Channel(channel_id=-100123, title="Test", username="testchan", channel_type="channel")
    await db.add_channel(ch)
    msg = Message(
        channel_id=-100123, message_id=42,
        text="Hello world", date=datetime.now(timezone.utc),
    )
    await db.insert_message(msg)

    resp = await client.get("/?q=Hello&mode=local")
    assert resp.status_code == 200
    assert "t.me/testchan/42" in resp.text
    assert "&#8599;" in resp.text


@pytest.mark.asyncio
async def test_search_results_private_channel_link(client):
    """Private channel messages get t.me/c/ links."""
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = client._transport.app.state.db
    ch = Channel(channel_id=-100999, title="Private", username=None, channel_type="group")
    await db.add_channel(ch)
    msg = Message(
        channel_id=-100999, message_id=7,
        text="Secret message", date=datetime.now(timezone.utc),
    )
    await db.insert_message(msg)

    resp = await client.get("/?q=Secret&mode=local")
    assert resp.status_code == 200
    assert "t.me/c/-100999/7" in resp.text
