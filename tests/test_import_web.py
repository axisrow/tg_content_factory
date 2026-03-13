from __future__ import annotations

import base64
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.database import Database
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.collector import Collector
from src.web.app import create_app
from tests.helpers import FakeCliTelethonClient


def _resolved_channel_entity(identifier: object) -> SimpleNamespace:
    if not isinstance(identifier, str):
        return SimpleNamespace(id=abs(hash(str(identifier))) % 10**10, title="Channel peer")
    ident = identifier.strip().lower().lstrip("@")
    channel_id = abs(hash(ident)) % 10**10
    username = ident if not ident.lstrip("-").isdigit() else None
    return SimpleNamespace(
        id=channel_id,
        title=f"Channel {ident}",
        username=username,
        broadcast=True,
        megagroup=False,
        gigagroup=False,
        forum=False,
        monoforum=False,
        scam=False,
        fake=False,
        restricted=False,
    )


@pytest.fixture
async def client(tmp_path, real_pool_harness_factory):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(entity_resolver=_resolved_channel_entity),
    )
    await harness.connect_account("+7000", session_string="session", is_primary=True)
    app.state.auth = harness.auth
    app.state.pool = harness.pool
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
async def test_import_page_get(client):
    resp = await client.get("/channels/import")
    assert resp.status_code == 200
    assert "Импорт" in resp.text
    assert "Импортировать" in resp.text


@pytest.mark.asyncio
async def test_import_textarea(client):
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@channel1\n@channel2"},
    )
    assert resp.status_code == 200
    assert "Добавлен" in resp.text
    assert "channel1" in resp.text
    assert "channel2" in resp.text


@pytest.mark.asyncio
async def test_import_empty_input(client):
    resp = await client.post(
        "/channels/import",
        data={"text_input": ""},
    )
    assert resp.status_code == 200
    # No errors, results block shows zeros
    assert "Всего" in resp.text


@pytest.mark.asyncio
async def test_import_skips_duplicates(client):
    # First import
    await client.post(
        "/channels/import",
        data={"text_input": "@dupchan"},
    )
    # Second import of same channel
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@dupchan"},
    )
    assert resp.status_code == 200
    assert "Пропущен" in resp.text


@pytest.mark.asyncio
async def test_import_button_on_channels_page(client):
    resp = await client.get("/channels/")
    assert resp.status_code == 200
    assert '/channels/import' in resp.text
    assert "Импорт" in resp.text


@pytest.fixture
async def client_no_accounts(tmp_path, real_pool_harness_factory):
    """Client fixture with no connected Telegram accounts."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test_no_acc.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    harness = real_pool_harness_factory()
    app.state.auth = harness.auth
    app.state.pool = harness.pool
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
async def test_import_no_client_shows_error(client_no_accounts):
    resp = await client_no_accounts.post(
        "/channels/import",
        data={"text_input": "@chan1\n@chan2\n@chan3"},
    )
    assert resp.status_code == 200
    assert "Нет доступных аккаунтов" in resp.text
    # All 3 should fail
    assert "Ошибок: <strong>3</strong>" in resp.text


@pytest.mark.asyncio
async def test_import_file_upload(client):
    file_content = b"@filech1\n@filech2\n@filech3"
    resp = await client.post(
        "/channels/import",
        data={"text_input": ""},
        files={"file": ("channels.txt", file_content, "text/plain")},
    )
    assert resp.status_code == 200
    assert "filech1" in resp.text
    assert "filech2" in resp.text
    assert "filech3" in resp.text


@pytest.fixture
async def client_scam(tmp_path, real_pool_harness_factory):
    """Client fixture where resolve_channel returns a scam channel."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test_scam.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _no_users(self):
        return []

    async def _resolve_scam(self, identifier):
        return {
            "channel_id": -1008888888,
            "title": "Scam Import Channel",
            "username": "scamimport",
            "channel_type": "scam",
            "deactivate": True,
        }

    async def _get_dialogs(self):
        return []

    harness = real_pool_harness_factory()
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.pool.get_users_info = _no_users.__get__(app.state.pool, type(app.state.pool))
    app.state.pool.resolve_channel = _resolve_scam.__get__(app.state.pool, type(app.state.pool))
    app.state.pool.get_dialogs = _get_dialogs.__get__(app.state.pool, type(app.state.pool))
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
        yield c, db

    await db.close()


@pytest.mark.asyncio
async def test_import_scam_channel_is_inactive(client_scam):
    """Importing a scam channel creates it with is_active=False."""
    c, db = client_scam
    resp = await c.post(
        "/channels/import",
        data={"text_input": "@scamimport"},
    )
    assert resp.status_code == 200
    assert "Добавлен" in resp.text
    assert "неактивен" in resp.text

    channels = await db.get_channels()
    assert len(channels) == 1
    assert channels[0].is_active is False
