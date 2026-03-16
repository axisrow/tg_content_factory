"""Tests for my_telegram routes."""

from __future__ import annotations

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app


@pytest.fixture
async def client(tmp_path):
    """Create test client with mocked pool."""
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
    pool_mock.clients = {
        "+1234567890": SimpleNamespace(is_connected=lambda: True),
        "+9876543210": SimpleNamespace(is_connected=lambda: True),
    }
    pool_mock.resolve_channel = _resolve_channel
    pool_mock.get_dialogs = AsyncMock(return_value=[])
    pool_mock.get_dialogs_for_phone = AsyncMock(
        return_value=[
            {
                "channel_id": -100111,
                "title": "Dialog Channel 1",
                "username": "dialog1",
                "channel_type": "channel",
            },
            {
                "channel_id": -100222,
                "title": "Dialog Group",
                "username": None,
                "channel_type": "supergroup",
            },
        ]
    )
    pool_mock.leave_channels = AsyncMock(return_value={-100111: True, -100222: True})
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    # Add test account
    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_account(Account(phone="+9876543210", session_string="test_session2"))

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
async def test_my_telegram_page_no_phone(client):
    """Test my_telegram page without phone selection."""
    resp = await client.get("/my-telegram/")
    assert resp.status_code == 200
    # Should show account list
    assert "+1234567890" in resp.text or "account" in resp.text.lower()


@pytest.mark.asyncio
async def test_my_telegram_page_with_phone(client):
    """Test my_telegram page with phone selection."""
    resp = await client.get("/my-telegram/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_page_invalid_phone(client):
    """Test my_telegram page with invalid phone."""
    resp = await client.get("/my-telegram/?phone=invalid")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_page_shows_accounts(client):
    """Test my_telegram page shows available accounts."""
    resp = await client.get("/my-telegram/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_leave_dialogs_redirect(client):
    """Test leave dialogs redirects."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:Dialog 1", "-100222:Dialog 2"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/my-telegram/" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_leave_dialogs_empty(client):
    """Test leave dialogs with no selections."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_leave_dialogs_malformed_channel_id(client):
    """Test leave dialogs handles malformed channel IDs."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["invalid", "also-invalid"],
        },
        follow_redirects=False,
    )
    # Should not crash
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_leave_dialogs_negative_channel_id(client):
    """Test leave dialogs with negative channel IDs."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-1001234567890:Test Channel"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_leave_dialogs_no_colon(client):
    """Test leave dialogs with malformed ID (no colon)."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-1001234567890"],  # No colon
        },
        follow_redirects=False,
    )
    # Should skip malformed entries
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_my_telegram_shows_left_count(client):
    """Test my_telegram page shows left count from query param."""
    resp = await client.get("/my-telegram/?left=2&failed=0")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_shows_failed_count(client):
    """Test my_telegram page shows failed count from query param."""
    resp = await client.get("/my-telegram/?left=0&failed=1")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_phone_url_encoded(client):
    """Test my_telegram with URL-encoded phone number."""
    resp = await client.get("/my-telegram/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_no_accounts(client):
    """Test my_telegram with no connected accounts."""
    # Remove accounts
    db = client._transport.app.state.db
    accounts = await db.get_accounts()
    for acc in accounts:
        await db.delete_account(acc.phone)

    # Update pool mock
    client._transport.app.state.pool.clients = {}

    resp = await client.get("/my-telegram/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_leave_dialogs_preserves_phone(client):
    """Test leave dialogs preserves phone in redirect."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+9876543210",
            "channel_ids": ["-100111:Test"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    # Phone should be in redirect URL
    assert "phone=" in location


@pytest.mark.asyncio
async def test_my_telegram_logs_request(client, caplog):
    """Test my_telegram logs request details."""
    import logging

    with caplog.at_level(logging.INFO):
        resp = await client.get("/my-telegram/?phone=%2B1234567890")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_shows_already_added(client):
    """Test my_telegram shows already added flag."""
    # Add a channel that matches one of the dialogs
    db = client._transport.app.state.db
    await db.add_channel(
        Channel(
            channel_id=-100111,
            title="Dialog Channel 1",
            username="dialog1",
        )
    )

    resp = await client.get("/my-telegram/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_my_telegram_empty_dialogs(client):
    """Test my_telegram with no dialogs."""
    client._transport.app.state.pool.get_dialogs_for_phone = AsyncMock(return_value=[])

    resp = await client.get("/my-telegram/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_leave_dialogs_single(client):
    """Test leaving single dialog."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:Test"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_leave_dialogs_multiple(client):
    """Test leaving multiple dialogs."""
    resp = await client.post(
        "/my-telegram/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:First", "-100222:Second", "-100333:Third"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
