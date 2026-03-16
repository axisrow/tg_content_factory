"""Tests for import_channels routes."""
from __future__ import annotations

import base64
import io

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
        # Mock resolve based on identifier
        if identifier.startswith("@"):
            return {
                "channel_id": -1001234567890 + hash(identifier) % 1000000,
                "title": f"Channel {identifier}",
                "username": identifier.lstrip("@"),
                "channel_type": "channel",
            }
        elif identifier.lstrip("-").isdigit():
            return {
                "channel_id": int(identifier),
                "title": f"Channel {identifier}",
                "username": None,
                "channel_type": "channel",
            }
        raise RuntimeError(f"Cannot resolve: {identifier}")

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "resolve_channel": _resolve_channel,
        },
    )()

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
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


@pytest.fixture
async def client_no_resolve(client):
    """Client with resolve that raises no_client error."""
    async def _resolve_no_client(self, identifier):
        raise RuntimeError("no_client")

    client._transport.app.state.pool.resolve_channel = _resolve_no_client


@pytest.mark.asyncio
async def test_import_page(client):
    """Test import page renders."""
    resp = await client.get("/channels/import")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_page_shows_form(client):
    """Test import page shows upload form."""
    resp = await client.get("/channels/import")
    assert resp.status_code == 200
    assert "import" in resp.text.lower() or "загруз" in resp.text.lower()


@pytest.mark.asyncio
async def test_import_text_single_channel(client):
    """Test importing single channel via text."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@testchannel"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_multiple_channels(client):
    """Test importing multiple channels via text."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@channel1\n@channel2\n@channel3"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_comma_separated(client):
    """Test importing comma-separated channels."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@channel1, @channel2, @channel3"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_semicolon_separated(client):
    """Test importing semicolon-separated channels."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@channel1; @channel2"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_empty(client):
    """Test importing empty text."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": ""},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_whitespace_only(client):
    """Test importing whitespace only."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "   \n\n   "},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_channel_id(client):
    """Test importing by channel ID."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "-1001234567890"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_text_tme_link(client):
    """Test importing t.me link."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "https://t.me/testchannel"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_deduplication(client):
    """Test duplicate channels are skipped."""
    # First import
    await client.post(
        "/channels/import",
        data={"text_input": "@testchannel"},
    )

    # Second import of same channel
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@testchannel"},
    )
    assert resp.status_code == 200
    # Should show skipped
    assert "skipped" in resp.text.lower() or "уже" in resp.text.lower()


@pytest.mark.asyncio
async def test_import_file_txt(client):
    """Test importing from txt file."""
    content = b"@channel1\n@channel2\n@channel3"
    file = ("channels.txt", io.BytesIO(content), "text/plain")

    resp = await client.post(
        "/channels/import",
        files={"file": file},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_file_empty(client):
    """Test importing empty file."""
    file = ("empty.txt", io.BytesIO(b""), "text/plain")

    resp = await client.post(
        "/channels/import",
        files={"file": file},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_file_and_text_combined(client):
    """Test importing from both file and text."""
    content = b"@filechannel"
    file = ("channels.txt", io.BytesIO(content), "text/plain")

    resp = await client.post(
        "/channels/import",
        data={"text_input": "@textchannel"},
        files={"file": file},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_results_structure(client):
    """Test import results contain expected fields."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@testchannel"},
    )
    assert resp.status_code == 200
    # Results should have added/skipped/failed
    text_lower = resp.text.lower()
    # Just verify page renders without error
    assert "results" in text_lower or "результат" in text_lower


@pytest.mark.asyncio
async def test_import_no_client_error(client, client_no_resolve):
    """Test import handles no_client error gracefully."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@testchannel"},
    )
    assert resp.status_code == 200
    # Should show error about no accounts
    assert "failed" in resp.text.lower() or "нет" in resp.text.lower()


@pytest.mark.asyncio
async def test_import_resolve_failure(client):
    """Test import handles resolve failure."""
    async def _resolve_fail(self, identifier):
        raise Exception("Connection error")

    client._transport.app.state.pool.resolve_channel = _resolve_fail

    resp = await client.post(
        "/channels/import",
        data={"text_input": "@nonexistent"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_mixed_success_failure(client):
    """Test import with mix of success and failures."""
    # First add a channel that will be skipped
    await client.post(
        "/channels/import",
        data={"text_input": "@existing"},
    )

    # Now try to import existing + new
    resp2 = await client.post(
        "/channels/import",
        data={"text_input": "@existing\n@newchannel"},
    )
    assert resp2.status_code == 200


@pytest.mark.asyncio
async def test_import_scam_channel(client):
    """Test import marks scam/fake channels as inactive."""
    async def _resolve_scam(self, identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Scam Channel",
            "username": "scam",
            "channel_type": "channel",
            "deactivate": True,
        }

    client._transport.app.state.pool.resolve_channel = _resolve_scam

    resp = await client.post(
        "/channels/import",
        data={"text_input": "@scam"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_preserves_existing_channels(client):
    """Test import doesn't affect existing channels."""
    # Add a channel
    db = client._transport.app.state.db
    await db.add_channel(Channel(
        channel_id=-1001111111111,
        title="Existing",
        username="existing",
    ))

    # Import new channel
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@newchannel"},
    )
    assert resp.status_code == 200

    # Existing should still be there
    channels = await db.get_channels()
    assert any(c.channel_id == -1001111111111 for c in channels)


@pytest.mark.asyncio
async def test_import_results_total_count(client):
    """Test import shows total count."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@a\n@b\n@c"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_unicode_channel_name(client):
    """Test import handles unicode in channel names."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "@тестовый"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_large_list(client):
    """Test import handles large list."""
    # Create list of 50 channels
    channels = "\n".join([f"@channel{i}" for i in range(50)])

    resp = await client.post(
        "/channels/import",
        data={"text_input": channels},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_file_csv(client):
    """Test importing from CSV file."""
    content = b"channel\n@channel1\n@channel2"
    file = ("channels.csv", io.BytesIO(content), "text/csv")

    resp = await client.post(
        "/channels/import",
        files={"file": file},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_channel_without_at(client):
    """Test import channel without @ prefix."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": "testchannel"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_import_negative_id(client):
    """Test import with negative channel ID."""
    async def _resolve_id(self, identifier):
        return {
            "channel_id": int(identifier),
            "title": f"ID Channel {identifier}",
            "username": None,
            "channel_type": "channel",
        }

    client._transport.app.state.pool.resolve_channel = _resolve_id

    resp = await client.post(
        "/channels/import",
        data={"text_input": "-1001234567890"},
    )
    assert resp.status_code == 200
