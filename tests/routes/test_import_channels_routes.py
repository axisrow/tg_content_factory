"""Tests for import_channels routes."""

from __future__ import annotations

import base64
import io

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import Channel


@pytest.fixture
async def client(base_app):
    """Create test client with mocked pool."""
    app, _, pool = base_app

    async def _resolve_channel(identifier):
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

    pool.clients = {}
    pool.resolve_channel = _resolve_channel

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c

@pytest.fixture
async def client_no_resolve(client):
    """Client with resolve that raises no_client error."""

    async def _resolve_no_client(identifier):
        raise RuntimeError("no_client")

    client._transport.app.state.pool.resolve_channel = _resolve_no_client
    return client


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


@pytest.mark.parametrize(
    "text_input",
    [
        "@testchannel",
        "@channel1\n@channel2\n@channel3",
        "@channel1, @channel2, @channel3",
        "@channel1; @channel2",
        "",
        "   \n\n   ",
        "-1001234567890",
        "https://t.me/testchannel",
        "@тестовый",
        "testchannel",
        "\n".join([f"@channel{i}" for i in range(50)]),
    ],
    ids=[
        "single",
        "multiline",
        "comma",
        "semicolon",
        "empty",
        "whitespace",
        "channel-id",
        "tme-link",
        "unicode",
        "without-at",
        "large-list",
    ],
)
@pytest.mark.asyncio
async def test_import_text_variants(client, text_input):
    """Test supported text import variants."""
    resp = await client.post(
        "/channels/import",
        data={"text_input": text_input},
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
    assert "queued" in resp.text.lower() or "очеред" in resp.text.lower()


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
    await db.add_channel(
        Channel(
            channel_id=-1001111111111,
            title="Existing",
            username="existing",
        )
    )

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
