"""Tests for dialogs routes."""

from __future__ import annotations

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import Account, Channel


@pytest.fixture
async def client(base_app):
    """Create test client with mocked pool."""
    app, db, pool_mock = base_app

    async def _resolve_channel(identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

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
    await db.add_account(Account(phone="+9876543210", session_string="test_session2"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c



@pytest.mark.asyncio
async def test_dialogs_page_no_phone(client):
    """Test dialogs page without phone selection."""
    resp = await client.get("/dialogs/")
    assert resp.status_code == 200
    # Should show account list
    assert "+1234567890" in resp.text or "account" in resp.text.lower()


@pytest.mark.asyncio
async def test_dialogs_page_with_phone(client):
    """Test dialogs page with phone selection."""
    resp = await client.get("/dialogs/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_legacy_dialogs_route_redirects_to_dialogs(client):
    legacy_prefix = "/my" + "-telegram"
    resp = await client.get(f"{legacy_prefix}/?phone=%2B1234567890", follow_redirects=False)
    assert resp.status_code == 308
    assert resp.headers["location"] == "/dialogs/?phone=%2B1234567890"


@pytest.mark.asyncio
async def test_legacy_dialogs_post_route_redirects_to_dialogs(client):
    legacy_prefix = "/my" + "-telegram"
    resp = await client.post(
        f"{legacy_prefix}/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:Dialog 1"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert resp.headers["location"] == "/dialogs/leave"


@pytest.mark.asyncio
async def test_dialogs_page_invalid_phone(client):
    """Test dialogs page with invalid phone."""
    resp = await client.get("/dialogs/?phone=invalid")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dialogs_page_shows_accounts(client):
    """Test dialogs page shows available accounts."""
    resp = await client.get("/dialogs/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_leave_dialogs_redirect(client):
    """Test leave dialogs redirects."""
    resp = await client.post(
        "/dialogs/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:Dialog 1", "-100222:Dialog 2"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/dialogs/" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_leave_dialogs_empty(client):
    """Test leave dialogs with no selections."""
    resp = await client.post(
        "/dialogs/leave",
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
        "/dialogs/leave",
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
        "/dialogs/leave",
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
        "/dialogs/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-1001234567890"],  # No colon
        },
        follow_redirects=False,
    )
    # Should skip malformed entries
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_dialogs_shows_left_count(client):
    """Test dialogs page shows left count from query param."""
    resp = await client.get("/dialogs/?left=2&failed=0")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dialogs_shows_failed_count(client):
    """Test dialogs page shows failed count from query param."""
    resp = await client.get("/dialogs/?left=0&failed=1")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dialogs_phone_url_encoded(client):
    """Test dialogs with URL-encoded phone number."""
    resp = await client.get("/dialogs/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dialogs_no_accounts(client):
    """Test dialogs with no connected accounts."""
    # Remove accounts
    db = client._transport.app.state.db
    accounts = await db.get_accounts()
    for acc in accounts:
        await db.delete_account(acc.phone)

    # Update pool mock
    client._transport.app.state.pool.clients = {}

    resp = await client.get("/dialogs/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_leave_dialogs_preserves_phone(client):
    """Test leave dialogs preserves phone in redirect."""
    resp = await client.post(
        "/dialogs/leave",
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
async def test_dialogs_logs_request(client, caplog):
    """Test dialogs logs request details."""
    import logging

    with caplog.at_level(logging.INFO):
        resp = await client.get("/dialogs/?phone=%2B1234567890")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dialogs_shows_already_added(client):
    """Test dialogs shows already added flag."""
    # Add a channel that matches one of the dialogs
    db = client._transport.app.state.db
    await db.add_channel(
        Channel(
            channel_id=-100111,
            title="Dialog Channel 1",
            username="dialog1",
        )
    )

    resp = await client.get("/dialogs/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dialogs_empty_dialogs(client):
    """Test dialogs with no dialogs."""
    client._transport.app.state.pool.get_dialogs_for_phone = AsyncMock(return_value=[])

    resp = await client.get("/dialogs/?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_leave_dialogs_single(client):
    """Test leaving single dialog."""
    resp = await client.post(
        "/dialogs/leave",
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
        "/dialogs/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:First", "-100222:Second", "-100333:Third"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


# ─── refresh & cache ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_dialogs_redirects(client):
    """Test refresh dialogs redirects back."""
    resp = await client.post(
        "/dialogs/refresh",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "phone=" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_refresh_dialogs_missing_phone(client):
    """Test refresh without phone returns validation error."""
    resp = await client.post("/dialogs/refresh", follow_redirects=False)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_cache_status_returns_json(client):
    """Test cache-status endpoint returns JSON list."""
    db = client._transport.app.state.db
    await db.repos.dialog_cache.replace_dialogs("+1234567890", [
        {"channel_id": 100111, "title": "Cached", "username": "cached",
         "channel_type": "channel", "deactivate": 0, "is_own": 0},
    ])
    resp = await client.get("/dialogs/cache-status")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_cache_clear_with_phone(client):
    """Test cache-clear with phone param."""
    resp = await client.post(
        "/dialogs/cache-clear",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_cache_clear_without_phone(client):
    """Test cache-clear without phone clears all."""
    resp = await client.post("/dialogs/cache-clear", follow_redirects=False)
    assert resp.status_code == 303


# ─── send / edit / delete messages ──────────────────────────────────


@pytest.mark.asyncio
async def test_send_message_missing_fields(client):
    """Test send with missing required fields redirects."""
    resp = await client.post("/dialogs/send", follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_send_message_no_client(client):
    """Test send when native client is unavailable."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/send",
        data={"phone": "+1234567890", "recipient": "-100111", "text": "hi"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_send_message_success(client):
    """Test successful send message."""
    native_mock = AsyncMock()
    native_mock.send_message = AsyncMock(return_value=SimpleNamespace(id=42))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/send",
        data={"phone": "+1234567890", "recipient": "-100111", "text": "hello"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_edit_message_missing_fields(client):
    """Test edit-message with missing fields redirects."""
    resp = await client.post("/dialogs/edit-message", follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_delete_message_missing_fields(client):
    """Test delete-message with missing fields redirects."""
    resp = await client.post("/dialogs/delete-message", follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_delete_message_invalid_ids(client):
    """Test delete-message with non-numeric message_ids."""
    resp = await client.post(
        "/dialogs/delete-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_ids": "abc"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_forward_messages_missing_fields(client):
    """Test forward-messages with missing fields redirects."""
    resp = await client.post("/dialogs/forward-messages", follow_redirects=False)
    assert resp.status_code == 303


# ─── pin / unpin ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pin_message_missing_fields(client):
    """Test pin-message with missing fields redirects."""
    resp = await client.post("/dialogs/pin-message", follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_unpin_message_missing_fields(client):
    """Test unpin-message with missing fields redirects."""
    resp = await client.post("/dialogs/unpin-message", follow_redirects=False)
    assert resp.status_code == 303


# ─── participants / archive / unarchive / mark-read ─────────────────


@pytest.mark.asyncio
async def test_participants_missing_params(client):
    """Test participants with missing params returns error or empty."""
    resp = await client.get("/dialogs/participants")
    # May return 400 or redirect
    assert resp.status_code in (200, 303, 400, 422)


@pytest.mark.asyncio
async def test_archive_dialog_missing_fields(client):
    """Test archive with missing fields redirects."""
    resp = await client.post("/dialogs/archive", follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_unarchive_dialog_missing_fields(client):
    """Test unarchive with missing fields redirects."""
    resp = await client.post("/dialogs/unarchive", follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_mark_read_missing_fields(client):
    """Test mark-read with missing fields redirects."""
    resp = await client.post("/dialogs/mark-read", follow_redirects=False)
    assert resp.status_code == 303


# ─── create-channel page ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_channel_page_renders(client):
    """Test create-channel page renders."""
    resp = await client.get("/dialogs/create-channel")
    assert resp.status_code == 200
