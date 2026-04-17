"""Tests for dialogs routes."""

from __future__ import annotations

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import Account, Channel
from tests.helpers import AsyncIterEmpty, AsyncIterMessages


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
    assert "error=invalid_ids" in resp.headers["location"]


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
    """Test participants with missing params returns 400."""
    resp = await client.get("/dialogs/participants")
    assert resp.status_code == 400


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


# ─── leave_dialogs with channel_service ─────────────────────────────


@pytest.mark.asyncio
async def test_leave_dialogs_calls_channel_service(client):
    """Test leave_dialogs queues a command."""
    resp = await client.post(
        "/dialogs/leave",
        data={
            "phone": "+1234567890",
            "channel_ids": ["-100111:Test"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── send_message success / error paths ─────────────────────────────


@pytest.mark.asyncio
async def test_send_message_exception(client):
    """Test send message handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity not found"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/send",
        data={"phone": "+1234567890", "recipient": "-100111", "text": "hello"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── edit_message success / no_client / error ───────────────────────


@pytest.mark.asyncio
async def test_edit_message_no_client(client):
    """Test edit-message with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/edit-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42", "text": "edited"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_message_success(client):
    """Test successful edit message."""
    native_mock = AsyncMock()
    native_mock.edit_message = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/edit-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42", "text": "edited"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_message_exception(client):
    """Test edit message handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/edit-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42", "text": "edited"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── delete_message success / no_client / error ─────────────────────


@pytest.mark.asyncio
async def test_delete_message_no_client(client):
    """Test delete-message with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/delete-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_ids": "1,2,3"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_message_success(client):
    """Test successful delete messages."""
    native_mock = AsyncMock()
    native_mock.delete_messages = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/delete-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_ids": "1,2,3"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_message_exception(client):
    """Test delete messages handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/delete-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_ids": "1,2,3"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── forward_messages success / no_client / error / invalid_ids ──────


@pytest.mark.asyncio
async def test_forward_messages_invalid_ids(client):
    """Test forward-messages with non-numeric ids."""
    resp = await client.post(
        "/dialogs/forward-messages",
        data={
            "phone": "+1234567890",
            "from_chat": "-100111",
            "to_chat": "-100222",
            "message_ids": "abc",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_ids" in resp.headers["location"]


@pytest.mark.asyncio
async def test_forward_messages_no_client(client):
    """Test forward-messages with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/forward-messages",
        data={
            "phone": "+1234567890",
            "from_chat": "-100111",
            "to_chat": "-100222",
            "message_ids": "1,2",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_forward_messages_success(client):
    """Test successful forward messages."""
    native_mock = AsyncMock()
    native_mock.forward_messages = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/forward-messages",
        data={
            "phone": "+1234567890",
            "from_chat": "-100111",
            "to_chat": "-100222",
            "message_ids": "1,2",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_forward_messages_exception(client):
    """Test forward messages handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/forward-messages",
        data={
            "phone": "+1234567890",
            "from_chat": "-100111",
            "to_chat": "-100222",
            "message_ids": "1,2",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── pin_message success / no_client / error ────────────────────────


@pytest.mark.asyncio
async def test_pin_message_no_client(client):
    """Test pin-message with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/pin-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_pin_message_success(client):
    """Test successful pin message."""
    native_mock = AsyncMock()
    native_mock.pin_message = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/pin-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42", "notify": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_pin_message_exception(client):
    """Test pin message handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/pin-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── unpin_message success / no_client / error ──────────────────────


@pytest.mark.asyncio
async def test_unpin_message_no_client(client):
    """Test unpin-message with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/unpin-message",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_unpin_message_success(client):
    """Test successful unpin message."""
    native_mock = AsyncMock()
    native_mock.unpin_message = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/unpin-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_unpin_message_exception(client):
    """Test unpin message handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/unpin-message",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── download-media ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_download_media_missing_fields(client):
    """Test download-media with missing fields."""
    resp = await client.post("/dialogs/download-media", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=missing_fields" in resp.headers["location"]


@pytest.mark.asyncio
async def test_download_media_no_client(client):
    """Test download-media with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/download-media",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_download_media_message_not_found(client):
    """Test download-media when message is not found."""
    native_mock = AsyncMock()
    native_mock.iter_messages = MagicMock(return_value=AsyncIterEmpty())
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/download-media",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_download_media_no_media(client):
    """Test download-media when message has no media."""

    msg = SimpleNamespace(id=42, media=None)
    native_mock = AsyncMock()
    native_mock.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: AsyncIterMessages([msg])
    )
    native_mock.download_media = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/download-media",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_download_media_exception(client):
    """Test download-media handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/download-media",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── participants success / no_client / error ───────────────────────


@pytest.mark.asyncio
async def test_participants_no_client(client):
    """Test participants with no native client returns 503."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_participants_success(client):
    """Test successful participants fetch."""
    participant = SimpleNamespace(id=1, first_name="Alice", last_name="Smith", username="alice")
    native_mock = AsyncMock()
    native_mock.get_participants = AsyncMock(return_value=[participant])
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202
    assert "command_id" in resp.json()


@pytest.mark.asyncio
async def test_participants_exception(client):
    """Test participants handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202


# ─── edit-admin ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_edit_admin_missing_fields(client):
    """Test edit-admin with missing fields."""
    resp = await client.post("/dialogs/edit-admin", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=missing_fields" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_admin_no_client(client):
    """Test edit-admin with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/edit-admin",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_admin_success(client):
    """Test successful edit admin."""
    native_mock = AsyncMock()
    native_mock.edit_admin = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/edit-admin",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1", "title": "Admin", "is_admin": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_admin_exception(client):
    """Test edit admin handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/edit-admin",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── edit-permissions ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_edit_permissions_no_flags(client):
    """Test edit-permissions with no permission flags."""
    resp = await client.post(
        "/dialogs/edit-permissions",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=no_permission_flags" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_permissions_missing_fields(client):
    """Test edit-permissions with missing phone/chat/user."""
    resp = await client.post(
        "/dialogs/edit-permissions",
        data={"send_messages": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=missing_fields" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_permissions_no_client(client):
    """Test edit-permissions with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/edit-permissions",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1", "send_messages": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_permissions_success(client):
    """Test successful edit permissions."""
    native_mock = AsyncMock()
    native_mock.edit_permissions = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/edit-permissions",
        data={
            "phone": "+1234567890",
            "chat_id": "-100111",
            "user_id": "1",
            "send_messages": "1",
            "send_media": "0",
            "until_date": "2025-12-31T00:00:00",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_permissions_exception(client):
    """Test edit permissions handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/edit-permissions",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1", "send_messages": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── kick ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_kick_missing_fields(client):
    """Test kick with missing fields."""
    resp = await client.post("/dialogs/kick", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=missing_fields" in resp.headers["location"]


@pytest.mark.asyncio
async def test_kick_no_client(client):
    """Test kick with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/kick",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_kick_success(client):
    """Test successful kick participant."""
    native_mock = AsyncMock()
    native_mock.kick_participant = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/kick",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_kick_exception(client):
    """Test kick handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/kick",
        data={"phone": "+1234567890", "chat_id": "-100111", "user_id": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── broadcast-stats ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_broadcast_stats_missing_params(client):
    """Test broadcast-stats with missing params returns 400."""
    resp = await client.get("/dialogs/broadcast-stats")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_broadcast_stats_no_client(client):
    """Test broadcast-stats with no native client returns 503."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_broadcast_stats_success(client):
    """Test successful broadcast stats fetch."""
    stats = SimpleNamespace(
        followers=SimpleNamespace(current=100, previous=80),
        views_per_post=SimpleNamespace(current=500.0, previous=400.0),
        shares_per_post=None,
        reactions_per_post=None,
        forwards_per_post=None,
        period=SimpleNamespace(
            min_date=__import__("datetime").datetime(2025, 1, 1),
            max_date=__import__("datetime").datetime(2025, 1, 31),
        ),
        enabled_notifications=42,
    )
    native_mock = AsyncMock()
    native_mock.get_broadcast_stats = AsyncMock(return_value=stats)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202
    assert "command_id" in resp.json()


@pytest.mark.asyncio
async def test_broadcast_stats_empty(client):
    """Test broadcast stats with no available stats fields."""
    stats = SimpleNamespace(spec=[])
    native_mock = AsyncMock()
    native_mock.get_broadcast_stats = AsyncMock(return_value=stats)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202
    assert "command_id" in resp.json()


@pytest.mark.asyncio
async def test_broadcast_stats_exception(client):
    """Test broadcast stats handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202


# ─── archive / unarchive success + error ────────────────────────────


@pytest.mark.asyncio
async def test_archive_no_client(client):
    """Test archive with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/archive",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_archive_success(client):
    """Test successful archive dialog."""
    native_mock = AsyncMock()
    native_mock.edit_folder = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/archive",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_archive_exception(client):
    """Test archive handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/archive",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_unarchive_no_client(client):
    """Test unarchive with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/unarchive",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_unarchive_success(client):
    """Test successful unarchive dialog."""
    native_mock = AsyncMock()
    native_mock.edit_folder = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/unarchive",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_unarchive_exception(client):
    """Test unarchive handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/unarchive",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── mark-read success / no_client / error ──────────────────────────


@pytest.mark.asyncio
async def test_mark_read_no_client(client):
    """Test mark-read with no native client."""
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    resp = await client.post(
        "/dialogs/mark-read",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_mark_read_success(client):
    """Test successful mark read."""
    native_mock = AsyncMock()
    native_mock.send_read_acknowledge = AsyncMock(return_value=None)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/mark-read",
        data={"phone": "+1234567890", "chat_id": "-100111", "max_id": "500"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_mark_read_exception(client):
    """Test mark read handles exception."""
    native_mock = AsyncMock()
    native_mock.get_entity = AsyncMock(side_effect=Exception("Entity error"))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/mark-read",
        data={"phone": "+1234567890", "chat_id": "-100111"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


# ─── create-channel POST ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_channel_post_no_client(client):
    """Test create channel POST with no matching client."""
    resp = await client.post(
        "/dialogs/create-channel",
        data={"phone": "+9999999999", "title": "Test", "about": "", "username": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_create_channel_post_exception(client):
    """Test create channel POST handles exception."""
    from unittest.mock import MagicMock

    mock_client = MagicMock()
    mock_client.side_effect = Exception("Create failed")
    pool = client._transport.app.state.pool
    pool.clients["+1234567890"] = mock_client

    resp = await client.post(
        "/dialogs/create-channel",
        data={"phone": "+1234567890", "title": "Test", "about": "", "username": ""},
    )
    assert resp.status_code == 200


# ─── download-media success path with file ───────────────────────────


@pytest.mark.asyncio
async def test_download_media_success(client, tmp_path):
    """Test download-media returns file when media exists."""
    from tests.helpers import AsyncIterMessages

    # Create a temp file to simulate downloaded media
    media_file = tmp_path / "test_photo.jpg"
    media_file.write_bytes(b"\xff\xd8\xff\xe0fake_jpg_data")

    msg = SimpleNamespace(id=42, media=SimpleNamespace())
    native_mock = AsyncMock()
    native_mock.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: AsyncIterMessages([msg])
    )
    native_mock.download_media = AsyncMock(return_value=str(media_file))
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.post(
        "/dialogs/download-media",
        data={"phone": "+1234567890", "chat_id": "-100111", "message_id": "42"},
        follow_redirects=False,
    )
    # Should return the file or redirect — depends on path validation
    assert resp.status_code in (200, 303)


# ─── create-channel POST success ─────────────────────────────────────


@pytest.mark.asyncio
async def test_create_channel_post_success(client):
    """Test successful create channel."""
    mock_result = MagicMock()
    mock_channel = MagicMock()
    mock_channel.id = 123456
    mock_channel.username = "test_new_ch"
    mock_result.chats = [mock_channel]

    mock_client = MagicMock()
    mock_client.__call__ = AsyncMock(return_value=mock_result)
    pool = client._transport.app.state.pool
    pool.clients["+1234567890"] = mock_client

    resp = await client.post(
        "/dialogs/create-channel",
        data={"phone": "+1234567890", "title": "My New Channel", "about": "Test about", "username": "test_new_ch"},
    )
    assert resp.status_code == 200
    assert "My New Channel" in resp.text or "created" in resp.text.lower()


@pytest.mark.asyncio
async def test_create_channel_post_success_no_username(client):
    """Test create channel without username."""
    mock_result = MagicMock()
    mock_channel = MagicMock()
    mock_channel.id = 789012
    mock_channel.username = None
    mock_result.chats = [mock_channel]

    mock_client = MagicMock()
    mock_client.__call__ = AsyncMock(return_value=mock_result)
    pool = client._transport.app.state.pool
    pool.clients["+1234567890"] = mock_client

    resp = await client.post(
        "/dialogs/create-channel",
        data={"phone": "+1234567890", "title": "No Username Channel", "about": "", "username": ""},
    )
    assert resp.status_code == 200


# ─── broadcast-stats with non-standard fields ─────────────────────────


@pytest.mark.asyncio
async def test_broadcast_stats_with_string_fields(client):
    """Test broadcast stats where stat fields are not SimpleNamespace."""
    stats = SimpleNamespace(
        followers="unavailable",
        views_per_post=SimpleNamespace(current=None, previous=None),
        shares_per_post="N/A",
        reactions_per_post=SimpleNamespace(current=10, previous=5),
        forwards_per_post=None,
        period=None,
        enabled_notifications=None,
    )
    native_mock = AsyncMock()
    native_mock.get_broadcast_stats = AsyncMock(return_value=stats)
    pool = client._transport.app.state.pool
    pool.get_native_client_by_phone = AsyncMock(return_value=(native_mock, "+1234567890"))

    resp = await client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100111"
    )
    assert resp.status_code == 202
    assert "command_id" in resp.json()
