"""Tests for auth routes."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import TelegramCommand, TelegramCommandStatus


@pytest.fixture
async def client(base_app):
    """Create test client with mocked auth."""
    app, _, pool = base_app

    async def _resolve_channel(identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    pool.clients = {}
    pool.resolve_channel = _resolve_channel
    pool.add_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()

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
async def client_unconfigured(base_app):
    """Create test client with unconfigured auth."""
    app, _, pool = base_app
    auth = MagicMock()
    auth.is_configured = False
    app.state.auth = auth

    pool.clients = {}
    pool.add_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()

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
async def test_login_page(client):
    """Test login page renders."""
    resp = await client.get("/auth/login")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_login_page_shows_phone_step(client):
    """Test login page shows phone step when configured."""
    resp = await client.get("/auth/login")
    assert resp.status_code == 200
    # Should show phone input
    assert "phone" in resp.text.lower() or "телефон" in resp.text.lower()


@pytest.mark.asyncio
async def test_login_page_shows_credentials_step(client_unconfigured):
    """Test login page shows credentials step when not configured."""
    resp = await client_unconfigured.get("/auth/login")
    assert resp.status_code == 200
    # Should show api_id/api_hash input
    assert "api" in resp.text.lower() or "credential" in resp.text.lower()


@pytest.mark.asyncio
async def test_save_credentials_redirect(client_unconfigured):
    """Test save credentials redirects to login."""
    resp = await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_id": 12345, "api_hash": "test_hash"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/auth/login" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_save_credentials_updates_auth(client_unconfigured):
    """Test save credentials updates auth."""
    await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_id": 12345, "api_hash": "new_hash"},
    )

    # Check DB was updated
    db = client_unconfigured._transport.app.state.db
    api_id = await db.get_setting("tg_api_id")
    assert api_id == "12345"


@pytest.mark.asyncio
async def test_send_code_success(client):
    """Test send code is enqueued instead of executed inline."""
    db = client._transport.app.state.db
    auth = client._transport.app.state.auth

    with patch.object(auth, "send_code", new_callable=AsyncMock) as mock_send:
        resp = await client.post(
            "/auth/send-code",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "/auth/login?command_id=" in resp.headers["location"]
        mock_send.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "auth.send_code"
    assert commands[0].payload["phone"] == "+1234567890"


@pytest.mark.asyncio
async def test_send_code_unconfigured(client_unconfigured):
    """Test send code when auth not configured."""
    resp = await client_unconfigured.post(
        "/auth/send-code",
        data={"phone": "+1234567890"},
    )
    assert resp.status_code == 200
    # Should show error
    assert "error" in resp.text.lower() or "api" in resp.text.lower()


@pytest.mark.asyncio
async def test_send_code_error(client):
    """Test send code route does not call TelegramAuth inline on web."""
    db = client._transport.app.state.db
    auth = client._transport.app.state.auth

    with patch.object(auth, "send_code", new_callable=AsyncMock) as mock_send:
        mock_send.side_effect = Exception("Network error")

        resp = await client.post(
            "/auth/send-code",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        mock_send.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "auth.send_code"


@pytest.mark.asyncio
async def test_resend_code_success(client):
    """Test resend code is enqueued instead of executed inline."""
    db = client._transport.app.state.db
    auth = client._transport.app.state.auth

    with patch.object(auth, "resend_code", new_callable=AsyncMock) as mock_resend:
        resp = await client.post(
            "/auth/resend-code",
            data={"phone": "+1234567890", "phone_code_hash": "old_hash"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        mock_resend.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "auth.resend_code"
    assert commands[0].payload["phone_code_hash"] == "old_hash"


@pytest.mark.asyncio
async def test_resend_code_error(client):
    """Test resend code route does not call TelegramAuth inline on web."""
    db = client._transport.app.state.db
    auth = client._transport.app.state.auth

    with patch.object(auth, "resend_code", new_callable=AsyncMock) as mock_resend:
        mock_resend.side_effect = Exception("Flood wait")

        resp = await client.post(
            "/auth/resend-code",
            data={"phone": "+1234567890", "phone_code_hash": "hash"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        mock_resend.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "auth.resend_code"


@pytest.mark.asyncio
async def test_verify_code_success(client):
    """Test verify code is enqueued instead of executed inline."""
    auth = client._transport.app.state.auth
    db = client._transport.app.state.db

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        resp = await client.post(
            "/auth/verify-code",
            data={
                "phone": "+1234567890",
                "code": "12345",
                "phone_code_hash": "hash123",
                "password_2fa": "",
                "code_type": "sms",
                "next_type": "call",
                "timeout": "60",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "/auth/login?command_id=" in resp.headers.get("location", "")
        mock_verify.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "auth.verify_code"
    assert commands[0].payload["phone"] == "+1234567890"
    assert commands[0].payload["password_2fa"] == ""


@pytest.mark.asyncio
async def test_verify_code_2fa_required(client):
    """Test login page shows 2FA form for failed queued verify command."""
    db = client._transport.app.state.db
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="auth.verify_code",
            payload={"phone": "+1234567890", "code": "12345", "phone_code_hash": "hash123"},
        )
    )
    await db.repos.telegram_commands.update_command(
        command_id,
        status=TelegramCommandStatus.FAILED,
        error="2FA password required",
    )
    resp = await client.get(f"/auth/login?command_id={command_id}")
    assert resp.status_code == 200
    assert "2fa" in resp.text.lower() or "password" in resp.text.lower()


@pytest.mark.asyncio
async def test_verify_code_with_2fa(client):
    """Test verify code enqueues 2FA password for worker processing."""
    db = client._transport.app.state.db
    resp = await client.post(
        "/auth/verify-code",
        data={
            "phone": "+1234567890",
            "code": "12345",
            "phone_code_hash": "hash123",
            "password_2fa": "mypassword",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].payload["password_2fa"] == "mypassword"


@pytest.mark.asyncio
async def test_verify_code_invalid_code(client):
    """Test login page surfaces invalid-code error from failed queued command."""
    db = client._transport.app.state.db
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="auth.verify_code",
            payload={
                "phone": "+1234567890",
                "code": "00000",
                "phone_code_hash": "hash123",
                "code_type": "sms",
                "next_type": "call",
                "timeout": "60",
            },
        )
    )
    await db.repos.telegram_commands.update_command(
        command_id,
        status=TelegramCommandStatus.FAILED,
        error="Invalid code",
    )
    resp = await client.get(f"/auth/login?command_id={command_id}")
    assert resp.status_code == 200
    assert "Invalid code" in resp.text


@pytest.mark.asyncio
async def test_verify_code_generic_error(client):
    """Test login page surfaces generic verify error from failed queued command."""
    db = client._transport.app.state.db
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="auth.verify_code",
            payload={"phone": "+1234567890", "code": "12345", "phone_code_hash": "hash123"},
        )
    )
    await db.repos.telegram_commands.update_command(
        command_id,
        status=TelegramCommandStatus.FAILED,
        error="Connection lost",
    )
    resp = await client.get(f"/auth/login?command_id={command_id}")
    assert resp.status_code == 200
    assert "Connection lost" in resp.text


@pytest.mark.asyncio
async def test_verify_code_sets_primary(client):
    """Test verify_code command reflects existing DB primary-state context."""
    db = client._transport.app.state.db
    await client.post(
        "/auth/verify-code",
        data={
            "phone": "+1234567890",
            "code": "12345",
            "phone_code_hash": "hash",
            "password_2fa": "",
        },
        follow_redirects=False,
    )
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].payload["is_primary"] is False


@pytest.mark.asyncio
async def test_verify_code_detects_premium(client):
    """Test verify code route no longer mutates accounts synchronously."""
    db = client._transport.app.state.db
    await client.post(
        "/auth/verify-code",
        data={
            "phone": "+1234567890",
            "code": "12345",
            "phone_code_hash": "hash",
            "password_2fa": "",
        },
        follow_redirects=False,
    )
    accounts = await db.get_accounts()
    assert len(accounts) == 1


@pytest.mark.asyncio
async def test_verify_code_timeout_parsing(client):
    """Test login page tolerates non-numeric timeout from failed queued command."""
    db = client._transport.app.state.db
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="auth.verify_code",
            payload={
                "phone": "+1234567890",
                "code": "12345",
                "phone_code_hash": "hash",
                "timeout": "not_a_number",
            },
            status=TelegramCommandStatus.FAILED,
            error="Code expired",
        )
    )
    resp = await client.get(f"/auth/login?command_id={command_id}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_send_code_returns_code_info(client):
    """Test login page renders code-step data from successful queued command."""
    db = client._transport.app.state.db
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="auth.send_code",
            payload={"phone": "+1234567890"},
            status=TelegramCommandStatus.SUCCEEDED,
            result_payload={
                "phone": "+1234567890",
                "phone_code_hash": "hash123",
                "code_type": "app",
                "next_type": "sms",
                "timeout": 30,
            },
        )
    )
    resp = await client.get(f"/auth/login?command_id={command_id}")
    assert resp.status_code == 200
    text_lower = resp.text.lower()
    assert "code" in text_lower or "код" in text_lower


@pytest.mark.asyncio
async def test_login_page_api_configured_flag(client):
    """Test login page correctly detects API configuration."""
    resp = await client.get("/auth/login")
    assert resp.status_code == 200
    # When configured, should show phone step
    assert "phone" in resp.text.lower()


@pytest.mark.asyncio
async def test_save_credentials_validates_input(client_unconfigured):
    """Test save credentials accepts form data."""
    resp = await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_id": "99999", "api_hash": "abcdef123456"},
    )
    # Should redirect (success)
    assert resp.status_code == 200 or resp.status_code == 303


@pytest.mark.asyncio
async def test_verify_code_empty_password(client):
    """Test empty 2FA password is preserved as empty string in queued payload."""
    db = client._transport.app.state.db
    await client.post(
        "/auth/verify-code",
        data={
            "phone": "+1234567890",
            "code": "12345",
            "phone_code_hash": "hash",
            "password_2fa": "",
        },
        follow_redirects=False,
    )
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].payload["password_2fa"] == ""


@pytest.mark.asyncio
async def test_verify_code_get_me_error(client):
    """Test verify_code route itself is unaffected by later worker-side premium refresh."""
    resp = await client.post(
        "/auth/verify-code",
        data={
            "phone": "+1234567890",
            "code": "12345",
            "phone_code_hash": "hash",
            "password_2fa": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
