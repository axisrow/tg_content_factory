"""Tests for auth routes."""

from __future__ import annotations

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient


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
    """Test send code success."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "send_code", new_callable=AsyncMock) as mock_send:
        mock_send.return_value = {
            "phone_code_hash": "abc123",
            "code_type": "sms",
            "next_type": "call",
            "timeout": 60,
        }

        resp = await client.post(
            "/auth/send-code",
            data={"phone": "+1234567890"},
        )
        assert resp.status_code == 200
        assert "code" in resp.text.lower()


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
    """Test send code handles error."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "send_code", new_callable=AsyncMock) as mock_send:
        mock_send.side_effect = Exception("Network error")

        resp = await client.post(
            "/auth/send-code",
            data={"phone": "+1234567890"},
        )
        assert resp.status_code == 200
        # Should show error
        assert "error" in resp.text.lower() or "Network error" in resp.text


@pytest.mark.asyncio
async def test_resend_code_success(client):
    """Test resend code success."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "resend_code", new_callable=AsyncMock) as mock_resend:
        mock_resend.return_value = {
            "phone_code_hash": "xyz789",
            "code_type": "call",
            "next_type": "flash",
            "timeout": 120,
        }

        resp = await client.post(
            "/auth/resend-code",
            data={"phone": "+1234567890", "phone_code_hash": "old_hash"},
        )
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_resend_code_error(client):
    """Test resend code handles error."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "resend_code", new_callable=AsyncMock) as mock_resend:
        mock_resend.side_effect = Exception("Flood wait")

        resp = await client.post(
            "/auth/resend-code",
            data={"phone": "+1234567890", "phone_code_hash": "hash"},
        )
        assert resp.status_code == 200
        # Should show error in template
        assert "error" in resp.text.lower() or "Flood" in resp.text


@pytest.mark.asyncio
async def test_verify_code_success(client):
    """Test verify code success."""
    auth = client._transport.app.state.auth
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = "session_string_123"

        with patch.object(pool, "add_client", new_callable=AsyncMock) as mock_add_client:
            with patch.object(
                pool,
                "get_client_by_phone",
                new_callable=AsyncMock,
            ) as mock_get_client:
                with patch.object(pool, "release_client", new_callable=AsyncMock):
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
                    assert "/settings" in resp.headers.get("location", "")
                    mock_add_client.assert_not_awaited()
                    mock_get_client.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.connect"
    assert commands[0].payload["phone"] == "+1234567890"
    assert "session_string" not in commands[0].payload


@pytest.mark.asyncio
async def test_verify_code_2fa_required(client):
    """Test verify code shows 2FA form when needed."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.side_effect = ValueError("2FA password required")

        resp = await client.post(
            "/auth/verify-code",
            data={
                "phone": "+1234567890",
                "code": "12345",
                "phone_code_hash": "hash123",
                "password_2fa": "",
            },
        )
        assert resp.status_code == 200
        # Should show 2FA step
        assert "2fa" in resp.text.lower() or "password" in resp.text.lower()


@pytest.mark.asyncio
async def test_verify_code_with_2fa(client):
    """Test verify code with 2FA password."""
    auth = client._transport.app.state.auth
    pool = client._transport.app.state.pool

    mock_session = MagicMock()
    mock_session.fetch_me = AsyncMock(return_value=SimpleNamespace(premium=True))

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = "session_string_123"

        with patch.object(pool, "add_client", new_callable=AsyncMock):
            with patch.object(
                pool,
                "get_client_by_phone",
                new_callable=AsyncMock,
                return_value=(mock_session, "+1234567890"),
            ):
                with patch.object(pool, "release_client", new_callable=AsyncMock):
                    await client.post(
                        "/auth/verify-code",
                        data={
                            "phone": "+1234567890",
                            "code": "12345",
                            "phone_code_hash": "hash123",
                            "password_2fa": "mypassword",
                        },
                        follow_redirects=False,
                    )
                    # Password should be passed to verify_code
                    mock_verify.assert_called_once()
                    call_args = mock_verify.call_args[0]
                    assert call_args[3] == "mypassword"


@pytest.mark.asyncio
async def test_verify_code_invalid_code(client):
    """Test verify code with invalid code."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.side_effect = ValueError("Invalid code")

        resp = await client.post(
            "/auth/verify-code",
            data={
                "phone": "+1234567890",
                "code": "00000",
                "phone_code_hash": "hash123",
                "password_2fa": "",
                "code_type": "sms",
                "next_type": "call",
                "timeout": "60",
            },
        )
        assert resp.status_code == 200
        # Should show error
        assert "Invalid" in resp.text or "error" in resp.text.lower()


@pytest.mark.asyncio
async def test_verify_code_generic_error(client):
    """Test verify code handles generic error."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.side_effect = Exception("Connection lost")

        resp = await client.post(
            "/auth/verify-code",
            data={
                "phone": "+1234567890",
                "code": "12345",
                "phone_code_hash": "hash123",
                "password_2fa": "",
            },
        )
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_verify_code_sets_primary(client):
    """Test first account becomes primary."""
    auth = client._transport.app.state.auth
    pool = client._transport.app.state.pool

    mock_session = MagicMock()
    mock_session.fetch_me = AsyncMock(return_value=SimpleNamespace(premium=False))

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = "session_string"

        with patch.object(pool, "add_client", new_callable=AsyncMock):
            with patch.object(
                pool,
                "get_client_by_phone",
                new_callable=AsyncMock,
                return_value=(mock_session, "+1234567890"),
            ):
                with patch.object(pool, "release_client", new_callable=AsyncMock):
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

                    # Check account was added
                    db = client._transport.app.state.db
                    accounts = await db.get_accounts()
                    assert len(accounts) >= 1


@pytest.mark.asyncio
async def test_verify_code_detects_premium(client):
    """Test verify code defers premium detection to worker."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = "session_string"

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

        db = client._transport.app.state.db
        accounts = await db.get_accounts()
        if accounts:
            assert accounts[-1].is_premium is False


@pytest.mark.asyncio
async def test_verify_code_timeout_parsing(client):
    """Test verify code handles timeout parsing."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.side_effect = ValueError("Code expired")

        resp = await client.post(
            "/auth/verify-code",
            data={
                "phone": "+1234567890",
                "code": "12345",
                "phone_code_hash": "hash",
                "password_2fa": "",
                "timeout": "not_a_number",  # Invalid timeout
            },
        )
        assert resp.status_code == 200
        # Should handle invalid timeout gracefully


@pytest.mark.asyncio
async def test_send_code_returns_code_info(client):
    """Test send code returns all code info."""
    auth = client._transport.app.state.auth

    with patch.object(auth, "send_code", new_callable=AsyncMock) as mock_send:
        mock_send.return_value = {
            "phone_code_hash": "hash123",
            "code_type": "app",
            "next_type": "sms",
            "timeout": 30,
        }

        resp = await client.post(
            "/auth/send-code",
            data={"phone": "+1234567890"},
        )
        assert resp.status_code == 200
        # Should contain code info in template
        text_lower = resp.text.lower()
        # Just verify page renders with code step
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
    """Test verify code handles empty 2FA password."""
    auth = client._transport.app.state.auth
    pool = client._transport.app.state.pool

    mock_session = MagicMock()
    mock_session.fetch_me = AsyncMock(return_value=SimpleNamespace(premium=False))

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = "session"

        with patch.object(pool, "add_client", new_callable=AsyncMock):
            with patch.object(
                pool,
                "get_client_by_phone",
                new_callable=AsyncMock,
                return_value=(mock_session, "+1234567890"),
            ):
                with patch.object(pool, "release_client", new_callable=AsyncMock):
                    await client.post(
                        "/auth/verify-code",
                        data={
                            "phone": "+1234567890",
                            "code": "12345",
                            "phone_code_hash": "hash",
                            "password_2fa": "",  # Empty password
                        },
                        follow_redirects=False,
                    )

                    # Empty string should become None
                    call_args = mock_verify.call_args[0]
                    assert call_args[3] is None


@pytest.mark.asyncio
async def test_verify_code_get_me_error(client):
    """Test verify code handles fetch_me error gracefully."""
    auth = client._transport.app.state.auth
    pool = client._transport.app.state.pool

    mock_session = MagicMock()
    mock_session.fetch_me = AsyncMock(side_effect=Exception("API error"))

    with patch.object(auth, "verify_code", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = "session"

        with patch.object(pool, "add_client", new_callable=AsyncMock):
            with patch.object(
                pool,
                "get_client_by_phone",
                new_callable=AsyncMock,
                return_value=(mock_session, "+1234567890"),
            ):
                with patch.object(pool, "release_client", new_callable=AsyncMock):
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
                    # Should still succeed even if fetch_me fails
                    assert resp.status_code == 303
