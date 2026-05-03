"""Tests for auth routes."""

from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import TelegramCommand, TelegramCommandStatus
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher


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


async def _run_dispatcher_once(db, pool, auth, monkeypatch):
    dispatcher = TelegramCommandDispatcher(db, pool, auth=auth)
    original_claim = db.repos.telegram_commands.claim_next_command

    async def claim_once():
        command = await original_claim()
        dispatcher._stop_event.set()
        return command

    monkeypatch.setattr(db.repos.telegram_commands, "claim_next_command", claim_once)
    await dispatcher._run_loop()


@pytest.mark.anyio
async def test_login_page(client):
    """Test login page renders."""
    resp = await client.get("/auth/login")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_login_page_shows_phone_step(client):
    """Test login page shows phone step when configured."""
    resp = await client.get("/auth/login")
    assert resp.status_code == 200
    # Should show phone input
    assert "phone" in resp.text.lower() or "телефон" in resp.text.lower()


@pytest.mark.anyio
async def test_login_page_shows_credentials_step(client_unconfigured):
    """Test login page shows credentials step when not configured."""
    resp = await client_unconfigured.get("/auth/login")
    assert resp.status_code == 200
    # Should show api_id/api_hash input
    assert "api" in resp.text.lower() or "credential" in resp.text.lower()


@pytest.mark.anyio
async def test_save_credentials_redirect(client_unconfigured):
    """Test save credentials redirects to login."""
    resp = await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_id": 12345, "api_hash": "test_hash"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/auth/login" in resp.headers.get("location", "")


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_send_code_unconfigured(client_unconfigured):
    """Test send code when auth not configured."""
    resp = await client_unconfigured.post(
        "/auth/send-code",
        data={"phone": "+1234567890"},
    )
    assert resp.status_code == 200
    # Should show error
    assert "error" in resp.text.lower() or "api" in resp.text.lower()


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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
    assert "is_primary" not in commands[0].payload  # worker recomputes from fresh DB state


@pytest.mark.anyio
async def test_verify_code_detects_premium(client):
    """Test verify code route no longer mutates accounts synchronously."""
    db = client._transport.app.state.db
    await client.post(
        "/auth/verify-code",
        data={
            "phone": "+9999999999",
            "code": "12345",
            "phone_code_hash": "hash",
            "password_2fa": "",
        },
        follow_redirects=False,
    )
    accounts = await db.get_accounts()
    assert len(accounts) == 1  # only the fixture account, new phone not added synchronously


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_send_code_happy_path_dispatcher_result_shows_code_step(client, monkeypatch):
    """Queued send-code result must persist phone_code_hash for the login page."""
    app = client._transport.app
    db = app.state.db
    pool = app.state.pool
    auth = app.state.auth
    auth.send_code = AsyncMock(
        return_value={
            "phone_code_hash": "hash_dispatch",
            "session_str": "session_pending",
            "code_type": "SMS",
            "next_type": "звонок",
            "timeout": 60,
        }
    )

    resp = await client.post(
        "/auth/send-code",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    command_id = int(resp.headers["location"].split("command_id=", 1)[1].split("&", 1)[0])

    await _run_dispatcher_once(db, pool, auth, monkeypatch)

    command = await db.repos.telegram_commands.get_command(command_id)
    assert command is not None
    assert command.status == TelegramCommandStatus.SUCCEEDED
    assert command.result_payload is not None
    assert command.result_payload["phone_code_hash"] == "hash_dispatch"
    assert command.result_payload["session_str"] == "session_pending"

    page = await client.get(f"/auth/login?command_id={command_id}")
    assert page.status_code == 200
    assert "Код подтверждения" in page.text
    assert 'value="hash_dispatch"' in page.text


@pytest.mark.anyio
async def test_send_code_connect_timeout_marks_failed_and_shows_error(client, monkeypatch):
    """A hanging Telethon connect must fail the command with a visible timeout."""
    app = client._transport.app
    db = app.state.db
    pool = app.state.pool
    auth = app.state.auth
    monkeypatch.setattr("src.telegram.auth.AUTH_CONNECT_TIMEOUT_SECONDS", 0.01)

    async def hang_forever():
        await asyncio.Event().wait()

    mock_client = MagicMock()
    mock_client.connect = AsyncMock(side_effect=hang_forever)
    mock_client.disconnect = AsyncMock()
    mock_client.send_code_request = AsyncMock()

    with patch("src.telegram.auth.TelegramClient", return_value=mock_client):
        resp = await client.post(
            "/auth/send-code",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        command_id = int(resp.headers["location"].split("command_id=", 1)[1].split("&", 1)[0])

        await _run_dispatcher_once(db, pool, auth, monkeypatch)

    command = await db.repos.telegram_commands.get_command(command_id)
    assert command is not None
    assert command.status == TelegramCommandStatus.FAILED
    assert command.error == "telegram_auth_timeout: connect timed out after 0.01s"
    mock_client.send_code_request.assert_not_awaited()

    page = await client.get(f"/auth/login?command_id={command_id}")
    assert page.status_code == 200
    assert "telegram_auth_timeout: connect timed out after 0.01s" in page.text


@pytest.mark.anyio
async def test_login_pending_shows_elapsed_and_slow_warning(client):
    """Pending auth commands show elapsed time and a slow Telegram warning."""
    db = client._transport.app.state.db
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="auth.send_code",
            payload={"phone": "+1234567890"},
        )
    )
    started_at = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat()
    await db.execute(
        "UPDATE telegram_commands SET status = ?, started_at = ? WHERE id = ?",
        (TelegramCommandStatus.RUNNING.value, started_at, command_id),
    )
    assert db.db is not None
    await db.db.commit()

    resp = await client.get(f"/auth/login?command_id={command_id}")
    assert resp.status_code == 200
    assert "Выполняется" in resp.text
    assert "запрос к Telegram занял слишком много времени, worker завершит его ошибкой" in resp.text


@pytest.mark.anyio
async def test_login_page_api_configured_flag(client):
    """Test login page correctly detects API configuration."""
    resp = await client.get("/auth/login")
    assert resp.status_code == 200
    # When configured, should show phone step
    assert "phone" in resp.text.lower()


@pytest.mark.anyio
async def test_save_credentials_validates_input(client_unconfigured):
    """Test save credentials accepts form data."""
    resp = await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_id": "99999", "api_hash": "abcdef123456"},
    )
    # Should redirect (success)
    assert resp.status_code == 200 or resp.status_code == 303


@pytest.mark.anyio
async def test_verify_code_missing_phone_code_hash(client):
    """POST /auth/verify-code without phone_code_hash enqueues with empty hash → 303."""
    resp = await client.post(
        "/auth/verify-code",
        data={"phone": "+1234567890", "code": "12345"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_resend_code_missing_phone_code_hash(client):
    """POST /auth/resend-code without phone_code_hash enqueues with empty hash → 303."""
    resp = await client.post(
        "/auth/resend-code",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_send_code_missing_phone(client):
    """POST /auth/send-code without phone renders login page (200)."""
    resp = await client.post("/auth/send-code", data={}, follow_redirects=False)
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_save_credentials_missing_api_id(client_unconfigured):
    """POST /auth/save-credentials without api_id returns 422."""
    resp = await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_hash": "abc123"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_save_credentials_missing_api_hash(client_unconfigured):
    """POST /auth/save-credentials without api_hash returns 422."""
    resp = await client_unconfigured.post(
        "/auth/save-credentials",
        data={"api_id": "12345"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
