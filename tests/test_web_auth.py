"""Tests for src/web/routes/auth.py - web authentication routes.

Note: /auth/* routes require web panel authentication (not to be confused
with /login which is for the web panel itself).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.config import AppConfig
from src.models import Account, TelegramCommand, TelegramCommandStatus
from src.telegram.auth import TelegramAuth
from tests.helpers import build_web_app, make_auth_client


class TestLoginPage:
    """Tests for GET /auth/login (Telegram account login page)."""

    @pytest.mark.asyncio
    async def test_login_page_api_configured(self, db, real_pool_harness_factory):
        """Test login page when API credentials are configured."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        # Use with_auth=True since /auth/* requires web panel authentication
        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.get("/auth/login")

        assert response.status_code == 200
        # When API is configured, step should be "phone"
        assert "phone" in response.text.lower() or "телефон" in response.text.lower()

    @pytest.mark.asyncio
    async def test_login_page_api_not_configured(self, db, real_pool_harness_factory):
        """Test login page when API credentials are not configured."""
        config = AppConfig()
        config.web.password = "testpass"
        # API not configured (default values)
        config.telegram.api_id = 0
        config.telegram.api_hash = ""

        harness = real_pool_harness_factory()
        auth = TelegramAuth(0, "")  # Not configured
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.get("/auth/login")

        assert response.status_code == 200
        # When API is not configured, step should be "credentials"
        assert "credentials" in response.text or "api_id" in response.text


class TestSaveCredentials:
    """Tests for POST /auth/save-credentials."""

    @pytest.mark.asyncio
    async def test_save_credentials_success(self, db, real_pool_harness_factory):
        """Test successful credential save."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 0
        config.telegram.api_hash = ""

        harness = real_pool_harness_factory()
        auth = TelegramAuth(0, "")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.post(
                "/auth/save-credentials",
                data={"api_id": "12345", "api_hash": "my_api_hash"},
                follow_redirects=False,
            )

        assert response.status_code == 303
        assert response.headers["location"] == "/auth/login"

        # Verify credentials were saved to DB
        saved_id = await db.get_setting("tg_api_id")
        saved_hash = await db.get_setting("tg_api_hash")
        assert saved_id == "12345"
        assert saved_hash == "my_api_hash"

        # Verify auth object was updated
        assert auth.api_id == 12345
        assert auth.api_hash == "my_api_hash"


class TestSendCode:
    """Tests for POST /auth/send-code."""

    @pytest.mark.asyncio
    async def test_send_code_api_not_configured(self, db, real_pool_harness_factory):
        """Test send-code returns error when API credentials are not configured."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 0
        config.telegram.api_hash = ""

        harness = real_pool_harness_factory()
        auth = TelegramAuth(0, "")  # Not configured
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.post(
                "/auth/send-code",
                data={"phone": "+70001112233"},
            )

        assert response.status_code == 200
        # Should show credentials step with error
        assert "API credentials" in response.text or "api_id" in response.text

    @pytest.mark.asyncio
    async def test_send_code_success(self, db, real_pool_harness_factory):
        """Test send-code enqueues command instead of direct RPC."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        with patch.object(auth, "send_code", new_callable=AsyncMock) as mock_send:
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/send-code",
                    data={"phone": "+70001112233"},
                    follow_redirects=False,
                )

        assert response.status_code == 303
        mock_send.assert_not_awaited()
        commands = await db.repos.telegram_commands.list_commands(limit=1)
        assert commands[0].command_type == "auth.send_code"

    @pytest.mark.asyncio
    async def test_send_code_exception(self, db, real_pool_harness_factory):
        """Test send-code route does not execute auth inline."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        with patch.object(
            auth,
            "send_code",
            new_callable=AsyncMock,
            side_effect=Exception("Phone number invalid"),
        ) as mock_send:
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/send-code",
                    data={"phone": "+70001112233"},
                    follow_redirects=False,
                )

        assert response.status_code == 303
        mock_send.assert_not_awaited()


class TestResendCode:
    """Tests for POST /auth/resend-code."""

    @pytest.mark.asyncio
    async def test_resend_code_success(self, db, real_pool_harness_factory):
        """Test resend-code enqueues command instead of direct RPC."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        with patch.object(auth, "resend_code", new_callable=AsyncMock) as mock_resend:
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/resend-code",
                    data={"phone": "+70001112233", "phone_code_hash": "abc123"},
                    follow_redirects=False,
                )

        assert response.status_code == 303
        mock_resend.assert_not_awaited()
        commands = await db.repos.telegram_commands.list_commands(limit=1)
        assert commands[0].command_type == "auth.resend_code"

    @pytest.mark.asyncio
    async def test_resend_code_exception(self, db, real_pool_harness_factory):
        """Test resend-code route does not execute auth inline."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        with patch.object(
            auth,
            "resend_code",
            new_callable=AsyncMock,
            side_effect=Exception("Rate limit exceeded"),
        ) as mock_resend:
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/resend-code",
                    data={"phone": "+70001112233", "phone_code_hash": "abc123"},
                    follow_redirects=False,
                )

        assert response.status_code == 303
        mock_resend.assert_not_awaited()


class TestVerifyCode:
    """Tests for POST /auth/verify-code."""

    @pytest.mark.asyncio
    async def test_verify_code_success(self, db, real_pool_harness_factory):
        """Test verify-code enqueues command instead of direct RPC."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        with patch.object(
            auth,
            "verify_code",
            new_callable=AsyncMock,
        ) as mock_verify:
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/verify-code",
                    data={
                        "phone": "+70001112233",
                        "code": "12345",
                        "phone_code_hash": "abc123",
                        "password_2fa": "",
                        "code_type": "",
                        "next_type": "",
                        "timeout": "",
                    },
                    follow_redirects=False,
                )

        assert response.status_code == 303
        assert "command_id=" in response.headers["location"]
        mock_verify.assert_not_awaited()
        commands = await db.repos.telegram_commands.list_commands(limit=1)
        assert commands[0].command_type == "auth.verify_code"

    @pytest.mark.asyncio
    async def test_verify_code_2fa_required(self, db, real_pool_harness_factory):
        """Test login page shows 2FA state from failed queued command."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        cid = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="auth.verify_code",
                payload={"phone": "+70001112233", "code": "12345", "phone_code_hash": "abc123"},
                status=TelegramCommandStatus.FAILED,
                error="2FA password required",
            )
        )
        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.get(f"/auth/login?command_id={cid}")

        assert response.status_code == 200
        assert "2fa" in response.text.lower() or "password" in response.text.lower()

    @pytest.mark.asyncio
    async def test_verify_code_invalid_code(self, db, real_pool_harness_factory):
        """Test login page surfaces invalid-code error from failed queued command."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        cid = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="auth.verify_code",
                payload={
                    "phone": "+70001112233",
                    "code": "00000",
                    "phone_code_hash": "abc123",
                    "code_type": "SMS",
                    "next_type": "call",
                    "timeout": "120",
                },
            )
        )
        await db.repos.telegram_commands.update_command(
            cid,
            status=TelegramCommandStatus.FAILED,
            error="Invalid code",
        )
        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.get(f"/auth/login?command_id={cid}")

        assert response.status_code == 200
        assert "Invalid code" in response.text

    @pytest.mark.asyncio
    async def test_verify_code_exception(self, db, real_pool_harness_factory):
        """Test login page surfaces generic queued verify error."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        cid = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="auth.verify_code",
                payload={
                    "phone": "+70001112233",
                    "code": "12345",
                    "phone_code_hash": "abc123",
                },
            )
        )
        await db.repos.telegram_commands.update_command(
            cid,
            status=TelegramCommandStatus.FAILED,
            error="Flood wait: 60 seconds",
        )
        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.get(f"/auth/login?command_id={cid}")

        assert response.status_code == 200
        assert "Flood wait" in response.text

    @pytest.mark.asyncio
    async def test_verify_code_with_2fa_password(self, db, real_pool_harness_factory):
        """Test verify-code enqueues 2FA password for worker processing."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.post(
                "/auth/verify-code",
                data={
                    "phone": "+70001112233",
                    "code": "12345",
                    "phone_code_hash": "abc123",
                    "password_2fa": "my2fapassword",
                    "code_type": "",
                    "next_type": "",
                    "timeout": "",
                },
                follow_redirects=False,
            )

        assert response.status_code == 303
        commands = await db.repos.telegram_commands.list_commands(limit=1)
        assert commands[0].payload["password_2fa"] == "my2fapassword"

    @pytest.mark.asyncio
    async def test_verify_code_existing_account_not_primary(self, db, real_pool_harness_factory):
        """Test queued verify-code carries non-primary context for later accounts."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        # Add first account
        await db.add_account(
            Account(phone="+70001110000", session_string="first_session", is_primary=True)
        )

        async with make_auth_client(app, password="testpass", with_auth=True) as client:
            response = await client.post(
                "/auth/verify-code",
                data={
                    "phone": "+70001112233",
                    "code": "12345",
                    "phone_code_hash": "abc123",
                    "password_2fa": "",
                    "code_type": "",
                    "next_type": "",
                    "timeout": "",
                },
                follow_redirects=False,
            )

        assert response.status_code == 303
        commands = await db.repos.telegram_commands.list_commands(limit=1)
        assert "is_primary" not in commands[0].payload  # worker recomputes from fresh DB state
