"""Tests for src/web/routes/auth.py - web authentication routes.

Note: /auth/* routes require web panel authentication (not to be confused
with /login which is for the web panel itself).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.models import Account
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
        """Test successful code sending."""
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
            return_value={
                "phone_code_hash": "abc123",
                "code_type": "SMS",
                "next_type": "call",
                "timeout": 120,
            },
        ):
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/send-code",
                    data={"phone": "+70001112233"},
                )

        assert response.status_code == 200
        # Should show code input step
        assert "code" in response.text.lower() or "код" in response.text.lower()

    @pytest.mark.asyncio
    async def test_send_code_exception(self, db, real_pool_harness_factory):
        """Test send-code handles exceptions gracefully."""
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
        ):
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/send-code",
                    data={"phone": "+70001112233"},
                )

        assert response.status_code == 200
        # Should show error message
        assert "Phone number invalid" in response.text


class TestResendCode:
    """Tests for POST /auth/resend-code."""

    @pytest.mark.asyncio
    async def test_resend_code_success(self, db, real_pool_harness_factory):
        """Test successful code resend."""
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
            return_value={
                "phone_code_hash": "xyz789",
                "code_type": "call",
                "next_type": None,
                "timeout": 60,
            },
        ):
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/resend-code",
                    data={"phone": "+70001112233", "phone_code_hash": "abc123"},
                )

        assert response.status_code == 200
        assert "code" in response.text.lower() or "код" in response.text.lower()

    @pytest.mark.asyncio
    async def test_resend_code_exception(self, db, real_pool_harness_factory):
        """Test resend-code handles exceptions gracefully."""
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
        ):
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/resend-code",
                    data={"phone": "+70001112233", "phone_code_hash": "abc123"},
                )

        assert response.status_code == 200
        assert "Rate limit exceeded" in response.text


class TestVerifyCode:
    """Tests for POST /auth/verify-code."""

    @pytest.mark.asyncio
    async def test_verify_code_success(self, db, real_pool_harness_factory):
        """Test successful code verification."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        # Mock pool.add_client to capture the session
        added_sessions = []

        async def mock_add_client(phone, session_string):
            added_sessions.append((phone, session_string))

        app.state.pool.add_client = mock_add_client

        # Mock pool.get_client_by_phone and release_client
        mock_client = MagicMock()
        mock_client.fetch_me = AsyncMock(return_value=SimpleNamespace(premium=False))
        app.state.pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+70001112233"))
        app.state.pool.release_client = AsyncMock()

        with patch.object(
            auth,
            "verify_code",
            new_callable=AsyncMock,
            return_value="session_string_123",
        ):
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
        assert "settings" in response.headers["location"]

        # Verify account was added to DB
        accounts = await db.get_accounts()
        assert len(accounts) == 1
        assert accounts[0].phone == "+70001112233"
        assert accounts[0].is_primary is True  # First account is primary

    @pytest.mark.asyncio
    async def test_verify_code_2fa_required(self, db, real_pool_harness_factory):
        """Test code verification when 2FA password is required."""
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
            side_effect=ValueError("2FA password required"),
        ):
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
                )

        assert response.status_code == 200
        # Should show 2FA password input
        assert "2fa" in response.text.lower() or "password" in response.text.lower()

    @pytest.mark.asyncio
    async def test_verify_code_invalid_code(self, db, real_pool_harness_factory):
        """Test code verification with invalid code."""
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
            side_effect=ValueError("Invalid code"),
        ):
            async with make_auth_client(app, password="testpass", with_auth=True) as client:
                response = await client.post(
                    "/auth/verify-code",
                    data={
                        "phone": "+70001112233",
                        "code": "00000",
                        "phone_code_hash": "abc123",
                        "password_2fa": "",
                        "code_type": "SMS",
                        "next_type": "call",
                        "timeout": "120",
                    },
                )

        assert response.status_code == 200
        assert "Invalid code" in response.text

    @pytest.mark.asyncio
    async def test_verify_code_exception(self, db, real_pool_harness_factory):
        """Test code verification handles general exceptions."""
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
            side_effect=Exception("Flood wait: 60 seconds"),
        ):
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
                )

        assert response.status_code == 200
        assert "Flood wait" in response.text

    @pytest.mark.asyncio
    async def test_verify_code_with_2fa_password(self, db, real_pool_harness_factory):
        """Test code verification with 2FA password provided."""
        config = AppConfig()
        config.web.password = "testpass"
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash"

        harness = real_pool_harness_factory()
        auth = TelegramAuth(12345, "test_hash")
        app, _ = await build_web_app(config, harness, db=db, add_account=None)
        app.state.auth = auth

        # Mock pool methods
        async def mock_add_client(phone, session_string):
            pass

        app.state.pool.add_client = mock_add_client
        mock_client = MagicMock()
        mock_client.fetch_me = AsyncMock(return_value=SimpleNamespace(premium=True))
        app.state.pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+70001112233"))
        app.state.pool.release_client = AsyncMock()

        with patch.object(
            auth,
            "verify_code",
            new_callable=AsyncMock,
            return_value="session_string_with_2fa",
        ) as mock_verify:
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
        # Verify 2FA password was passed to verify_code
        mock_verify.assert_awaited_once_with("+70001112233", "12345", "abc123", "my2fapassword")

        # Verify premium status was fetched
        accounts = await db.get_accounts()
        assert len(accounts) == 1
        assert accounts[0].is_premium is True

    @pytest.mark.asyncio
    async def test_verify_code_existing_account_not_primary(self, db, real_pool_harness_factory):
        """Test that subsequent accounts are not marked as primary."""
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

        # Mock pool methods
        async def mock_add_client(phone, session_string):
            pass

        app.state.pool.add_client = mock_add_client
        mock_client = MagicMock()
        mock_client.fetch_me = AsyncMock(return_value=SimpleNamespace(premium=False))
        app.state.pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+70001112233"))
        app.state.pool.release_client = AsyncMock()

        with patch.object(
            auth,
            "verify_code",
            new_callable=AsyncMock,
            return_value="second_session",
        ):
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

        # Verify second account is NOT primary
        accounts = await db.get_accounts()
        assert len(accounts) == 2
        new_account = next(a for a in accounts if a.phone == "+70001112233")
        assert new_account.is_primary is False
