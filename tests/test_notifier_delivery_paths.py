"""Tests for notifier delivery paths, error handling, and bot API integration."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database.bundles import NotificationBundle
from src.models import NotificationBot
from src.telegram.notifier import Notifier, _send_via_bot_api


@pytest.fixture
def mock_target_service():
    """Create a mock NotificationTargetService."""
    service = MagicMock()
    service.use_client = MagicMock()
    return service


@pytest.fixture
def mock_notification_bundle():
    """Create a mock NotificationBundle."""
    bundle = MagicMock(spec=NotificationBundle)
    bundle.get_bot = AsyncMock(return_value=None)
    return bundle


class TestNotifierFastPath:
    """Tests for Notifier fast path with cached me.id and bot."""

    @pytest.mark.anyio
    async def test_notify_with_cached_me_id_and_bot(
        self,
        mock_target_service,
        mock_notification_bundle,
    ):
        """Test fast path when me.id is cached and bot is available."""
        bot = NotificationBot(
            tg_user_id=123,
            tg_username="test_user",
            bot_id=456,
            bot_username="test_bot",
            bot_token="123456:ABC-DEF",
        )
        mock_notification_bundle.get_bot = AsyncMock(return_value=bot)

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        # Simulate cached me.id
        notifier._cached_me_id = 123

        with patch(
            "src.telegram.notifier._send_via_bot_api",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_send:
            result = await notifier.notify("Test message")

        assert result is True
        mock_send.assert_awaited_once_with("123456:ABC-DEF", 123, "Test message")
        # Should not use client since fast path succeeded
        mock_target_service.use_client.assert_not_called()

    @pytest.mark.anyio
    async def test_notify_with_cached_me_id_but_no_bot_falls_back(
        self, mock_target_service, mock_notification_bundle
    ):
        """Test fallback when me.id is cached but bot lookup returns None."""
        mock_notification_bundle.get_bot = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        notifier._cached_me_id = 123

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with(789, "Test message")


class TestNotifierSlowPath:
    """Tests for Notifier slow path that requires client connection."""

    @pytest.mark.anyio
    async def test_notify_without_cached_me_id_fetches_it(
        self, mock_target_service, mock_notification_bundle
    ):
        """Test that notifier fetches and caches me.id when not cached."""
        bot = NotificationBot(
            tg_user_id=123,
            tg_username="test_user",
            bot_id=456,
            bot_username="test_bot",
            bot_token="123456:ABC-DEF",
        )
        mock_notification_bundle.get_bot = AsyncMock(return_value=bot)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        # me.id is not cached initially
        assert notifier._cached_me_id is None

        with patch(
            "src.telegram.notifier._send_via_bot_api",
            new_callable=AsyncMock,
            return_value=True,
        ):
            result = await notifier.notify("Test message")

        assert result is True
        # Should have cached the me.id
        assert notifier._cached_me_id == 123
        mock_client.get_me.assert_awaited_once()

    @pytest.mark.anyio
    async def test_notify_fallback_to_client_when_no_bot(
        self, mock_target_service, mock_notification_bundle
    ):
        """Test fallback to client.send_message when no bot is configured."""
        mock_notification_bundle.get_bot = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with(789, "Test message")

    @pytest.mark.anyio
    async def test_notify_admin_chat_id_me(self, mock_target_service, mock_notification_bundle):
        """Test sending to 'me' when admin_chat_id is None."""
        mock_notification_bundle.get_bot = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=None,  # No admin_chat_id, should use "me"
            notification_bundle=mock_notification_bundle,
        )

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with("me", "Test message")


class TestNotifierWithoutBundle:
    """Tests for Notifier when notification_bundle is None."""

    @pytest.mark.anyio
    async def test_notify_without_bundle_uses_client_directly(self, mock_target_service):
        """Test that without bundle, notifier uses client.send_message directly."""
        mock_client = MagicMock()
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,  # No bundle
        )

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with(789, "Test message")


class TestNotifierErrorHandling:
    """Tests for Notifier error handling."""

    @pytest.mark.anyio
    async def test_notify_cancelled_error_propagates(
        self,
        mock_target_service,
        mock_notification_bundle,
    ):
        """Test that CancelledError is re-raised, not swallowed."""
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=asyncio.CancelledError)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        with pytest.raises(asyncio.CancelledError):
            await notifier.notify("Test message")

    @pytest.mark.anyio
    async def test_notify_general_exception_returns_false(
        self,
        mock_target_service,
        mock_notification_bundle,
    ):
        """Test that general exceptions are caught and result in False return."""
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=RuntimeError("Connection failed"))

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        result = await notifier.notify("Test message")

        assert result is False


class TestNotifierInvalidateCache:
    """Tests for cache invalidation."""

    def test_invalidate_me_cache(self, mock_target_service, mock_notification_bundle):
        """Test that invalidate_me_cache clears the cached me.id."""
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        notifier._cached_me_id = 123

        notifier.invalidate_me_cache()

        assert notifier._cached_me_id is None

    def test_admin_chat_id_property(self, mock_target_service, mock_notification_bundle):
        """Test admin_chat_id property accessor."""
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        assert notifier.admin_chat_id == 789


class TestSendViaBotApi:
    """Tests for _send_via_bot_api helper function."""

    @pytest.mark.anyio
    async def test_send_via_bot_api_success(self):
        """Test successful bot API call."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"ok": True, "result": {}})

        # session.post(...) is used as async context manager
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_post_context)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is True

    @pytest.mark.anyio
    async def test_send_via_bot_api_error_response(self):
        """Test bot API call when response has ok=false."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(
            return_value={"ok": False, "error_code": 400, "description": "Bad Request"}
        )

        # session.post(...) is used as async context manager
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_post_context)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is False

    @pytest.mark.anyio
    async def test_send_via_bot_api_network_error(self):
        """Test bot API call when network error occurs during ClientSession creation."""
        # The error happens inside the async with ClientSession() block
        # We need to make the session's __aenter__ raise the error
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=ConnectionError("Network unreachable"))
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is False

    @pytest.mark.anyio
    async def test_send_via_bot_api_cancelled_error_propagates(self):
        """Test that CancelledError in bot API call is re-raised."""
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=asyncio.CancelledError)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            with pytest.raises(asyncio.CancelledError):
                await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

    @pytest.mark.anyio
    async def test_send_via_bot_api_timeout_error(self):
        """Test bot API call when timeout occurs during session creation."""
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is False
