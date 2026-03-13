from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.telegram.auth import TelegramAuth, _describe_code_type, _describe_next_type

pytestmark = pytest.mark.native_backend_allowed


class FakeSentCodeTypeApp:
    pass


class FakeSentCodeTypeSms:
    pass


class FakeCodeTypeSms:
    pass


class FakeCodeTypeCall:
    pass


class TestDescribeCodeType:
    def test_app(self):
        with patch("src.telegram.auth.SentCodeTypeApp", FakeSentCodeTypeApp):
            assert _describe_code_type(FakeSentCodeTypeApp()) == "приложение Telegram"

    def test_sms(self):
        with patch("src.telegram.auth.SentCodeTypeSms", FakeSentCodeTypeSms):
            assert _describe_code_type(FakeSentCodeTypeSms()) == "SMS"

    def test_fallback(self):
        assert _describe_code_type("unknown") == "Telegram"


class TestDescribeNextType:
    def test_none(self):
        assert _describe_next_type(None) is None

    def test_sms(self):
        with patch("src.telegram.auth.CodeTypeSms", FakeCodeTypeSms):
            assert _describe_next_type(FakeCodeTypeSms()) == "SMS"

    def test_call(self):
        with patch("src.telegram.auth.CodeTypeCall", FakeCodeTypeCall):
            assert _describe_next_type(FakeCodeTypeCall()) == "звонок"

    def test_unknown(self):
        assert _describe_next_type("something") is None


class TestSendCode:
    @pytest.mark.asyncio
    async def test_send_code_returns_dict(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        fake_type = FakeSentCodeTypeApp()
        fake_next = FakeCodeTypeSms()
        fake_result = SimpleNamespace(
            phone_code_hash="hash123",
            type=fake_type,
            next_type=fake_next,
            timeout=60,
        )
        mock_client = AsyncMock()
        mock_client.send_code_request = AsyncMock(return_value=fake_result)

        with (
            patch("src.telegram.auth.TelegramClient", return_value=mock_client),
            patch("src.telegram.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
            patch("src.telegram.auth.CodeTypeSms", FakeCodeTypeSms),
        ):
            info = await auth.send_code("+1234567890")

        assert isinstance(info, dict)
        assert info["phone_code_hash"] == "hash123"
        assert info["code_type"] == "приложение Telegram"
        assert info["next_type"] == "SMS"
        assert info["timeout"] == 60
        assert "+1234567890" in auth._pending

    @pytest.mark.asyncio
    async def test_send_code_disconnects_previous_pending_client(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        old_client = AsyncMock()
        auth._pending["+1234567890"] = (old_client, "old_hash")

        fake_result = SimpleNamespace(
            phone_code_hash="hash123",
            type=FakeSentCodeTypeApp(),
            next_type=None,
            timeout=60,
        )
        new_client = AsyncMock()
        new_client.send_code_request = AsyncMock(return_value=fake_result)

        with (
            patch("src.telegram.auth.TelegramClient", return_value=new_client),
            patch("src.telegram.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
        ):
            await auth.send_code("+1234567890")

        old_client.disconnect.assert_awaited_once()
        assert auth._pending["+1234567890"][0] is new_client


class TestResendCode:
    @pytest.mark.asyncio
    async def test_resend_code_no_pending(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        with pytest.raises(ValueError, match="No pending auth"):
            await auth.resend_code("+1234567890")

    @pytest.mark.asyncio
    async def test_resend_code_calls_resend_request(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        auth._pending["+1234567890"] = (mock_client, "old_hash")

        fake_type = FakeSentCodeTypeSms()
        fake_result = SimpleNamespace(
            phone_code_hash="new_hash",
            type=fake_type,
            next_type=None,
            timeout=120,
        )
        mock_client.return_value = fake_result

        with (
            patch("src.telegram.auth.SentCodeTypeSms", FakeSentCodeTypeSms),
        ):
            info = await auth.resend_code("+1234567890")

        assert info["phone_code_hash"] == "new_hash"
        assert info["code_type"] == "SMS"
        assert info["next_type"] is None
        assert info["timeout"] == 120
        # Verify hash was updated
        _, stored_hash = auth._pending["+1234567890"]
        assert stored_hash == "new_hash"
        # Verify ResendCodeRequest was called
        mock_client.assert_called_once()


class TestVerifyCode:
    @pytest.mark.asyncio
    async def test_verify_code_disconnects_temporary_client(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "session123")
        auth._pending["+1234567890"] = (mock_client, "hash123")

        session = await auth.verify_code("+1234567890", "11111", "hash123")

        assert session == "session123"
        mock_client.sign_in.assert_awaited_once_with(
            "+1234567890", "11111", phone_code_hash="hash123"
        )
        mock_client.disconnect.assert_awaited_once()
        assert "+1234567890" not in auth._pending
