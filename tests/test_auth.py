from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import SessionPasswordNeededError

from src.telegram.auth import (
    TelegramAuth,
    TwoFactorRequiredError,
    _describe_code_type,
    _describe_next_type,
)

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
    @pytest.mark.anyio
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
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "session_str_123")
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

    @pytest.mark.anyio
    async def test_send_code_disconnects_previous_pending_client(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        old_client = MagicMock()
        old_client.disconnect = AsyncMock()
        auth._pending["+1234567890"] = (old_client, "old_hash")

        fake_result = SimpleNamespace(
            phone_code_hash="hash123",
            type=FakeSentCodeTypeApp(),
            next_type=None,
            timeout=60,
        )
        new_client = MagicMock()
        new_client.connect = AsyncMock()
        new_client.disconnect = AsyncMock()
        new_client.session = SimpleNamespace(save=lambda: "session_str_456")
        new_client.send_code_request = AsyncMock(return_value=fake_result)

        with (
            patch("src.telegram.auth.TelegramClient", return_value=new_client),
            patch("src.telegram.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
        ):
            await auth.send_code("+1234567890")

        old_client.disconnect.assert_awaited_once()
        assert auth._pending["+1234567890"][0] is new_client


class TestResendCode:
    @pytest.mark.anyio
    async def test_resend_code_no_pending(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        with pytest.raises(ValueError, match="No pending auth"):
            await auth.resend_code("+1234567890")

    @pytest.mark.anyio
    async def test_resend_code_calls_resend_request(self):
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "session_str_resend")
        auth._pending["+1234567890"] = (mock_client, "old_hash")

        fake_type = FakeSentCodeTypeSms()
        fake_result = SimpleNamespace(
            phone_code_hash="new_hash",
            type=fake_type,
            next_type=None,
            timeout=120,
        )
        mock_client.return_value = fake_result

        with (patch("src.telegram.auth.SentCodeTypeSms", FakeSentCodeTypeSms),):
            info = await auth.resend_code("+1234567890")

        assert info["phone_code_hash"] == "new_hash"
        assert info["session_str"] == "session_str_resend"
        assert info["code_type"] == "SMS"
        assert info["next_type"] is None
        assert info["timeout"] == 120
        # Verify hash was updated
        _, stored_hash = auth._pending["+1234567890"]
        assert stored_hash == "new_hash"
        # Verify ResendCodeRequest was called
        mock_client.assert_called_once()


class TestVerifyCode:
    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_verify_code_2fa_required_logs_info_not_error(self, caplog):
        """#633 bug #27: needing a 2FA password is expected, not an ERROR."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "session123")
        mock_client.sign_in.side_effect = SessionPasswordNeededError(request=None)
        auth._pending["+1234567890"] = (mock_client, "hash123")

        with caplog.at_level(logging.INFO, logger="src.telegram.auth"):
            with pytest.raises(TwoFactorRequiredError):
                await auth.verify_code("+1234567890", "11111", "hash123")

        # Expected outcome must not be logged as an ERROR with a stack trace.
        assert not any(r.levelno >= logging.ERROR for r in caplog.records)
        assert any(
            "needs 2FA password" in r.getMessage() and r.levelno == logging.INFO
            for r in caplog.records
        )
        # needs_2fa keeps the pending client alive for the follow-up password step.
        assert "+1234567890" in auth._pending

    @pytest.mark.anyio
    async def test_verify_code_hash_mismatch_raises_before_sign_in(self):
        """A phone_code_hash that does not match the pending one is rejected
        before any sign_in RPC — the stored hash is bound to the MTProto session,
        so a stale hash must fail loudly rather than silently signing in."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "session123")
        auth._pending["+1234567890"] = (mock_client, "hash123")

        with pytest.raises(ValueError, match="Phone code hash mismatch"):
            await auth.verify_code("+1234567890", "11111", "stale_hash")

        mock_client.sign_in.assert_not_awaited()
        # Pending stays intact so a retry with the right hash can still succeed.
        assert "+1234567890" in auth._pending

    @pytest.mark.anyio
    async def test_verify_code_no_pending_raises(self):
        """#1029: verifying with no pending entry (e.g. a cross-process verify
        that never ran send_code in this process) must fail with a clear error,
        not a KeyError."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        with pytest.raises(ValueError, match="No pending auth"):
            await auth.verify_code("+1234567890", "11111", "hash123")

    @pytest.mark.anyio
    async def test_verify_code_session_save_failure_does_not_lose_pending(self):
        """#1029 data-loss regression: if ``session.save()`` raises AFTER a
        successful ``sign_in``, the account is already authorized on Telegram's
        side but we have no session string. The pending entry MUST survive so the
        operator can retry the save instead of losing the authenticated session
        forever (which would force a fresh send-code/verify onboarding)."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()

        def _exploding_save():
            raise RuntimeError("session serialization failed")

        mock_client.session = SimpleNamespace(save=_exploding_save)
        auth._pending["+1234567890"] = (mock_client, "hash123")

        with pytest.raises(RuntimeError, match="session serialization failed"):
            await auth.verify_code("+1234567890", "11111", "hash123")

        # sign_in succeeded — the account is authorized — so the pending client
        # must NOT have been dropped/disconnected: that would strand the session.
        assert "+1234567890" in auth._pending
        mock_client.disconnect.assert_not_awaited()

    @pytest.mark.anyio
    async def test_verify_code_retry_after_save_failure_recovers_session(self):
        """#1029: after a transient ``session.save()`` failure the operator can
        retry ``verify_code`` and recover the session — the surviving pending
        client signs in idempotently and the second save succeeds."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        save_calls = {"n": 0}

        def _flaky_save():
            save_calls["n"] += 1
            if save_calls["n"] == 1:
                raise RuntimeError("transient serialization failure")
            return "recovered_session"

        mock_client.session = SimpleNamespace(save=_flaky_save)
        auth._pending["+1234567890"] = (mock_client, "hash123")

        with pytest.raises(RuntimeError):
            await auth.verify_code("+1234567890", "11111", "hash123")

        # Retry: pending survived, so this call recovers the session.
        session = await auth.verify_code("+1234567890", "11111", "hash123")
        assert session == "recovered_session"
        # Now that the session is durably captured, the client is cleaned up.
        assert "+1234567890" not in auth._pending
        mock_client.disconnect.assert_awaited_once()


class TestSignInFresh:
    @pytest.mark.anyio
    async def test_sign_in_fresh_restores_string_session(self):
        """#1029 cross-process: sign_in_fresh must rebuild the MTProto session
        from the session_str produced by send_code (possibly in another process),
        because Telegram binds phone_code_hash to that exact session."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "final_session")

        captured: dict[str, object] = {}

        def _fake_string_session(value=""):
            captured["session_str"] = value
            return SimpleNamespace(_value=value)

        with (
            patch("src.telegram.auth.TelegramClient", return_value=mock_client),
            patch("src.telegram.auth.StringSession", _fake_string_session),
        ):
            session = await auth.sign_in_fresh(
                "+1234567890",
                "11111",
                "hash123",
                session_str="restored_mtproto_state",
            )

        assert session == "final_session"
        # The send_code session string was fed back into StringSession verbatim.
        assert captured["session_str"] == "restored_mtproto_state"
        mock_client.sign_in.assert_awaited_once_with(
            "+1234567890", "11111", phone_code_hash="hash123"
        )
        mock_client.disconnect.assert_awaited_once()

    @pytest.mark.anyio
    async def test_sign_in_fresh_session_save_failure_does_not_swallow(self):
        """#1029 data-loss regression for the cross-process path: a save() that
        raises after a successful sign_in must propagate (so the caller knows the
        session was NOT captured) rather than being masked as success."""
        auth = TelegramAuth(api_id=123, api_hash="abc")
        mock_client = AsyncMock()

        def _exploding_save():
            raise RuntimeError("save failed post sign_in")

        mock_client.session = SimpleNamespace(save=_exploding_save)

        with (
            patch("src.telegram.auth.TelegramClient", return_value=mock_client),
            patch("src.telegram.auth.StringSession", lambda _value="": SimpleNamespace()),
        ):
            with pytest.raises(RuntimeError, match="save failed post sign_in"):
                await auth.sign_in_fresh(
                    "+1234567890", "11111", "hash123", session_str="state"
                )

        # sign_in_fresh has no in-memory pending to preserve, but it must always
        # disconnect its throwaway client (no leak) even on the save failure.
        mock_client.disconnect.assert_awaited_once()
