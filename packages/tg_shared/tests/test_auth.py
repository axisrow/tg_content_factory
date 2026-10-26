from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import SessionPasswordNeededError

from tg_shared.auth import (
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


_PROXY_DICT = {"proxy_type": "socks5", "addr": "h", "port": 1080, "rdns": True}


class TestDescribeCodeType:
    def test_app(self):
        with patch("tg_shared.auth.SentCodeTypeApp", FakeSentCodeTypeApp):
            assert _describe_code_type(FakeSentCodeTypeApp()) == "приложение Telegram"

    def test_sms(self):
        with patch("tg_shared.auth.SentCodeTypeSms", FakeSentCodeTypeSms):
            assert _describe_code_type(FakeSentCodeTypeSms()) == "SMS"

    def test_fallback(self):
        assert _describe_code_type("unknown") == "Telegram"


class TestDescribeNextType:
    def test_none(self):
        assert _describe_next_type(None) is None

    def test_sms(self):
        with patch("tg_shared.auth.CodeTypeSms", FakeCodeTypeSms):
            assert _describe_next_type(FakeCodeTypeSms()) == "SMS"

    def test_call(self):
        with patch("tg_shared.auth.CodeTypeCall", FakeCodeTypeCall):
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
            patch("tg_shared.auth.TelegramClient", return_value=mock_client),
            patch("tg_shared.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
            patch("tg_shared.auth.CodeTypeSms", FakeCodeTypeSms),
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
            patch("tg_shared.auth.TelegramClient", return_value=new_client),
            patch("tg_shared.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
        ):
            await auth.send_code("+1234567890")

        old_client.disconnect.assert_awaited_once()
        assert auth._pending["+1234567890"][0] is new_client

    @pytest.mark.parametrize(
        ("auth_kwargs", "expected_proxy"),
        [({"proxy": _PROXY_DICT}, _PROXY_DICT), ({}, None)],
    )
    @pytest.mark.anyio
    async def test_send_code_passes_proxy_to_telethon(self, auth_kwargs, expected_proxy):
        auth = TelegramAuth(api_id=123, api_hash="abc", **auth_kwargs)
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.session = SimpleNamespace(save=lambda: "session_str")
        mock_client.send_code_request = AsyncMock(
            return_value=SimpleNamespace(
                phone_code_hash="hash1",
                type=FakeSentCodeTypeApp(),
                next_type=None,
                timeout=60,
            )
        )

        with (
            patch("tg_shared.auth.TelegramClient", return_value=mock_client) as fake_tc,
            patch("tg_shared.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
        ):
            await auth.send_code("+1234567890")

        assert fake_tc.call_args.kwargs["proxy"] == expected_proxy


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

        with (patch("tg_shared.auth.SentCodeTypeSms", FakeSentCodeTypeSms),):
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

        with caplog.at_level(logging.INFO, logger="tg_shared.auth"):
            with pytest.raises(TwoFactorRequiredError):
                await auth.verify_code("+1234567890", "11111", "hash123")

        # Expected outcome must not be logged as an ERROR with a stack trace.
        assert not any(r.levelno >= logging.ERROR for r in caplog.records)
        assert any(
            "needs 2FA password" in r.getMessage() and r.levelno == logging.INFO
            for r in caplog.records
        )
        # TwoFactorRequiredError re-raises before the post-try cleanup, so the
        # pending client stays alive for the follow-up password step.
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
            patch("tg_shared.auth.TelegramClient", return_value=mock_client),
            patch("tg_shared.auth.StringSession", _fake_string_session),
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
            patch("tg_shared.auth.TelegramClient", return_value=mock_client),
            patch("tg_shared.auth.StringSession", lambda _value="": SimpleNamespace()),
        ):
            with pytest.raises(RuntimeError, match="save failed post sign_in"):
                await auth.sign_in_fresh(
                    "+1234567890", "11111", "hash123", session_str="state"
                )

        # sign_in_fresh has no in-memory pending to preserve, but it must always
        # disconnect its throwaway client (no leak) even on the save failure.
        mock_client.disconnect.assert_awaited_once()


# ------------------------------------------------------------------
# Mutation slice #6 killer tests: auth contract pins
# ------------------------------------------------------------------


@pytest.mark.anyio
async def test_send_code_pins_full_result_contract():
    """send_code must hand back the CAPTURED session string (the code hash is
    bound to it) and the delivery metadata verbatim (#mutation slice 6)."""
    auth = TelegramAuth(api_id=123, api_hash="abc")
    fake_result = SimpleNamespace(
        phone_code_hash="hash123",
        type=FakeSentCodeTypeApp(),
        next_type=None,
        timeout=60,
    )
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.session = SimpleNamespace(save=lambda: "session_str_123")
    mock_client.send_code_request = AsyncMock(return_value=fake_result)

    with (
        patch("tg_shared.auth.TelegramClient", return_value=mock_client) as fake_tc,
        patch("tg_shared.auth.SentCodeTypeApp", FakeSentCodeTypeApp),
    ):
        info = await auth.send_code("+1234567890")

    assert set(info) == {
        "phone_code_hash",
        "session_str",
        "code_type",
        "next_type",
        "timeout",
    }
    assert info["session_str"] == "session_str_123"
    mock_client.send_code_request.assert_awaited_once_with("+1234567890")
    # The client is built on a FRESH StringSession with this auth's credentials.
    args, kwargs = fake_tc.call_args
    assert args[1:] == (123, "abc")
    assert kwargs == {"proxy": None}


@pytest.mark.anyio
async def test_resend_code_reuses_original_phone_and_code_hash():
    """ResendCodeRequest must carry the ORIGINAL phone and phone_code_hash —
    the code hash is bound to that session, a drift makes the resend useless
    (#mutation slice 6)."""
    from telethon.tl.functions.auth import ResendCodeRequest

    auth = TelegramAuth(api_id=123, api_hash="abc")
    client = AsyncMock()
    client.return_value = SimpleNamespace(
        phone_code_hash="hash_new",
        type=FakeSentCodeTypeApp(),
        next_type=None,
        timeout=30,
    )
    client.session = SimpleNamespace(save=lambda: "session_resend")
    auth._pending["+1234567890"] = (client, "hash_old")

    with patch("tg_shared.auth.SentCodeTypeApp", FakeSentCodeTypeApp):
        info = await auth.resend_code("+1234567890")

    request = client.call_args.args[0]
    assert isinstance(request, ResendCodeRequest)
    assert request.phone_number == "+1234567890"
    assert request.phone_code_hash == "hash_old"
    assert info["phone_code_hash"] == "hash_new"
    assert info["session_str"] == "session_resend"
    assert auth._pending["+1234567890"] == (client, "hash_new")


@pytest.mark.anyio
async def test_verify_code_rejects_hash_from_another_session():
    """A phone_code_hash that does not match the pending one must be rejected
    before any sign_in RPC — the hash is session-bound (#mutation slice 6)."""
    auth = TelegramAuth(api_id=123, api_hash="abc")
    client = MagicMock()
    client.sign_in = AsyncMock()
    auth._pending["+1234567890"] = (client, "hash_session")

    with pytest.raises(ValueError, match=r"^Phone code hash mismatch$"):
        await auth.verify_code("+1234567890", "11111", "hash_foreign")

    client.sign_in.assert_not_awaited()


@pytest.mark.anyio
async def test_sign_in_fresh_consumed_code_requires_and_uses_2fa_password():
    """code_consumed=True: without a password the sign-in must NOT proceed;
    with one, sign_in is called with EXACTLY that password (#mutation slice 6)."""
    auth = TelegramAuth(api_id=123, api_hash="abc")
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.sign_in = AsyncMock()
    mock_client.session = SimpleNamespace(save=lambda: "session_fresh")

    with (
        patch("tg_shared.auth.TelegramClient", return_value=mock_client),
        patch("tg_shared.auth.StringSession", lambda _value="": SimpleNamespace()),
    ):
        with pytest.raises(TwoFactorRequiredError):
            await auth.sign_in_fresh(
                "+1234567890", "11111", "hash1", session_str="state", code_consumed=True
            )
        mock_client.sign_in.assert_not_awaited()

        result = await auth.sign_in_fresh(
            "+1234567890",
            "11111",
            "hash1",
            session_str="state",
            password_2fa="hunter2",
            code_consumed=True,
        )

    assert result == "session_fresh"
    mock_client.sign_in.assert_awaited_once_with(password="hunter2")
    mock_client.disconnect.assert_awaited()


@pytest.mark.anyio
async def test_sign_in_fresh_password_needed_falls_back_to_2fa():
    """SessionPasswordNeededError on the code sign-in falls back to the 2FA
    password sign-in; without a password TwoFactorRequiredError propagates
    (#mutation slice 6)."""
    auth = TelegramAuth(api_id=123, api_hash="abc")

    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.sign_in = AsyncMock(side_effect=SessionPasswordNeededError(request=None))
    client.session = SimpleNamespace(save=lambda: "session_no_2fa")
    with (
        patch("tg_shared.auth.TelegramClient", return_value=client),
        patch("tg_shared.auth.StringSession", lambda _value="": SimpleNamespace()),
    ):
        with pytest.raises(TwoFactorRequiredError):
            await auth.sign_in_fresh("+1234567890", "11111", "hash1", session_str="state")
        client.disconnect.assert_awaited_once()

    client2 = MagicMock()
    client2.connect = AsyncMock()
    client2.disconnect = AsyncMock()

    async def _sign_in(*args, **kwargs):
        if kwargs.get("password") == "hunter2":
            return None
        raise SessionPasswordNeededError(request=None)

    client2.sign_in = AsyncMock(side_effect=_sign_in)
    client2.session = SimpleNamespace(save=lambda: "session_2fa")
    with (
        patch("tg_shared.auth.TelegramClient", return_value=client2),
        patch("tg_shared.auth.StringSession", lambda _value="": SimpleNamespace()),
    ):
        result = await auth.sign_in_fresh(
            "+1234567890", "11111", "hash1", session_str="state", password_2fa="hunter2"
        )

    assert result == "session_2fa"
    assert client2.sign_in.await_count == 2
    assert client2.sign_in.await_args_list[1].kwargs == {"password": "hunter2"}


def test_describe_code_type_labels_every_delivery_variant():
    """Every SentCodeType maps to its operator-facing label; unknown stays
    'Telegram' (#mutation slice 6)."""
    cases = [
        ("SentCodeTypeCall", "телефонный звонок"),
        ("SentCodeTypeFlashCall", "flash-звонок"),
        ("SentCodeTypeMissedCall", "пропущенный звонок"),
        ("SentCodeTypeFirebaseSms", "SMS"),
        ("SentCodeTypeFragmentSms", "SMS (Fragment)"),
        ("SentCodeTypeSmsPhrase", "SMS (фраза)"),
        ("SentCodeTypeSmsWord", "SMS (слово)"),
        ("SentCodeTypeEmailCode", "email"),
        ("SentCodeTypeSetUpEmailRequired", "email (требуется настройка)"),
    ]
    for cls_name, label in cases:
        fake_cls = type(cls_name, (), {})
        with patch(f"tg_shared.auth.{cls_name}", fake_cls):
            assert _describe_code_type(fake_cls()) == label
    assert _describe_code_type(object()) == "Telegram"
