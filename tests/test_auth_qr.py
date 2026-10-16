"""QR login unit tests for :class:`TelegramAuth` (#1551).

Fake-first: a scripted FakeQRLogin replaces Telethon's QRLogin — no transport,
no live Telegram (live login stays behind the explicit owner gate).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError

from src.telegram.auth import QR_MAX_REFRESHES, TelegramAuth

PHONE = "+79991234567"
SESSION_STR = "session_string_qr"


class FakeQRLogin:
    """Scripted stand-in for telethon's QRLogin.

    ``wait()`` pops ``outcomes`` in order; an outcome of ``"hang"`` parks
    forever; once the script is exhausted it times out immediately (driving
    the recreate loop to its refresh cap).
    """

    def __init__(self, outcomes=(), hang: bool = False):
        self._outcomes = list(outcomes)
        self._hang = hang
        self.recreates = 0
        self.url = "tg://login?token=AAA"
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)

    async def wait(self, timeout=None):
        if self._hang or self._outcomes and self._outcomes[0] == "hang":
            if self._outcomes:
                self._outcomes.pop(0)
            await asyncio.Event().wait()
        outcome = self._outcomes.pop(0) if self._outcomes else asyncio.TimeoutError()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def recreate(self):
        self.recreates += 1
        self.url = f"tg://login?token=REFRESH{self.recreates}"
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)


def _client_mock() -> MagicMock:
    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.session.save = MagicMock(return_value=SESSION_STR)
    client.get_me = AsyncMock(return_value=SimpleNamespace(phone=PHONE))
    return client


async def test_qr_start_returns_url_and_pending_state():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        qrlogin = FakeQRLogin(hang=True)
        client.qr_login = AsyncMock(return_value=qrlogin)
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        assert started["url"] == "tg://login?token=AAA"
        assert started["qr_id"]
        assert started["expires_at"]
        status = auth.qr_status(started["qr_id"])
        assert status is not None
        assert status["state"] == "pending"
        # no session string leaks through status snapshots
        assert "session_string" not in status
    finally:
        await auth.cleanup()


async def test_qr_wait_loop_refreshes_url_on_expiry():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        # one expiry → recreate → fresh URL, then park (still scannable)
        client.qr_login = AsyncMock(return_value=FakeQRLogin([asyncio.TimeoutError(), "hang"]))
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        task = auth._qr.task
        for _ in range(50):  # let the loop hit the expiry and recreate
            if auth.qr_status(started["qr_id"])["refreshes"] >= 1:
                break
            await asyncio.sleep(0.01)
        assert not task.done()  # loop keeps waiting after the refresh
        status = auth.qr_status(started["qr_id"])
        assert status["state"] == "pending"
        assert status["url"] == "tg://login?token=REFRESH1"
        assert status["refreshes"] == 1
    finally:
        await auth.cleanup()


async def test_qr_wait_loop_success_captures_session_one_shot():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(return_value=FakeQRLogin([SimpleNamespace(phone=PHONE)]))
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        await auth._qr.task
        status = auth.qr_status(started["qr_id"])
        assert status["state"] == "success"
        assert status["phone"] == PHONE
        consumed = await auth.consume_qr_session(started["qr_id"])
        assert consumed is not None
        assert consumed["session_string"] == SESSION_STR
        assert consumed["phone"] == PHONE
        # flow flips to consumed; second consume gets nothing (no dup account)
        assert auth.qr_status(started["qr_id"])["state"] == "consumed"
        assert await auth.consume_qr_session(started["qr_id"]) is None
        client.disconnect.assert_awaited()
    finally:
        await auth.cleanup()


async def test_consume_requires_success_state():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(
            return_value=FakeQRLogin([SessionPasswordNeededError(request=None)])
        )
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        await auth._qr.task
        assert auth.qr_status(started["qr_id"])["state"] == "password_needed"
        assert await auth.consume_qr_session(started["qr_id"]) is None
    finally:
        await auth.cleanup()


async def test_qr_password_flow_invalid_then_valid():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(
            return_value=FakeQRLogin([SessionPasswordNeededError(request=None)])
        )
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        await auth._qr.task

        client.sign_in = AsyncMock(side_effect=PasswordHashInvalidError(request=None))
        result = await auth.qr_password(started["qr_id"], "wrong")
        assert result == {"state": "password_needed", "error": "invalid_password"}
        assert auth.qr_status(started["qr_id"])["state"] == "password_needed"

        client.sign_in = AsyncMock()
        result = await auth.qr_password(started["qr_id"], "right")
        assert result == {"state": "success"}
        consumed = await auth.consume_qr_session(started["qr_id"])
        assert consumed is not None
        assert consumed["session_string"] == SESSION_STR
        assert consumed["phone"] == PHONE
    finally:
        await auth.cleanup()


async def test_qr_password_empty_returns_password_required_without_sign_in():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(
            return_value=FakeQRLogin([SessionPasswordNeededError(request=None)])
        )
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        await auth._qr.task
        client.sign_in = AsyncMock()  # AsyncMock carries assert_not_awaited
        result = await auth.qr_password(started["qr_id"], "")
        assert result == {"state": "password_needed", "error": "password_required"}
        client.sign_in.assert_not_awaited()
    finally:
        await auth.cleanup()


async def test_qr_password_rejects_non_password_needed_state():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(return_value=FakeQRLogin([SimpleNamespace(phone=PHONE)]))
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        await auth._qr.task
        with pytest.raises(ValueError, match="qr_state_not_password_needed"):
            await auth.qr_password(started["qr_id"], "x")
    finally:
        await auth.cleanup()


async def test_qr_status_unknown_id_returns_none():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(return_value=FakeQRLogin(hang=True))
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            await auth.qr_start()
        assert auth.qr_status("no_such_id") is None
        with pytest.raises(ValueError, match="qr_session_not_found"):
            await auth.qr_password("no_such_id", "pw")
    finally:
        await auth.cleanup()


async def test_qr_expired_max_refreshes_fails_and_disconnects():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        # empty script: every wait() times out immediately → refreshes to the cap
        client.qr_login = AsyncMock(return_value=FakeQRLogin())
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started = await auth.qr_start()
        await auth._qr.task
        status = auth.qr_status(started["qr_id"])
        assert status["state"] == "failed"
        assert status["error"] == "qr_expired_max_refreshes"
        assert status["refreshes"] == QR_MAX_REFRESHES
        client.disconnect.assert_awaited()
    finally:
        await auth.cleanup()


async def test_qr_start_cancels_previous_flow():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    try:
        client.qr_login = AsyncMock(return_value=FakeQRLogin(hang=True))
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started1 = await auth.qr_start()
        old_task = auth._qr.task
        with patch("src.telegram.auth.TelegramClient", return_value=client):
            started2 = await auth.qr_start()
        assert started2["qr_id"] != started1["qr_id"]
        assert auth.qr_status(started1["qr_id"]) is None
        assert old_task.done()
        client.disconnect.assert_awaited()
    finally:
        await auth.cleanup()


async def test_qr_start_connect_failure_disconnects_and_leaves_no_state():
    client = _client_mock()
    client.connect = AsyncMock(side_effect=OSError("net down"))
    auth = TelegramAuth(1, "h")
    with patch("src.telegram.auth.TelegramClient", return_value=client):
        with pytest.raises(OSError):
            await auth.qr_start()
    client.disconnect.assert_awaited()
    assert auth._qr is None


async def test_cleanup_cancels_running_qr_task():
    client = _client_mock()
    auth = TelegramAuth(1, "h")
    client.qr_login = AsyncMock(return_value=FakeQRLogin(hang=True))
    with patch("src.telegram.auth.TelegramClient", return_value=client):
        await auth.qr_start()
    task = auth._qr.task
    await auth.cleanup()
    assert task.done()
    client.disconnect.assert_awaited()
    assert auth._qr is None
