"""Dispatcher tests for auth.qr_* handlers (#1551): real DB, fake TelegramAuth.

The materialization path (require encryption key → Account insert → pool
connect) is the data-loss-sensitive part, so it runs against a real Database —
same pattern as the accounts_connect tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database import Database
from src.models import Account
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher

PHONE = "+79991234567"
SESSION_STR = "SECRET_QR_SESSION"


class FakeQrAuth:
    """Minimal TelegramAuth surface for the three qr handlers."""

    is_configured = True

    def __init__(self):
        self.snapshots: dict[str, dict] = {}
        self.consumed: dict[str, dict] = {}
        self.password_results: dict[str, dict] = {}

    async def qr_start(self) -> dict:
        return {
            "qr_id": "q1",
            "url": "tg://login?token=AAA",
            "expires_at": "2026-10-16T00:00:30+00:00",
        }

    def qr_status(self, qr_id: str) -> dict | None:
        return self.snapshots.get(qr_id)

    async def consume_qr_session(self, qr_id: str) -> dict | None:
        return self.consumed.pop(qr_id, None)

    async def qr_password(self, qr_id: str, password: str) -> dict:
        return self.password_results.get(
            qr_id, {"state": "password_needed", "error": "invalid_password"}
        )


def _pool_mock() -> MagicMock:
    pool = MagicMock()
    pool.add_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    return pool


async def _db(tmp_path, *, encryption: bool = True) -> Database:
    # explicit None: an unset arg would fall back to SESSION_ENCRYPTION_KEY from env
    secret = "test-session-encryption-key" if encryption else None
    db = Database(str(tmp_path / "test.db"), session_encryption_secret=secret)
    await db.initialize()
    return db


async def test_qr_start_requires_auth(tmp_path):
    db = await _db(tmp_path)
    try:
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=None)
        with pytest.raises(RuntimeError, match="auth_not_configured"):
            await d._handle_auth_qr_start({})
    finally:
        await db.close()


async def test_qr_start_returns_token(tmp_path):
    db = await _db(tmp_path)
    try:
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=FakeQrAuth())
        result = await d._handle_auth_qr_start({})
        assert result["result"]["qr_id"] == "q1"
        assert result["result"]["url"] == "tg://login?token=AAA"
    finally:
        await db.close()


async def test_qr_status_unknown_session_raises(tmp_path):
    db = await _db(tmp_path)
    try:
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=FakeQrAuth())
        with pytest.raises(RuntimeError, match="qr_session_not_found"):
            await d._handle_auth_qr_status({"qr_id": "ghost"})
    finally:
        await db.close()


async def test_qr_status_pending_passthrough_no_materialization(tmp_path):
    db = await _db(tmp_path)
    try:
        auth = FakeQrAuth()
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "pending", "url": "tg://login?token=R1"}
        pool = _pool_mock()
        d = TelegramCommandDispatcher(db, pool, auth=auth)
        result = await d._handle_auth_qr_status({"qr_id": "q1"})
        assert result["result"]["state"] == "pending"
        assert await db.get_accounts() == []
        pool.add_client.assert_not_awaited()
    finally:
        await db.close()


async def test_qr_status_success_materializes_account_once(tmp_path):
    db = await _db(tmp_path)
    try:
        auth = FakeQrAuth()
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "success", "phone": PHONE}
        auth.consumed["q1"] = {
            "qr_id": "q1",
            "state": "success",
            "phone": PHONE,
            "session_string": SESSION_STR,
        }
        pool = _pool_mock()
        d = TelegramCommandDispatcher(db, pool, auth=auth)

        result = await d._handle_auth_qr_status({"qr_id": "q1"})
        assert result["result"]["state"] == "success"
        assert result["result"]["phone"] == PHONE
        assert SESSION_STR not in str(result)  # secret never reaches the command result

        accounts = await db.get_accounts()
        assert len(accounts) == 1
        assert accounts[0].phone == PHONE
        assert accounts[0].is_primary is True  # first account becomes primary
        pool.add_client.assert_awaited_once_with(PHONE, SESSION_STR)

        # duplicate poll after consume: terminal state, no second account
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "success", "phone": PHONE}
        result = await d._handle_auth_qr_status({"qr_id": "q1"})
        assert result["result"]["state"] == "consumed"
        assert len(await db.get_accounts()) == 1
        pool.add_client.assert_awaited_once()
    finally:
        await db.close()


async def test_qr_status_success_not_primary_when_account_exists(tmp_path):
    db = await _db(tmp_path)
    try:
        await db.add_account(Account(phone="+111", session_string="EXISTING", is_primary=True))
        auth = FakeQrAuth()
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "success", "phone": PHONE}
        auth.consumed["q1"] = {
            "qr_id": "q1",
            "state": "success",
            "phone": PHONE,
            "session_string": SESSION_STR,
        }
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=auth)
        result = await d._handle_auth_qr_status({"qr_id": "q1"})
        assert result["result"]["state"] == "success"
        accounts = await db.get_accounts()
        new = next(a for a in accounts if a.phone == PHONE)
        assert new.is_primary is False
    finally:
        await db.close()


async def test_qr_status_success_requires_session_encryption_key(tmp_path):
    db = await _db(tmp_path, encryption=False)
    try:
        auth = FakeQrAuth()
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "success", "phone": PHONE}
        auth.consumed["q1"] = {
            "qr_id": "q1",
            "state": "success",
            "phone": PHONE,
            "session_string": SESSION_STR,
        }
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=auth)
        with pytest.raises(RuntimeError, match="SESSION_ENCRYPTION_KEY"):
            await d._handle_auth_qr_status({"qr_id": "q1"})
        assert await db.get_accounts() == []
    finally:
        await db.close()


async def test_qr_password_invalid_keeps_state(tmp_path):
    db = await _db(tmp_path)
    try:
        auth = FakeQrAuth()
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "password_needed"}
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=auth)
        result = await d._handle_auth_qr_password({"qr_id": "q1", "password_2fa": "wrong"})
        assert result["result"]["state"] == "password_needed"
        assert result["result"]["error"] == "invalid_password"
        assert await db.get_accounts() == []
    finally:
        await db.close()


async def test_qr_password_scrubs_secret_from_payload(tmp_path):
    db = await _db(tmp_path)
    try:
        auth = FakeQrAuth()
        payload = {"qr_id": "q1", "password_2fa": "topsecret"}
        d = TelegramCommandDispatcher(db, _pool_mock(), auth=auth)
        await d._handle_auth_qr_password(payload)
        assert payload["password_2fa"] == ""
    finally:
        await db.close()


async def test_qr_password_success_materializes_account(tmp_path):
    db = await _db(tmp_path)
    try:
        auth = FakeQrAuth()
        auth.password_results["q1"] = {"state": "success"}
        auth.snapshots["q1"] = {"qr_id": "q1", "state": "success", "phone": PHONE}
        auth.consumed["q1"] = {
            "qr_id": "q1",
            "state": "success",
            "phone": PHONE,
            "session_string": SESSION_STR,
        }
        pool = _pool_mock()
        d = TelegramCommandDispatcher(db, pool, auth=auth)
        result = await d._handle_auth_qr_password({"qr_id": "q1", "password_2fa": "right"})
        assert result["result"]["state"] == "success"
        assert result["result"]["phone"] == PHONE
        assert len(await db.get_accounts()) == 1
        pool.add_client.assert_awaited_once_with(PHONE, SESSION_STR)
    finally:
        await db.close()
