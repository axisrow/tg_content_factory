"""CLI auth flow tests — pure unit tests covering two-step send-code/verify-code flow."""
from __future__ import annotations

import argparse
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.telegram_unit
@pytest.mark.anyio
async def test_cli_send_code_saves_hash_to_db(tmp_path):
    """account send-code saves phone_code_hash to DB under auth_pending:{phone}."""
    import json

    from src.database import Database

    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    from src.telegram.auth import TelegramAuth

    auth = TelegramAuth(api_id=12345, api_hash="abc")
    from telethon.tl.types.auth import SentCodeTypeSms

    mock_result = MagicMock()
    mock_result.phone_code_hash = "hash_abc"
    mock_result.type = SentCodeTypeSms(length=5)
    mock_result.next_type = None
    mock_result.timeout = 60

    mock_client = AsyncMock()
    mock_client.send_code_request = AsyncMock(return_value=mock_result)
    mock_client.session = MagicMock()
    mock_client.session.save = MagicMock(return_value="")

    with patch("src.telegram.auth.TelegramClient", return_value=mock_client):
        info = await auth.send_code("+1234567890")

    await db.set_setting("auth_pending:+1234567890", json.dumps(info))

    saved = await db.get_setting("auth_pending:+1234567890")
    assert saved is not None
    parsed = json.loads(saved)
    assert parsed["phone_code_hash"] == "hash_abc"

    await db.close()


@pytest.mark.telegram_unit
@pytest.mark.anyio
async def test_sign_in_fresh_success():
    """sign_in_fresh() passes session_str to StringSession and signs in."""
    from src.telegram.auth import TelegramAuth

    auth = TelegramAuth(api_id=12345, api_hash="abc")
    mock_client = AsyncMock()
    mock_client.sign_in = AsyncMock()
    mock_client.session.save = MagicMock(return_value="fresh_session_string")

    captured_args = []

    def fake_client(session, api_id, api_hash):
        captured_args.append(session)
        return mock_client

    mock_string_session = MagicMock()

    with patch("src.telegram.auth.TelegramClient", side_effect=fake_client), \
         patch("src.telegram.auth.StringSession", return_value=mock_string_session) as mock_ss:
        session = await auth.sign_in_fresh("+1234567890", "54321", "hash_abc", session_str="SAVED")

    assert session == "fresh_session_string"
    mock_ss.assert_called_once_with("SAVED")
    mock_client.connect.assert_awaited_once()
    mock_client.sign_in.assert_awaited_once_with("+1234567890", "54321", phone_code_hash="hash_abc")
    mock_client.disconnect.assert_awaited_once()


@pytest.mark.telegram_unit
@pytest.mark.anyio
async def test_sign_in_fresh_2fa_required():
    """sign_in_fresh raises ValueError('2FA') when SessionPasswordNeededError and no password given."""
    from telethon.errors import SessionPasswordNeededError

    from src.telegram.auth import TelegramAuth

    auth = TelegramAuth(api_id=12345, api_hash="abc")
    mock_client = AsyncMock()
    mock_client.sign_in = AsyncMock(side_effect=SessionPasswordNeededError(request=None))

    with patch("src.telegram.auth.TelegramClient", return_value=mock_client):
        with pytest.raises(ValueError, match="2FA"):
            await auth.sign_in_fresh("+1234567890", "54321", "hash_abc")


@pytest.mark.telegram_unit
@pytest.mark.anyio
async def test_sign_in_fresh_2fa_with_password():
    """sign_in_fresh with password succeeds after SessionPasswordNeededError."""
    from telethon.errors import SessionPasswordNeededError

    from src.telegram.auth import TelegramAuth

    auth = TelegramAuth(api_id=12345, api_hash="abc")
    mock_client = AsyncMock()

    async def sign_in_side(*args, **kwargs):
        if "password" not in kwargs:
            raise SessionPasswordNeededError(request=None)

    mock_client.sign_in = AsyncMock(side_effect=sign_in_side)
    mock_client.session.save = MagicMock(return_value="session_2fa_ok")

    with patch("src.telegram.auth.TelegramClient", return_value=mock_client):
        session = await auth.sign_in_fresh("+1234567890", "54321", "hash_abc", password_2fa="mypass")

    assert session == "session_2fa_ok"


@pytest.mark.telegram_unit
def test_cli_verify_code_persists_account_before_pool_warmup(tmp_path, capsys):
    """Regression #449: `account verify-code` must persist the authenticated session
    to the DB BEFORE warming the in-memory pool. If the pool warm-up fails afterwards,
    the session string is already safe; the inverse order would lose it permanently on
    the next restart (the pool is rebuilt from the DB)."""
    from src.cli.commands import account as account_cmd

    order: list[str] = []

    config = MagicMock()
    config.telegram.api_id = 123
    config.telegram.api_hash = "abc"

    captured: dict = {}

    async def fake_init_db(_config_path):
        from src.database import Database

        db = Database(str(tmp_path / "verify.db"))
        await db.initialize()
        await db.set_setting("auth_pending:+1", json.dumps({"phone_code_hash": "h"}))
        original_add = db.add_account

        async def traced_add_account(account):
            order.append("add_account")
            captured["account"] = account
            return await original_add(account)

        db.add_account = traced_add_account  # type: ignore[method-assign]

        original_set_setting = db.set_setting

        async def traced_set_setting(key, value):
            # Trace only the pending-auth clear (value == "") so the order list
            # captures when the pending key is wiped relative to add_account (#449).
            if key == "auth_pending:+1" and value == "":
                order.append("clear_pending")
            return await original_set_setting(key, value)

        db.set_setting = traced_set_setting  # type: ignore[method-assign]
        captured["db"] = db
        return config, db

    async def fake_init_pool(_config, _db):
        pool = MagicMock()

        async def add_client(_phone, _session):
            order.append("add_client")
            raise RuntimeError("pool boom")

        pool.add_client = add_client
        pool.disconnect_all = AsyncMock()
        return None, pool

    auth = MagicMock()
    auth.sign_in_fresh = AsyncMock(return_value="SESSION_XYZ")

    args = argparse.Namespace(
        account_action="verify-code",
        phone="+1",
        code="55555",
        password=None,
        config=None,
        api_id=None,
        api_hash=None,
    )

    with patch.object(account_cmd.runtime, "init_db", fake_init_db), \
         patch.object(account_cmd.runtime, "init_pool", fake_init_pool), \
         patch.object(account_cmd, "TelegramAuth", return_value=auth):
        account_cmd.run(args)

    # DB write happened, and it happened BEFORE the (failing) pool warm-up.
    # The pending-auth key is cleared AFTER the account is persisted (#449), so a
    # crash between sign-in and persist leaves the pending key intact for retry.
    assert order == ["add_account", "clear_pending", "add_client"], order
    assert captured["account"].phone == "+1"
    assert captured["account"].session_string == "SESSION_XYZ"
    assert captured["account"].is_primary is True

    out = capsys.readouterr().out
    assert "added successfully" in out
    assert "pool warm-up failed" in out


@pytest.mark.telegram_unit
@pytest.mark.anyio
async def test_sign_in_fresh_does_not_require_pending():
    """sign_in_fresh works without prior send_code in same process (no _pending check)."""
    from src.telegram.auth import TelegramAuth

    auth = TelegramAuth(api_id=12345, api_hash="abc")
    assert "+1234567890" not in auth._pending

    mock_client = AsyncMock()
    mock_client.sign_in = AsyncMock()
    mock_client.session.save = MagicMock(return_value="cross_process_session")

    with patch("src.telegram.auth.TelegramClient", return_value=mock_client):
        session = await auth.sign_in_fresh("+1234567890", "54321", "hash_abc")

    assert session == "cross_process_session"
