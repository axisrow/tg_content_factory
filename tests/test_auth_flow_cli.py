"""CLI auth flow tests — pure unit tests covering two-step send-code/verify-code flow."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.telegram_unit
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
@pytest.mark.asyncio
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
