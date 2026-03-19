import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FloodWaitError, UsernameInvalidError

from src.models import Account, Channel
from src.telegram.backends import TelegramTransportSession
from src.telegram.client_pool import ClientPool


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[])
    db.update_account_flood = AsyncMock()
    db.update_account_premium = AsyncMock()
    db.get_channel_by_channel_id = AsyncMock()
    db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    db.repos.dialog_cache.replace_dialogs = AsyncMock()
    db.repos.dialog_cache.clear_dialogs = AsyncMock()
    return db


@pytest.fixture
def mock_auth():
    auth = MagicMock()
    auth.api_id = 12345
    auth.api_hash = "hash"
    auth.create_client_from_session = AsyncMock()
    return auth


@pytest.mark.asyncio
async def test_client_pool_initialize_success(mock_db, mock_auth, telethon_cli_spy):
    acc = Account(
        phone="+7999",
        session_string="sess",
        is_active=True,
        is_primary=True,
        is_premium=False,
    )
    mock_db.get_accounts.return_value = [acc]

    mock_client = AsyncMock()
    mock_client.is_user_authorized = AsyncMock(return_value=True)
    mock_client.get_me = AsyncMock(return_value=MagicMock(premium=True))
    telethon_cli_spy.default_client = mock_client

    pool = ClientPool(mock_auth, mock_db)
    await pool.initialize()

    assert "+7999" in pool.clients
    mock_db.update_account_premium.assert_called_once_with("+7999", True)


@pytest.mark.asyncio
async def test_get_available_client_rotation(mock_db, mock_auth):
    acc1 = Account(phone="+7001", is_active=True, session_string="s1")
    acc2 = Account(phone="+7002", is_active=True, session_string="s2")
    mock_db.get_accounts.return_value = [acc1, acc2]

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": MagicMock(), "+7002": MagicMock()}

    # First client
    res1 = await pool.get_available_client()
    assert res1[1] == "+7001"
    assert "+7001" in pool._in_use

    # Second client
    res2 = await pool.get_available_client()
    assert res2[1] == "+7002"

    # Fallback when all in use
    res3 = await pool.get_available_client()
    assert res3 is not None  # Should return one of them even if in use


@pytest.mark.asyncio
async def test_get_available_client_flood_waited(mock_db, mock_auth):
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    acc1 = Account(phone="+7001", is_active=True, session_string="s1", flood_wait_until=future)
    acc2 = Account(phone="+7002", is_active=True, session_string="s2")
    mock_db.get_accounts.return_value = [acc1, acc2]

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": MagicMock(), "+7002": MagicMock()}

    res = await pool.get_available_client()
    assert res[1] == "+7002"


@pytest.mark.asyncio
async def test_get_premium_client_unavailable(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, is_premium=False, session_string="s1")
    mock_db.get_accounts.return_value = [acc]
    pool = ClientPool(mock_auth, mock_db)

    res = await pool.get_premium_client()
    assert res is None

    reason = await pool.get_premium_unavailability_reason()
    assert "Нет аккаунтов с Telegram Premium" in reason


@pytest.mark.asyncio
async def test_get_premium_client_skips_premium_flood_waited(mock_db, mock_auth):
    acc1 = Account(phone="+7001", is_active=True, is_premium=True, session_string="s1")
    acc2 = Account(phone="+7002", is_active=True, is_premium=True, session_string="s2")
    mock_db.get_accounts.return_value = [acc1, acc2]

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": MagicMock(), "+7002": MagicMock()}
    await pool.report_premium_flood("+7001", 120)

    res = await pool.get_premium_client()
    assert res is not None
    assert res[1] == "+7002"
    mock_db.update_account_flood.assert_not_called()


@pytest.mark.asyncio
async def test_get_premium_unavailability_reason_reports_premium_flood(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, is_premium=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": MagicMock()}
    await pool.report_premium_flood("+7001", 120)

    reason = await pool.get_premium_unavailability_reason()
    assert "Flood Wait" in reason


@pytest.mark.asyncio
async def test_get_stats_availability_all_flooded(mock_db, mock_auth):
    future = datetime.now(timezone.utc) + timedelta(seconds=100)
    acc = Account(phone="+7001", is_active=True, session_string="s1", flood_wait_until=future)
    mock_db.get_accounts.return_value = [acc]

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": MagicMock()}

    stats = await pool.get_stats_availability()
    assert stats.state == "all_flooded"
    assert stats.retry_after_sec >= 99


@pytest.mark.asyncio
async def test_resolve_channel_flood_rotation(mock_db, mock_auth):
    acc1 = Account(phone="+7001", is_active=True, session_string="s1")
    acc2 = Account(phone="+7002", is_active=True, session_string="s2")
    mock_db.get_accounts.return_value = [acc1, acc2]

    c1 = AsyncMock()
    c1.get_entity.side_effect = FloodWaitError(10)
    c2 = AsyncMock()
    c2.get_entity.return_value = MagicMock(id=123, title="Title", broadcast=True)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": c1, "+7002": c2}

    # We mock get_available_client to force rotation
    with patch.object(pool, "get_available_client", side_effect=[(c1, "+7001"), (c2, "+7002")]):
        res = await pool.resolve_channel("@test")
        assert res["channel_id"] == 123
        mock_db.update_account_flood.assert_called_once()


@pytest.mark.asyncio
async def test_resolve_channel_errors(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]
    client = AsyncMock()
    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": TelegramTransportSession(client, disconnect_on_close=False)}

    # Timeout
    client.get_entity.side_effect = asyncio.TimeoutError()
    assert await pool.resolve_channel("@t") is None

    # Username errors
    client.get_entity.side_effect = UsernameInvalidError("inv")
    assert await pool.resolve_channel("@t") is None

    # No client
    mock_db.get_accounts.return_value = []
    with pytest.raises(RuntimeError, match="no_client"):
        await pool.resolve_channel("@t")


@pytest.mark.asyncio
async def test_get_users_info_with_avatar(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, is_primary=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]
    client = AsyncMock()
    client.get_me.return_value = MagicMock(first_name="F", last_name="L", username="u")
    client.download_profile_photo.return_value = True

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": TelegramTransportSession(client, disconnect_on_close=False)}

    with patch("io.BytesIO", return_value=MagicMock(read=lambda: b"imgdata")):
        info = await pool.get_users_info()
        assert len(info) == 1
        assert info[0].phone == "+7001"
        assert "data:image/jpeg;base64" in info[0].avatar_base64


@pytest.mark.asyncio
async def test_leave_channels_flood(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]
    client = AsyncMock()
    client.delete_dialog.side_effect = FloodWaitError(5)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": TelegramTransportSession(client, disconnect_on_close=False)}

    res = await pool.leave_channels("+7001", [(123, "channel"), (456, "channel")])
    assert res[123] is False
    assert res[456] is False
    mock_db.update_account_flood.assert_called_once()


@pytest.mark.asyncio
async def test_get_forum_topics_cache_hit_and_miss(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]
    client = AsyncMock()
    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": TelegramTransportSession(client, disconnect_on_close=False)}

    # Cache hit
    client.get_entity.return_value = MagicMock(id=1)
    client.return_value = MagicMock(topics=[MagicMock(id=10, title="T1")])
    topics = await pool.get_forum_topics(1)
    assert topics[0]["title"] == "T1"

    # Cache miss (ValueError) then resolve by username
    client.get_entity.side_effect = [ValueError(), MagicMock(id=1)]
    mock_db.get_channel_by_channel_id.return_value = Channel(channel_id=1, username="u1")
    topics = await pool.get_forum_topics(1)
    assert len(topics) == 1


@pytest.mark.asyncio
async def test_get_dialogs_timeout(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]
    client = AsyncMock()

    async def slow_iter():
        await asyncio.sleep(0.1)
        yield MagicMock()

    client.iter_dialogs = slow_iter
    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": client}

    with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
        res = await pool.get_dialogs()
        assert res == []


@pytest.mark.asyncio
async def test_get_dialogs_for_phone_sets_is_own(mock_db, mock_auth):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    own_entity = MagicMock(
        id=101,
        username="ownchan",
        creator=True,
        megagroup=False,
        broadcast=True,
        gigagroup=False,
        forum=False,
        monoforum=False,
        scam=False,
        fake=False,
        restricted=False,
    )
    other_entity = MagicMock(
        id=202,
        username="otherchan",
        creator=False,
        megagroup=False,
        broadcast=True,
        gigagroup=False,
        forum=False,
        monoforum=False,
        scam=False,
        fake=False,
        restricted=False,
    )

    own_dialog = MagicMock(
        entity=own_entity,
        title="Own",
        is_channel=True,
        is_group=False,
    )
    other_dialog = MagicMock(
        entity=other_entity,
        title="Other",
        is_channel=True,
        is_group=False,
    )

    async def iter_dialogs():
        yield own_dialog
        yield other_dialog

    client = MagicMock()
    client.iter_dialogs.return_value = iter_dialogs()

    pool = ClientPool(mock_auth, mock_db)
    pool.clients = {"+7001": TelegramTransportSession(client, disconnect_on_close=False)}

    dialogs = await pool.get_dialogs_for_phone("+7001")

    assert dialogs[0]["is_own"] is True
    assert dialogs[1]["is_own"] is False
