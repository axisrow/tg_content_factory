"""Tests for Telegram client pool and collector edge paths."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import Channel as TLChannel
from telethon.tl.types import (
    ChannelForbidden,
    DocumentAttributeAnimated,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    MessageMediaContact,
    MessageMediaDice,
    MessageMediaDocument,
    MessageMediaGame,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaWebPage,
)

from src.config import SchedulerConfig, TelegramRuntimeConfig
from src.models import Account, Channel, ChannelStats
from src.telegram.backends import TelegramTransportSession
from src.telegram.client_pool import (
    ClientPool,
    StatsClientAvailability,
)
from src.telegram.collector import (
    AllCollectionClientsFloodedError,
    AllStatsClientsFloodedError,
    Collector,
    NoActiveCollectionClientsError,
    NoActiveStatsClientsError,
)
from tests.helpers import (
    AsyncIterEmpty,
    AsyncIterMessages,
    make_mock_message,
    make_mock_pool,
)

# ---------------------------------------------------------------------------
# ClientPool fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[])
    db.update_account_flood = AsyncMock()
    db.update_account_premium = AsyncMock()
    db.get_channel_by_channel_id = AsyncMock()
    db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    db.repos.dialog_cache.get_cached_at = AsyncMock(return_value=None)
    db.repos.dialog_cache.replace_dialogs = AsyncMock()
    db.repos.dialog_cache.clear_dialogs = AsyncMock()
    db.repos.dialog_cache.get_dialog = AsyncMock(return_value=None)
    db.repos.channels.get_preferred_phone = AsyncMock(return_value=None)
    db.repos.channels.update_channel_preferred_phone = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()
    db.set_channel_type = AsyncMock()
    return db


@pytest.fixture
def mock_auth():
    auth = MagicMock()
    auth.api_id = 12345
    auth.api_hash = "hash"
    auth.create_client_from_session = AsyncMock()
    return auth


@pytest.fixture
def pool(mock_auth, mock_db):
    return ClientPool(mock_auth, mock_db)


# ---------------------------------------------------------------------------
# ClientPool: warm_all_dialogs, channel_phone_map, warming helpers
# Missing lines: 117, 121, 125, 129, 133, 137-141, 151-185
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_connected_phones_returns_set(pool):
    pool.clients = {"+7001": MagicMock()}
    assert pool.connected_phones() == {"+7001"}


@pytest.mark.anyio
async def test_get_phone_for_channel_returns_none_when_unknown(pool):
    assert pool.get_phone_for_channel(999) is None


@pytest.mark.anyio
async def test_register_and_get_phone_for_channel(pool):
    pool.register_channel_phone(42, "+7001")
    assert pool.get_phone_for_channel(42) == "+7001"


@pytest.mark.anyio
async def test_clear_channel_phone(pool):
    pool.register_channel_phone(42, "+7001")
    pool.clear_channel_phone(42)
    assert pool.get_phone_for_channel(42) is None


@pytest.mark.anyio
async def test_is_warming_false_initially(pool):
    assert pool.is_warming() is False


@pytest.mark.anyio
async def test_is_warming_true_when_task_running(pool):
    async def _long_task():
        await asyncio.sleep(10)

    pool._warming_task = asyncio.create_task(_long_task())
    try:
        assert pool.is_warming() is True
    finally:
        pool._warming_task.cancel()
        try:
            await pool._warming_task
        except asyncio.CancelledError:
            pass


@pytest.mark.anyio
async def test_wait_for_warm_no_task(pool):
    await pool.wait_for_warm()  # Should not raise


@pytest.mark.anyio
async def test_wait_for_warm_completed_task(pool):
    pool._warming_task = asyncio.create_task(asyncio.sleep(0))
    await asyncio.sleep(0.05)
    await pool.wait_for_warm(timeout=1.0)


@pytest.mark.anyio
async def test_wait_for_warm_timeout(pool):
    async def _long():
        await asyncio.sleep(999)

    pool._warming_task = asyncio.create_task(_long())
    try:
        await pool.wait_for_warm(timeout=0.1)
    finally:
        pool._warming_task.cancel()
        try:
            await pool._warming_task
        except asyncio.CancelledError:
            pass


@pytest.mark.anyio
async def test_warm_all_dialogs_registers_channel_phones(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)

    entity = MagicMock(spec=TLChannel)
    entity.id = 12345
    dialog = MagicMock()
    dialog.entity = entity

    raw_client = AsyncMock()
    raw_client.get_dialogs = AsyncMock(return_value=[dialog])
    session = TelegramTransportSession(raw_client, disconnect_on_close=False)

    pool.clients["+7001"] = session

    with patch.object(pool, "get_client_by_phone", return_value=(session, "+7001")):
        await pool.warm_all_dialogs()

    assert pool.get_phone_for_channel(12345) == "+7001"
    mock_db.repos.channels.update_channel_preferred_phone.assert_called_once_with(12345, "+7001")


@pytest.mark.anyio
async def test_warm_all_dialogs_skips_non_channel_entities(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)

    # Non-channel entity (e.g. a User)
    entity = SimpleNamespace(id=999)  # not TLChannel or Chat
    dialog = MagicMock()
    dialog.entity = entity

    raw_client = AsyncMock()
    raw_client.get_dialogs = AsyncMock(return_value=[dialog])
    session = TelegramTransportSession(raw_client, disconnect_on_close=False)

    pool.clients["+7001"] = session

    with patch.object(pool, "get_client_by_phone", return_value=(session, "+7001")):
        await pool.warm_all_dialogs()
    assert pool.get_phone_for_channel(999) is None


@pytest.mark.anyio
async def test_warm_all_dialogs_handles_failure(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)

    raw_client = AsyncMock()
    raw_client.get_dialogs = AsyncMock(side_effect=RuntimeError("connection lost"))
    session = TelegramTransportSession(raw_client, disconnect_on_close=False)

    pool.clients["+7001"] = session

    with patch.object(pool, "get_client_by_phone", return_value=(session, "+7001")):
        # Should not raise
        await pool.warm_all_dialogs()


@pytest.mark.anyio
async def test_warm_all_dialogs_skips_get_client_by_phone_none(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = MagicMock()

    with patch.object(pool, "get_client_by_phone", return_value=None):
        await pool.warm_all_dialogs()


# ---------------------------------------------------------------------------
# ClientPool: _get_cached_dialogs / _store_cached_dialogs / invalidate
# Missing lines: 203-209, 222-244, 247-251
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_invalidate_dialogs_cache_specific_phone(pool):
    pool._store_cached_dialogs("+7001", "full", [{"channel_id": 1}])
    pool._store_cached_dialogs("+7002", "full", [{"channel_id": 2}])

    pool.invalidate_dialogs_cache("+7001")
    assert pool._get_cached_dialogs("+7001", "full") is None
    assert pool._get_cached_dialogs("+7002", "full") is not None


@pytest.mark.anyio
async def test_invalidate_dialogs_cache_all(pool):
    pool._store_cached_dialogs("+7001", "full", [{"channel_id": 1}])
    pool.invalidate_dialogs_cache()
    assert pool._get_cached_dialogs("+7001", "full") is None


@pytest.mark.anyio
async def test_cached_dialogs_expired(pool):
    pool._dialogs_cache_ttl_sec = 0.0
    pool._store_cached_dialogs("+7001", "full", [{"channel_id": 1}])
    await asyncio.sleep(0.01)
    assert pool._get_cached_dialogs("+7001", "full") is None


@pytest.mark.anyio
async def test_cached_dialogs_channels_only_falls_back_to_full(pool):
    full_data = [{"channel_id": 1, "channel_type": "channel"}, {"channel_id": 2, "channel_type": "dm"}]
    pool._store_cached_dialogs("+7001", "full", full_data)

    result = pool._get_cached_dialogs("+7001", "channels_only")
    assert result is not None
    assert len(result) == 1  # Only non-dm/bot/saved


@pytest.mark.anyio
async def test_cached_dialogs_channels_only_no_full_cache(pool):
    assert pool._get_cached_dialogs("+7001", "channels_only") is None


@pytest.mark.anyio
async def test_get_db_cached_dialogs_stale(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)
    pool._dialogs_db_cache_ttl_sec = 0.0

    mock_db.repos.dialog_cache.list_dialogs.return_value = [{"channel_id": 1}]
    mock_db.repos.dialog_cache.get_cached_at.return_value = datetime.now(timezone.utc) - timedelta(hours=2)

    result = await pool._get_db_cached_dialogs("+7001", "channels_only")
    assert result is None  # stale, should return None to trigger fresh fetch


@pytest.mark.anyio
async def test_get_db_cached_dialogs_returns_full(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)

    mock_db.repos.dialog_cache.list_dialogs.return_value = [{"channel_id": 1}]
    mock_db.repos.dialog_cache.get_cached_at.return_value = datetime.now(timezone.utc)

    result = await pool._get_db_cached_dialogs("+7001", "full")
    assert result is not None
    assert len(result) == 1


# ---------------------------------------------------------------------------
# ClientPool: resolve_any_entity
# Missing lines: 880-969
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_resolve_any_entity_channel(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    entity = SimpleNamespace(id=123, title="Ch", broadcast=True, megagroup=False, gigagroup=False,
                             forum=False, monoforum=False, scam=False, fake=False, restricted=False)
    client.get_entity = AsyncMock(return_value=entity)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@ch")
    assert result is not None
    assert result["channel_type"] == "channel"


@pytest.mark.anyio
async def test_resolve_any_entity_user(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    entity = SimpleNamespace(id=456, first_name="Ivan", last_name="Petrov", username="ivan",
                             bot=False)
    client.get_entity = AsyncMock(return_value=entity)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@ivan")
    assert result is not None
    assert result["channel_type"] == "dm"
    assert result["title"] == "Ivan Petrov"


@pytest.mark.anyio
async def test_resolve_any_entity_bot(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    entity = SimpleNamespace(id=789, first_name="Bot", last_name="", bot=True)
    client.get_entity = AsyncMock(return_value=entity)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@mybot")
    assert result is not None
    assert result["channel_type"] == "bot"


@pytest.mark.anyio
async def test_resolve_any_entity_negative_id_as_channel(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    entity = SimpleNamespace(id=123, title="Ch", broadcast=True, megagroup=False, gigagroup=False,
                             forum=False, monoforum=False, scam=False, fake=False, restricted=False)
    client.get_entity = AsyncMock(return_value=entity)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    # -100123 is a Bot API format channel ID -> strips -100 prefix
    result = await pool.resolve_any_entity("-100123")
    assert result is not None


@pytest.mark.anyio
async def test_resolve_any_entity_positive_id_as_user(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    entity = SimpleNamespace(id=12345, first_name="User", last_name="", bot=False)
    client.get_entity = AsyncMock(return_value=entity)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("12345")
    assert result is not None
    assert result["channel_type"] == "dm"


@pytest.mark.anyio
async def test_resolve_any_entity_no_client_raises(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)

    with pytest.raises(RuntimeError, match="no_client"):
        await pool.resolve_any_entity("@test")


@pytest.mark.anyio
async def test_resolve_any_entity_timeout(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    client.get_entity = AsyncMock(side_effect=asyncio.TimeoutError())

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@test")
    assert result is None


@pytest.mark.anyio
async def test_resolve_any_entity_username_not_found(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    client.get_entity = AsyncMock(side_effect=UsernameNotOccupiedError("nope"))

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@gone")
    assert result is None


@pytest.mark.anyio
async def test_resolve_any_entity_flood_raises_after_exhaustion(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 60
    client.get_entity = AsyncMock(side_effect=flood_err)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with pytest.raises(Exception):  # HandledFloodWaitError
        await pool.resolve_any_entity("@test")


@pytest.mark.anyio
async def test_resolve_any_entity_channel_forbidden(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    cf = ChannelForbidden(id=1, access_hash=1, title="Forbidden", broadcast=False)
    client.get_entity = AsyncMock(return_value=cf)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@forbidden")
    assert result is None


@pytest.mark.anyio
async def test_resolve_any_entity_generic_error(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    client.get_entity = AsyncMock(side_effect=RuntimeError("generic"))

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@test")
    assert result is None


@pytest.mark.anyio
async def test_resolve_any_entity_with_preferred_phone(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    entity = SimpleNamespace(id=1, title="Ch", broadcast=True, megagroup=False, gigagroup=False,
                             forum=False, monoforum=False, scam=False, fake=False, restricted=False)
    client.get_entity = AsyncMock(return_value=entity)

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.resolve_any_entity("@ch", phone="+7001")
    assert result is not None


# ---------------------------------------------------------------------------
# ClientPool: fetch_channel_meta
# Missing lines: 1004-1067
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_fetch_channel_meta_group_returns_none(pool):
    result = await pool.fetch_channel_meta(1, "group")
    assert result is None


@pytest.mark.anyio
async def test_fetch_channel_meta_no_client(pool):
    result = await pool.fetch_channel_meta(1, "channel")
    assert result is None


@pytest.mark.anyio
async def test_fetch_channel_meta_success(pool):
    client = AsyncMock()
    entity = SimpleNamespace()
    client.get_entity = AsyncMock(return_value=entity)

    full_chat = MagicMock()
    full_chat.about = "Test about"
    full_chat.linked_chat_id = 42
    full_result = MagicMock()
    full_result.full_chat = full_chat
    client.side_effect = AsyncMock(return_value=full_result)  # _client(request)
    client.get_entity = AsyncMock(return_value=entity)

    session = TelegramTransportSession(client, disconnect_on_close=False)
    pool.clients["+7001"] = session

    with patch.object(pool, "get_available_client", return_value=(session, "+7001")):
        # Also mock the invoke_request on the session to return full_result
        with patch.object(session, "invoke_request", return_value=full_result):
            with patch.object(session, "resolve_entity", return_value=entity):
                result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    assert result["about"] == "Test about"
    assert result["linked_chat_id"] == 42
    assert result["has_comments"] is True


@pytest.mark.anyio
async def test_fetch_channel_meta_timeout(pool):
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=asyncio.TimeoutError())

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.fetch_channel_meta(123, "channel")
    assert result is None


@pytest.mark.anyio
async def test_fetch_channel_meta_generic_error(pool):
    client = AsyncMock()
    entity = SimpleNamespace()
    client.get_entity = AsyncMock(return_value=entity)

    session = TelegramTransportSession(client, disconnect_on_close=False)
    pool.clients["+7001"] = session

    with patch.object(pool, "get_available_client", return_value=(session, "+7001")):
        with patch.object(session, "resolve_entity", return_value=entity):
            with patch.object(session, "invoke_request", side_effect=RuntimeError("fail")):
                result = await pool.fetch_channel_meta(123, "channel")
    assert result is None


# ---------------------------------------------------------------------------
# ClientPool: _classify_entity edge cases
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_classify_entity_scam():
    entity = SimpleNamespace(scam=True, fake=False, restricted=False, monoforum=False,
                             forum=False, gigagroup=False, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "scam"
    assert deactivate is True


@pytest.mark.anyio
async def test_classify_entity_fake():
    entity = SimpleNamespace(scam=False, fake=True, restricted=False, monoforum=False,
                             forum=False, gigagroup=False, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "fake"
    assert deactivate is True


@pytest.mark.anyio
async def test_classify_entity_restricted():
    entity = SimpleNamespace(scam=False, fake=False, restricted=True, monoforum=False,
                             forum=False, gigagroup=False, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "restricted"
    assert deactivate is True


@pytest.mark.anyio
async def test_classify_entity_monoforum():
    entity = SimpleNamespace(scam=False, fake=False, restricted=False, monoforum=True,
                             forum=False, gigagroup=False, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "monoforum"
    assert deactivate is False


@pytest.mark.anyio
async def test_classify_entity_forum():
    entity = SimpleNamespace(scam=False, fake=False, restricted=False, monoforum=False,
                             forum=True, gigagroup=False, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "forum"
    assert deactivate is False


@pytest.mark.anyio
async def test_classify_entity_gigagroup():
    entity = SimpleNamespace(scam=False, fake=False, restricted=False, monoforum=False,
                             forum=False, gigagroup=True, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "gigagroup"
    assert deactivate is False


@pytest.mark.anyio
async def test_classify_entity_plain_group():
    entity = SimpleNamespace(scam=False, fake=False, restricted=False, monoforum=False,
                             forum=False, gigagroup=False, megagroup=False, broadcast=False)
    ctype, deactivate = ClientPool._classify_entity(entity)
    assert ctype == "group"
    assert deactivate is False


# ---------------------------------------------------------------------------
# ClientPool: resolve_channel edge cases
# Missing lines: 841-878
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_resolve_channel_channel_forbidden(pool):
    client = AsyncMock()
    cf = ChannelForbidden(id=1, access_hash=1, title="Forbidden", broadcast=False)
    client.get_entity = AsyncMock(return_value=cf)

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.resolve_channel("@forbidden")
    assert result is None


@pytest.mark.anyio
async def test_resolve_channel_numeric_id(pool):
    client = AsyncMock()
    entity = SimpleNamespace(id=123, title="Ch", broadcast=True, megagroup=False, gigagroup=False,
                             forum=False, monoforum=False, scam=False, fake=False, restricted=False)
    client.get_entity = AsyncMock(return_value=entity)

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.resolve_channel("-1001234567890")
    assert result is not None
    assert result["channel_id"] == 123


@pytest.mark.anyio
async def test_resolve_channel_generic_error(pool):
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=RuntimeError("unknown"))

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.resolve_channel("@test")
    assert result is None


@pytest.mark.anyio
async def test_resolve_channel_username_invalid(pool):
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=UsernameInvalidError("inv"))

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.resolve_channel("@invalid")
    assert result is None


# ---------------------------------------------------------------------------
# ClientPool: get_dialogs
# Missing lines: 1351-1361
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_dialogs_no_non_flooded_accounts(pool, mock_db):
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    mock_db.get_accounts.return_value = [
        Account(phone="+7001", is_active=True, session_string="s1", flood_wait_until=future)
    ]
    pool.clients["+7001"] = MagicMock()

    result = await pool.get_dialogs()
    assert result == []


# ---------------------------------------------------------------------------
# ClientPool: remove_client with active leases
# Missing lines: 554-573
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_remove_client_cleans_up_all_state(pool, mock_auth, mock_db):
    client = AsyncMock()
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool._in_use.add("+7001")
    pool._dialogs_fetched.add("+7001")
    pool._store_cached_dialogs("+7001", "full", [{"channel_id": 1}])
    pool._premium_flood_wait_until["+7001"] = datetime.now(timezone.utc) + timedelta(seconds=60)

    await pool.remove_client("+7001")

    assert "+7001" not in pool.clients
    assert "+7001" not in pool._in_use
    assert "+7001" not in pool._dialogs_fetched
    assert pool._get_cached_dialogs("+7001", "full") is None
    assert "+7001" not in pool._premium_flood_wait_until


@pytest.mark.anyio
async def test_remove_client_with_active_leases(pool, mock_auth, mock_db):
    client = AsyncMock()
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    lease = MagicMock()
    lease.disconnect_on_release = True
    pool._active_leases["+7001"].append(lease)

    # Need to mock _backend_router
    pool._backend_router = MagicMock()
    pool._backend_router.release = AsyncMock()

    await pool.remove_client("+7001")

    pool._backend_router.release.assert_called_once_with(lease)
    assert "+7001" not in pool.clients


# ---------------------------------------------------------------------------
# ClientPool: reconnect_phone
# Missing lines: 539-552
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reconnect_phone_not_connected(pool):
    result = await pool.reconnect_phone("+7001")
    assert result is False


@pytest.mark.anyio
async def test_reconnect_phone_already_connected(pool):
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.reconnect_phone("+7001")
    assert result is True


@pytest.mark.anyio
async def test_reconnect_phone_reconnect_succeeds(pool):
    client = AsyncMock()
    client.is_connected = MagicMock(side_effect=[False, True])
    client.connect = AsyncMock()
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.reconnect_phone("+7001")
    assert result is True
    client.connect.assert_awaited_once()


@pytest.mark.anyio
async def test_reconnect_phone_reconnect_fails(pool):
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=False)
    client.connect = AsyncMock(side_effect=RuntimeError("fail"))
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.reconnect_phone("+7001")
    assert result is False


# ---------------------------------------------------------------------------
# ClientPool: disconnect_all edge cases
# Missing lines: 586-601
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_disconnect_all_timeout_forces_cleanup(pool):
    client = AsyncMock()
    client.disconnect = AsyncMock(side_effect=asyncio.TimeoutError())
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    async def _slow_remove(phone):
        raise asyncio.TimeoutError()

    pool.remove_client = _slow_remove
    await pool.disconnect_all()

    assert "+7001" not in pool.clients


@pytest.mark.anyio
async def test_disconnect_all_general_error(pool):
    async def _error_remove(phone):
        raise RuntimeError("unexpected")

    pool.remove_client = _error_remove
    pool.clients["+7001"] = MagicMock()

    await pool.disconnect_all()


# ---------------------------------------------------------------------------
# ClientPool: get_users_info with session fallback
# Missing lines: 754-758, 798-799, 802, 827, 845
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_users_info_no_direct_session(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, is_primary=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    pool = ClientPool(mock_auth, mock_db)

    client = AsyncMock()
    client.get_me.return_value = MagicMock(first_name="F", last_name="L", username="u", premium=False)
    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    # Put a non-TelethonTransportSession in clients so _direct_session returns None
    pool.clients["+7001"] = MagicMock()

    lease = MagicMock()
    lease.session = transport_session
    lease.disconnect_on_release = True
    pool._backend_router = MagicMock()
    pool._backend_router.acquire_client = AsyncMock(return_value=lease)
    pool._backend_router.release = AsyncMock()

    info = await pool.get_users_info(include_avatar=False)
    assert len(info) == 1
    assert info[0].phone == "+7001"
    pool._backend_router.release.assert_called_once_with(lease)


@pytest.mark.anyio
async def test_get_users_info_error_skips_account(pool, mock_db):
    acc = Account(phone="+7001", is_active=True, is_primary=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    client = AsyncMock()
    client.get_me = AsyncMock(side_effect=RuntimeError("broken"))
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    info = await pool.get_users_info(include_avatar=False)
    assert len(info) == 0


@pytest.mark.anyio
async def test_get_users_info_no_account_for_phone(pool, mock_db):
    acc = Account(phone="+7001", is_active=True, is_primary=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    pool.clients["+7001"] = MagicMock()  # Not TelegramTransportSession

    # _get_account_for_phone returns None
    pool._session_overrides.clear()
    mock_db.get_accounts.return_value = [acc]
    pool._lease_pool = MagicMock()
    pool._lease_pool.get_account = AsyncMock(return_value=None)

    info = await pool.get_users_info(include_avatar=False)
    assert len(info) == 0


# ---------------------------------------------------------------------------
# ClientPool: normalize_runtime_config
# Missing lines: 612-622
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_normalize_runtime_config_none():
    config = ClientPool._normalize_runtime_config(None)
    assert config.backend_mode == "auto"
    assert config.cli_transport == "hybrid"


@pytest.mark.anyio
async def test_normalize_runtime_config_invalid_backend():
    rc = TelegramRuntimeConfig(backend_mode="invalid_mode", cli_transport="in_process")
    config = ClientPool._normalize_runtime_config(rc)
    assert config.backend_mode == "auto"


@pytest.mark.anyio
async def test_normalize_runtime_config_invalid_transport():
    rc = TelegramRuntimeConfig(backend_mode="auto", cli_transport="invalid_transport")
    config = ClientPool._normalize_runtime_config(rc)
    assert config.cli_transport == "hybrid"


# ---------------------------------------------------------------------------
# ClientPool: get_premium_unavailability_reason
# Missing lines: 428, 437, 446
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_premium_unavailability_not_connected(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, is_premium=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    pool = ClientPool(mock_auth, mock_db)
    # Not in pool.clients
    reason = await pool.get_premium_unavailability_reason()
    assert "не подключён" in reason


@pytest.mark.anyio
async def test_premium_unavailability_some_blocked(mock_auth, mock_db):
    acc = Account(phone="+7001", is_active=True, is_premium=True, session_string="s1")
    mock_db.get_accounts.return_value = [acc]

    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = MagicMock()
    # Not all blocked — no premium flood
    reason = await pool.get_premium_unavailability_reason()
    assert "недоступен" in reason


# ---------------------------------------------------------------------------
# ClientPool: leave_channels
# Missing lines: 1215-1257
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_leave_channels_no_client(pool):
    result = await pool.leave_channels("+7001", [(123, "channel")])
    assert result == {123: False}


@pytest.mark.anyio
async def test_leave_channels_dm_type(pool):
    client = AsyncMock()
    entity = SimpleNamespace()
    client.get_entity = AsyncMock(return_value=entity)
    client.delete_dialog = AsyncMock()

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_client_by_phone", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.leave_channels("+7001", [(123, "dm")])

    assert result[123] is True


@pytest.mark.anyio
async def test_leave_channels_generic_error(pool):
    client = AsyncMock()
    entity = SimpleNamespace()
    client.get_entity = AsyncMock(return_value=entity)
    client.delete_dialog = AsyncMock(side_effect=RuntimeError("fail"))

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_client_by_phone", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.leave_channels("+7001", [(123, "channel")])

    assert result[123] is False


# ---------------------------------------------------------------------------
# ClientPool: get_forum_topics edge cases
# Missing lines: 1303-1306, 1310-1321, 1343-1347
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_forum_topics_no_client(pool):
    result = await pool.get_forum_topics(1)
    assert result == []


@pytest.mark.anyio
async def test_get_forum_topics_dialogs_fetched_but_still_fails(pool, mock_db):
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool._dialogs_fetched.add("+7001")

    # No username in DB
    mock_db.get_channel_by_channel_id.return_value = Channel(channel_id=1, username=None)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.get_forum_topics(1)
    assert result == []


@pytest.mark.anyio
async def test_get_forum_topics_resolves_by_username(pool, mock_db):
    client = AsyncMock()
    entity = SimpleNamespace()

    # First resolve (PeerChannel) fails, second (username) succeeds
    client.get_entity = AsyncMock(side_effect=[ValueError(), entity])

    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool._dialogs_fetched.add("+7001")  # So it skips warm_dialog_cache

    topic = MagicMock()
    topic.id = 1
    topic.title = "General"
    topic.icon_emoji_id = None
    topic.date = None

    response = MagicMock()
    response.topics = [topic]
    client.side_effect = AsyncMock(return_value=response)  # for invoke_request

    session = TelegramTransportSession(client, disconnect_on_close=False)
    mock_db.get_channel_by_channel_id.return_value = Channel(channel_id=1, username="mychannel")

    with patch.object(pool, "get_available_client", return_value=(session, "+7001")):
        with patch.object(session, "invoke_request", return_value=response):
            result = await pool.get_forum_topics(1)

    assert len(result) == 1
    assert result[0]["title"] == "General"


# ---------------------------------------------------------------------------
# Collector: _get_media_type edge cases
# Missing lines: 438-439, 468-473, 495, etc
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_media_type_document_no_attributes():
    media = MessageMediaDocument(document=None)
    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "document"


@pytest.mark.anyio
async def test_get_media_type_video_note():
    attr = DocumentAttributeVideo(duration=10, w=100, h=100, round_message=True)
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)
    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "video_note"


@pytest.mark.anyio
async def test_get_media_type_audio():
    attr = DocumentAttributeAudio(duration=10, voice=False)
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)
    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "audio"


@pytest.mark.anyio
async def test_get_media_type_gif():
    attr = DocumentAttributeAnimated()
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)
    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "gif"


@pytest.mark.anyio
async def test_get_media_type_web_page():
    msg = SimpleNamespace(media=MessageMediaWebPage(webpage=SimpleNamespace()))
    assert Collector._get_media_type(msg) == "web_page"


@pytest.mark.anyio
async def test_get_media_type_location():
    msg = SimpleNamespace(media=MessageMediaGeo(geo=SimpleNamespace()))
    assert Collector._get_media_type(msg) == "location"


@pytest.mark.anyio
async def test_get_media_type_geo_live():
    msg = SimpleNamespace(media=MessageMediaGeoLive(geo=SimpleNamespace(), period=60))
    assert Collector._get_media_type(msg) == "geo_live"


@pytest.mark.anyio
async def test_get_media_type_contact():
    msg = SimpleNamespace(media=MessageMediaContact(
        phone_number="", first_name="", last_name="", vcard="", user_id=0
    ))
    assert Collector._get_media_type(msg) == "contact"


@pytest.mark.anyio
async def test_get_media_type_dice():
    msg = SimpleNamespace(media=MessageMediaDice(emoticon="🎲", value=6))
    assert Collector._get_media_type(msg) == "dice"


@pytest.mark.anyio
async def test_get_media_type_game():
    game = SimpleNamespace(id=0, access_hash=0, short_name="", title="", description="")
    msg = SimpleNamespace(media=MessageMediaGame(game=game))
    assert Collector._get_media_type(msg) == "game"


@pytest.mark.anyio
async def test_get_media_type_unknown():
    msg = SimpleNamespace(media=SimpleNamespace())
    assert Collector._get_media_type(msg) == "unknown"


# ---------------------------------------------------------------------------
# Collector: _extract_reactions edge cases
# Missing lines: 1267-1268, 1279, 1289-1290
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_extract_reactions_no_emoticon_no_document_id():
    """Reaction with no emoticon and no document_id should be skipped."""
    reaction_obj = SimpleNamespace(emoticon=None, document_id=None)
    result_item = SimpleNamespace(reaction=reaction_obj, count=3)
    msg = SimpleNamespace(reactions=SimpleNamespace(results=[result_item]))
    assert Collector._extract_reactions(msg) is None


@pytest.mark.anyio
async def test_extract_reactions_no_reaction_attr():
    """Reaction item with reaction=None should be skipped."""
    result_item = SimpleNamespace(reaction=None, count=1)
    msg = SimpleNamespace(reactions=SimpleNamespace(results=[result_item]))
    assert Collector._extract_reactions(msg) is None


@pytest.mark.anyio
async def test_extract_reactions_mixed_valid_and_invalid():
    """Only valid reactions should be included."""
    valid_reaction = SimpleNamespace(emoticon="👍")
    invalid_reaction = SimpleNamespace(emoticon=None, document_id=None)

    items = [
        SimpleNamespace(reaction=valid_reaction, count=5),
        SimpleNamespace(reaction=None, count=1),
        SimpleNamespace(reaction=invalid_reaction, count=0),
    ]
    msg = SimpleNamespace(reactions=SimpleNamespace(results=items))
    result = Collector._extract_reactions(msg)
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert parsed[0]["emoji"] == "👍"


# ---------------------------------------------------------------------------
# Collector: _get_sender_name edge cases
# Missing lines: 1289-1290
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_sender_name_no_sender():
    msg = SimpleNamespace(sender=None)
    assert Collector._get_sender_name(msg) is None


@pytest.mark.anyio
async def test_get_sender_name_with_title():
    sender = SimpleNamespace(title="My Channel")
    msg = SimpleNamespace(sender=sender)
    assert Collector._get_sender_name(msg) == "My Channel"


@pytest.mark.anyio
async def test_get_sender_name_with_first_and_last():
    sender = SimpleNamespace(first_name="Ivan", last_name="Petrov")
    msg = SimpleNamespace(sender=sender)
    assert Collector._get_sender_name(msg) == "Ivan Petrov"


@pytest.mark.anyio
async def test_get_sender_name_with_first_only():
    sender = SimpleNamespace(first_name="Ivan", last_name="")
    msg = SimpleNamespace(sender=sender)
    assert Collector._get_sender_name(msg) == "Ivan"


@pytest.mark.anyio
async def test_get_sender_name_empty_names():
    sender = SimpleNamespace(first_name="", last_name="")
    msg = SimpleNamespace(sender=sender)
    assert Collector._get_sender_name(msg) is None


# ---------------------------------------------------------------------------
# Collector: _precheck_sample with cancellation
# Missing lines: 957-958, 966
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_precheck_sample_breaks_on_cancel():
    pool = make_mock_pool()
    db = MagicMock()
    config = SchedulerConfig()
    collector = Collector(pool, db, config)
    collector._cancel_event.set()

    session = MagicMock()

    async def _stream(*a, **kw):
        yield SimpleNamespace(text="a" * 20)
        yield SimpleNamespace(text="b" * 20)

    session.iter_messages = MagicMock(return_value=_stream())

    result = await collector._precheck_sample(session, SimpleNamespace(), limit=10)
    assert result == []  # cancelled before any items


# ---------------------------------------------------------------------------
# Collector: collect_all_channels cancellation
# Missing lines: 351-352
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_all_channels_cancellation():
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    db = MagicMock()
    db.get_channels = AsyncMock(return_value=[
        Channel(channel_id=1, title="Ch1"),
        Channel(channel_id=2, title="Ch2"),
    ])
    db.get_setting = AsyncMock(return_value=None)

    config = SchedulerConfig()
    collector = Collector(pool, db, config)

    # Cancel immediately
    await collector.cancel()

    stats = await collector.collect_all_channels()
    assert stats["channels"] == 0


# ---------------------------------------------------------------------------
# Collector: _fallback_collection_availability
# Missing lines: 155-161
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_fallback_availability_with_clients():
    pool = make_mock_pool(clients={"+7001": object()})
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    # Remove get_stats_availability from pool
    del pool.get_stats_availability
    result = await collector.get_collection_availability()
    assert result.state == "available"


@pytest.mark.anyio
async def test_fallback_availability_no_clients():
    pool = make_mock_pool(clients={})
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    del pool.get_stats_availability
    result = await collector.get_collection_availability()
    assert result.state == "no_connected_active"


# ---------------------------------------------------------------------------
# Collector: is_running / is_stats_running properties
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_is_running_initially_false():
    pool = make_mock_pool()
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    assert collector.is_running is False
    assert collector.is_stats_running is False


@pytest.mark.anyio
async def test_delay_between_channels_sec():
    pool = make_mock_pool()
    config = SchedulerConfig(delay_between_channels_sec=5)
    collector = Collector(pool, MagicMock(), config)
    assert collector.delay_between_channels_sec == 5


# ---------------------------------------------------------------------------
# Collector: _log_collection_unavailability_once
# Missing lines: 146-171
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_log_unavailability_deduplication():
    pool = make_mock_pool()
    collector = Collector(pool, MagicMock(), SchedulerConfig())

    # First call logs
    collector._log_collection_unavailability_once(state="all_flooded", retry_after_sec=120,
                                                   next_available_at=datetime.now(timezone.utc))
    assert collector._last_unavailability_log is not None

    # Same signature — should not re-log (just debug)
    collector._log_collection_unavailability_once(state="all_flooded", retry_after_sec=120,
                                                   next_available_at=collector._last_unavailability_log[2])

    # Different state — should log again
    collector._log_collection_unavailability_once(state="no_connected_active")
    assert collector._last_unavailability_log[0] == "no_connected_active"


@pytest.mark.anyio
async def test_reset_unavailability_log():
    pool = make_mock_pool()
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    collector._last_unavailability_log = ("state", None, None)
    collector._reset_collection_unavailability_log()
    assert collector._last_unavailability_log is None


# ---------------------------------------------------------------------------
# Collector: _maybe_auto_delete
# Missing lines: 280-294
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_maybe_auto_delete_disabled():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    collector = Collector(pool, db, SchedulerConfig())

    result = await collector._maybe_auto_delete(123)
    assert result is False


@pytest.mark.anyio
async def test_maybe_auto_delete_enabled():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="1")
    db.delete_messages_for_channel = AsyncMock(return_value=42)
    collector = Collector(pool, db, SchedulerConfig())

    result = await collector._maybe_auto_delete(123)
    assert result is True
    db.delete_messages_for_channel.assert_called_once_with(123)


@pytest.mark.anyio
async def test_maybe_auto_delete_error():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="1")
    db.delete_messages_for_channel = AsyncMock(side_effect=RuntimeError("fail"))
    collector = Collector(pool, db, SchedulerConfig())

    result = await collector._maybe_auto_delete(123)
    assert result is False


# ---------------------------------------------------------------------------
# Collector: sample_channel
# Missing lines: 982-1052
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_sample_channel_no_client():
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector.sample_channel(123)
    assert result == []


@pytest.mark.anyio
async def test_sample_channel_resolve_entity_fails():
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector.sample_channel(123)
    assert result == []


@pytest.mark.anyio
async def test_sample_channel_flood_wait():
    client = AsyncMock()
    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 30
    client.get_entity = AsyncMock(side_effect=flood_err)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector.sample_channel(123)
    assert result == []


@pytest.mark.anyio
async def test_sample_channel_with_messages():
    entity = SimpleNamespace()
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=entity)

    async def _stream(*a, **kw):
        yield SimpleNamespace(id=1, text="hello", date=datetime.now(timezone.utc), media=None)
        yield SimpleNamespace(id=2, text="world", date=datetime.now(timezone.utc), media=None)

    client.iter_messages = MagicMock(return_value=_stream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector.sample_channel(123, limit=2)

    assert len(result) == 2
    assert result[0]["message_id"] == 1


@pytest.mark.anyio
async def test_sample_channel_cancelled_mid_stream():
    entity = SimpleNamespace()
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=entity)

    call_count = {"n": 0}

    async def _stream(*a, **kw):
        yield SimpleNamespace(id=1, text="hello", date=datetime.now(timezone.utc), media=None)
        call_count["n"] += 1

    client.iter_messages = MagicMock(return_value=_stream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    await collector.cancel()  # Cancel before streaming

    result = await collector.sample_channel(123)
    assert result == []


# ---------------------------------------------------------------------------
# Collector: _collect_channel_stats
# Missing lines: 1116-1123, 1138, 1166, 1187-1188, 1208
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_stats_username_fallback_fails():
    channel = Channel(channel_id=123, username="gone")
    client = AsyncMock()
    # First by username: UsernameNotOccupiedError, then PeerChannel: generic error
    client.get_entity = AsyncMock(
        side_effect=[UsernameNotOccupiedError("gone"), RuntimeError("all fail")]
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    db = MagicMock()
    db.save_channel_stats = AsyncMock()
    db.create_rename_event = AsyncMock()
    db.set_channels_filtered_bulk = AsyncMock()
    db.update_channel_meta = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    result = await collector._collect_channel_stats(channel)
    assert result is None


@pytest.mark.anyio
async def test_collect_stats_no_username_resolves_numeric():
    channel = Channel(channel_id=123)
    entity = SimpleNamespace(id=123, date=None)
    full_result = MagicMock()
    full_result.full_chat.participants_count = 100

    client = AsyncMock()
    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(transport_session, "+7001")))
    db = MagicMock()
    db.save_channel_stats = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())

    async def _stream(*a, **kw):
        return
        yield  # make it an async generator

    with patch.object(transport_session, "resolve_entity", return_value=entity):
        with patch.object(transport_session, "fetch_full_channel", return_value=full_result):
            with patch.object(transport_session, "stream_messages", return_value=_stream()):
                result = await collector._collect_channel_stats(channel)

    assert result is not None
    assert result.subscriber_count == 100


@pytest.mark.anyio
async def test_collect_stats_timeout_during_messages():
    channel = Channel(channel_id=123, username="test")
    entity = SimpleNamespace(id=123, date=None)
    full_result = MagicMock()
    full_result.full_chat.participants_count = 50

    client = AsyncMock()
    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(transport_session, "+7001")))
    db = MagicMock()
    db.save_channel_stats = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())

    async def _stream_timeout(*a, **kw):
        raise asyncio.TimeoutError()
        yield  # noqa: unreachable

    with patch.object(transport_session, "resolve_entity", return_value=entity):
        with patch.object(transport_session, "fetch_full_channel", return_value=full_result):
            with patch.object(transport_session, "stream_messages", return_value=_stream_timeout()):
                result = await collector._collect_channel_stats(channel)

    assert result is not None
    assert result.subscriber_count == 50


@pytest.mark.anyio
async def test_collect_stats_updates_channel_type():
    channel = Channel(channel_id=123, username="test", channel_type=None)
    entity = SimpleNamespace(id=123, date=None, broadcast=True, megagroup=False,
                             gigagroup=False, forum=False, monoforum=False,
                             scam=False, fake=False, restricted=False)
    full_result = MagicMock()
    full_result.full_chat.participants_count = 10

    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=entity)
    client.invoke = AsyncMock(return_value=full_result)

    async def _stream(*a, **kw):
        return
        yield

    client.iter_messages = MagicMock(return_value=_stream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    pool._classify_entity = ClientPool._classify_entity

    db = MagicMock()
    db.save_channel_stats = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel_stats(channel)

    db.set_channel_type.assert_called_once_with(123, "channel")


# ---------------------------------------------------------------------------
# Collector: collect_all_stats with AllStatsClientsFloodedError
# Missing lines: 1237-1242, 1247-1250
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_all_stats_waits_on_flood():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channels = AsyncMock(return_value=[Channel(channel_id=1, title="Ch")])

    config = SchedulerConfig()
    collector = Collector(pool, db, config)

    next_at = datetime.now(timezone.utc) + timedelta(seconds=1)

    call_count = {"n": 0}

    async def _collect_stats(channel):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise AllStatsClientsFloodedError(retry_after_sec=0, next_available_at=next_at)
        return ChannelStats(channel_id=1, subscriber_count=10)

    collector._collect_channel_stats = _collect_stats

    stats = await collector.collect_all_stats()
    assert stats["channels"] == 1
    assert stats["errors"] == 0


@pytest.mark.anyio
async def test_collect_all_stats_generic_error():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channels = AsyncMock(return_value=[Channel(channel_id=1, title="Ch")])

    collector = Collector(pool, db, SchedulerConfig())
    collector._collect_channel_stats = AsyncMock(side_effect=RuntimeError("fail"))

    stats = await collector.collect_all_stats()
    assert stats["errors"] == 1
    assert stats["channels"] == 0


# ---------------------------------------------------------------------------
# Collector: _discover_phone_for_channel
# Missing lines: 919-947
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_discover_phone_for_channel_success():
    pool = make_mock_pool()

    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=SimpleNamespace())
    client.get_dialogs = AsyncMock(return_value=[])

    pool.get_client_by_phone = AsyncMock(return_value=(client, "+7002"))
    pool.connected_phones = MagicMock(return_value={"+7002"})
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    pool.release_client = AsyncMock()

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector._discover_phone_for_channel(123, "+7001")

    assert result == "+7002"


@pytest.mark.anyio
async def test_discover_phone_for_channel_flood():
    pool = make_mock_pool()

    client = MagicMock()
    client.is_connected = MagicMock(return_value=True)
    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 30
    client.get_entity = AsyncMock(side_effect=flood_err)
    client.get_dialogs = AsyncMock(return_value=[])

    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool.get_client_by_phone = AsyncMock(return_value=(transport_session, "+7002"))
    pool.connected_phones = MagicMock(return_value={"+7002"})
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    pool.report_flood = AsyncMock()
    pool.release_client = AsyncMock()

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector._discover_phone_for_channel(123, "+7001")

    assert result is None
    pool.report_flood.assert_awaited_once()


@pytest.mark.anyio
async def test_discover_phone_for_channel_no_access():
    pool = make_mock_pool()

    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=ValueError("no access"))

    pool.get_client_by_phone = AsyncMock(return_value=(client, "+7002"))
    pool.connected_phones = MagicMock(return_value={"+7002"})
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    pool.release_client = AsyncMock()

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector._discover_phone_for_channel(123, "+7001")

    assert result is None


@pytest.mark.anyio
async def test_discover_phone_for_channel_client_none():
    pool = make_mock_pool()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.connected_phones = MagicMock(return_value={"+7002"})

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector._discover_phone_for_channel(123, "+7001")
    assert result is None


# ---------------------------------------------------------------------------
# Collector: _channel_still_exists
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_channel_still_exists_true():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=Channel(channel_id=1, title="Ch"))

    collector = Collector(pool, db, SchedulerConfig())
    assert await collector._channel_still_exists(1) is True


@pytest.mark.anyio
async def test_channel_still_exists_false():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=None)

    collector = Collector(pool, db, SchedulerConfig())
    assert await collector._channel_still_exists(1) is False


# ---------------------------------------------------------------------------
# Collector: cancel / is_cancelled
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_and_is_cancelled():
    pool = make_mock_pool()
    collector = Collector(pool, MagicMock(), SchedulerConfig())
    assert collector.is_cancelled is False
    await collector.cancel()
    assert collector.is_cancelled is True


# ---------------------------------------------------------------------------
# Collector: _raise_collection_unavailability branches
# Missing lines: 176-192
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_raise_collection_unavailability_all_flooded():
    pool = make_mock_pool()
    next_at = datetime.now(timezone.utc) + timedelta(seconds=120)
    pool.get_stats_availability = AsyncMock(
        return_value=StatsClientAvailability(
            state="all_flooded", retry_after_sec=120, next_available_at_utc=next_at
        )
    )

    collector = Collector(pool, MagicMock(), SchedulerConfig())

    with pytest.raises(AllCollectionClientsFloodedError) as exc:
        await collector._raise_collection_unavailability()

    assert exc.value.retry_after_sec == 120


@pytest.mark.anyio
async def test_raise_collection_unavailability_no_active():
    pool = make_mock_pool()
    pool.get_collection_availability = AsyncMock(
        return_value=SimpleNamespace(state="no_connected_active", retry_after_sec=None,
                                      next_available_at_utc=None)
    )

    collector = Collector(pool, MagicMock(), SchedulerConfig())

    with pytest.raises(NoActiveCollectionClientsError):
        await collector._raise_collection_unavailability()


# ---------------------------------------------------------------------------
# Collector: _collect_channel error paths
# Missing lines: 602-606, 608-609, 616-622, 783-784, 788-789,
#   791-792, 795, 806-813, 826-833, 839-840
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_channel_numeric_timeout():
    """Timeout resolving numeric PeerChannel -> returns 0."""
    channel = Channel(channel_id=123, title="Test")
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=asyncio.TimeoutError())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    collector = Collector(pool, MagicMock(), SchedulerConfig())

    result = await collector._collect_channel(channel)
    assert result == 0


@pytest.mark.anyio
async def test_collect_channel_numeric_flood_wait():
    """FloodWaitError on numeric resolve -> HandledFloodWaitError -> flood_wait_sec set."""
    channel = Channel(channel_id=123, title="Test")
    client = AsyncMock()
    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 30
    client.get_entity = AsyncMock(side_effect=flood_err)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))
    collector = Collector(pool, MagicMock(), SchedulerConfig())

    result = await collector._collect_channel(channel)
    # flood_wait_sec is set but channel has no PK, so loop continues until get_available_client returns None
    # On None, raises unavailability
    assert result == 0


@pytest.mark.anyio
async def test_collect_channel_private_group_invalidates_bad_phone():
    """When private group's phone can't resolve entity, clear and discover."""
    channel = Channel(id=7, channel_id=123, title="Test", preferred_phone="+7001")
    client = AsyncMock()
    # First get_entity (PeerChannel) fails with ValueError
    client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=(client, "+7001")),
        get_client_by_phone=AsyncMock(side_effect=[
            (client, "+7001"),  # first: get_client_by_phone for preferred_phone
            None,  # discover: candidate has no available client
        ]),
    )
    pool.get_phone_for_channel = MagicMock(return_value=None)
    pool.clear_channel_phone = MagicMock()
    pool.connected_phones = MagicMock(return_value={"+7001", "+7002"})

    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=channel)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.set_channel_active = AsyncMock()
    db.repos.channels.update_channel_preferred_phone = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    # This should try the phone, fail, clear it, try to discover, and skip without a traceback.
    result = await collector._collect_channel(channel)

    assert result == 0
    pool.clear_channel_phone.assert_called_once_with(123)
    db.repos.channels.update_channel_preferred_phone.assert_called_once_with(123, None)
    db.set_channel_active.assert_awaited_once_with(7, False)


@pytest.mark.anyio
async def test_collect_channel_cancelled_during_batch():
    """Cancel event set during message streaming breaks the loop."""
    ch = Channel(channel_id=-100999, title="Test", username="test", last_collected_id=0)
    pool = make_mock_pool()
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.insert_messages_batch = AsyncMock()
    db.update_channel_last_id = AsyncMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=ch)
    db.get_channel_by_pk = AsyncMock(return_value=None)
    db.filter_repo.count_matching_prefixes_in_other_channels = AsyncMock(return_value=0)
    db.get_notification_queries = AsyncMock(return_value=[])

    msgs = [make_mock_message(i, text=f"msg {i}") for i in range(1, 21)]

    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=SimpleNamespace())

    call_idx = {"n": 0}

    def _iter_factory(*a, **kw):
        if call_idx["n"] == 0:
            call_idx["n"] += 1
            return AsyncIterMessages(msgs)
        return AsyncIterEmpty()

    client.iter_messages = MagicMock(side_effect=_iter_factory)

    pool.get_available_client = AsyncMock(return_value=(client, "+7001"))

    collector = Collector(pool, db, SchedulerConfig())
    await collector.cancel()

    count = await collector._collect_channel(ch)
    assert count == 0  # all messages dropped because channel deleted


@pytest.mark.anyio
async def test_collect_channel_username_not_occupied_deactivates():
    """UsernameNotOccupiedError from run_with_flood_wait(_collect_messages()) deactivates channel."""
    ch = Channel(id=5, channel_id=123, title="Test", username="gone")
    entity = SimpleNamespace()
    msgs = [make_mock_message(11, text="msg")]

    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=entity)

    # First call: resolve succeeds; stream_messages raises UsernameNotOccupiedError
    def _iter(*a, **kw):
        return AsyncIterMessages(msgs)

    client.iter_messages = MagicMock(side_effect=_iter)

    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(transport_session, "+7001")))
    db = MagicMock()
    db.set_channel_active = AsyncMock()
    db.insert_messages_batch = AsyncMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=ch)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.get_notification_queries = AsyncMock(return_value=[])

    collector = Collector(pool, db, SchedulerConfig())

    # Patch stream_messages to raise UsernameNotOccupiedError
    with patch.object(transport_session, "resolve_entity", return_value=entity):
        with patch.object(transport_session, "stream_messages", side_effect=UsernameNotOccupiedError("gone")):
            with pytest.raises(UsernameNotOccupiedError):
                await collector._collect_channel(ch)

    db.set_channel_active.assert_called_once_with(5, False)


@pytest.mark.anyio
async def test_collect_channel_flush_error_in_finally():
    """Error flushing batch in finally block sets stop_due_to_persistence_error."""
    ch = Channel(channel_id=123, title="Test", username="test")
    msgs = [make_mock_message(11, text="msg")]

    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=SimpleNamespace())

    def _iter(*a, **kw):
        return AsyncIterMessages(msgs)

    client.iter_messages = MagicMock(side_effect=_iter)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))

    db = MagicMock()
    db.insert_messages_batch = AsyncMock(side_effect=RuntimeError("db error"))
    db.get_channel_by_channel_id = AsyncMock(return_value=ch)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)

    collector = Collector(pool, db, SchedulerConfig())
    result = await collector._collect_channel(ch)
    # Should return without crashing (error caught in finally)
    assert result == 0


@pytest.mark.anyio
async def test_collect_channel_update_last_id_error():
    """Error updating last_collected_id in finally is caught."""
    ch = Channel(channel_id=123, title="Test", username="test")
    msgs = [make_mock_message(11, text="msg")]

    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=SimpleNamespace())

    def _iter(*a, **kw):
        return AsyncIterMessages(msgs)

    client.iter_messages = MagicMock(side_effect=_iter)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7001")))

    db = MagicMock()
    db.insert_messages_batch = AsyncMock()

    # Mock execute to return message_id 11 for the flush batch check
    mock_cursor = MagicMock()
    mock_cursor.fetchall = AsyncMock(return_value=[{"message_id": 11}])
    db.execute = AsyncMock(return_value=mock_cursor)

    db.update_channel_last_id = AsyncMock(side_effect=RuntimeError("db error"))
    db.get_channel_by_channel_id = AsyncMock(return_value=ch)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.get_notification_queries = AsyncMock(return_value=[])

    collector = Collector(pool, db, SchedulerConfig())
    result = await collector._collect_channel(ch)
    assert result == 1


# ---------------------------------------------------------------------------
# Collector: _is_auto_delete_enabled caching
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_auto_delete_caching():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="1")

    collector = Collector(pool, db, SchedulerConfig())

    # First call reads DB
    result1 = await collector._is_auto_delete_enabled()
    assert result1 is True
    assert db.get_setting.await_count == 1

    # Second call uses cache
    result2 = await collector._is_auto_delete_enabled()
    assert result2 is True
    assert db.get_setting.await_count == 1  # not called again


# ---------------------------------------------------------------------------
# Collector: collect_channel_stats error paths
# Missing lines: 1065-1085 (NoActiveStatsClientsError without get_stats_availability)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_stats_no_availability_fn():
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    del pool.get_stats_availability  # No availability function

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    channel = Channel(channel_id=123)

    with pytest.raises(NoActiveStatsClientsError):
        await collector._collect_channel_stats(channel)


@pytest.mark.anyio
async def test_collect_stats_availability_not_async():
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    pool.get_stats_availability = MagicMock(return_value="not_async")  # not awaitable

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    channel = Channel(channel_id=123)

    with pytest.raises(NoActiveStatsClientsError):
        await collector._collect_channel_stats(channel)


# ---------------------------------------------------------------------------
# Collector: collect_channel_stats with flood wait retry
# Missing lines: 1217-1221
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_stats_flood_wait_retries():
    channel = Channel(channel_id=123, username="test")

    client = AsyncMock()
    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 30
    client.get_entity = AsyncMock(side_effect=flood_err)

    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool = make_mock_pool(get_available_client=AsyncMock(
        side_effect=[(transport_session, "+7001"), None]
    ))
    pool.get_stats_availability = AsyncMock(
        return_value=StatsClientAvailability(
            state="all_flooded",
            retry_after_sec=30,
            next_available_at_utc=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
    )

    db = MagicMock()

    collector = Collector(pool, db, SchedulerConfig())
    with pytest.raises(AllStatsClientsFloodedError):
        await collector._collect_channel_stats(channel)


# ---------------------------------------------------------------------------
# Collector: get_stats_availability delegates
# Missing lines: 121-123
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_stats_availability_delegates():
    pool = make_mock_pool()
    pool.get_stats_availability = AsyncMock(
        return_value=StatsClientAvailability(state="available")
    )

    collector = Collector(pool, MagicMock(), SchedulerConfig())
    result = await collector.get_stats_availability()
    assert result.state == "available"
