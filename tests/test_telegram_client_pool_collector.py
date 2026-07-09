"""Tests for Telegram client pool and collector edge paths."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
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
    PeerChannel,
    PeerChat,
    PeerUser,
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
from src.telegram.flood_wait import HandledFloodWaitError
from tests.helpers import (
    AsyncIterEmpty,
    AsyncIterMessages,
    make_mock_message,
    make_mock_pool,
    wait_until,
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


@pytest.mark.anyio
async def test_initialize_filters_requested_phones(pool, mock_db):
    accounts = [
        Account(phone="+111", session_string="s1", is_active=True),
        Account(phone="+222", session_string="s2", is_active=True),
    ]
    mock_db.get_accounts.return_value = accounts
    connected: list[str] = []

    async def connect_account(account: Account):
        connected.append(account.phone)
        lease = MagicMock()
        lease.session.fetch_me = AsyncMock(return_value=MagicMock(premium=False))
        return lease

    pool._connect_account = AsyncMock(side_effect=connect_account)  # type: ignore[method-assign]
    pool._backend_router.release = AsyncMock()

    await pool.initialize(phones=("+222",))

    assert connected == ["+222"]
    pool._backend_router.release.assert_awaited_once()


@pytest.mark.anyio
async def test_resolve_dialog_entity_uses_cached_dm_type_before_warm(pool, mock_db):
    client = AsyncMock()
    client.get_input_entity = AsyncMock(return_value="dm-entity")
    client.get_dialogs = AsyncMock(return_value=[])
    mock_db.repos.dialog_cache.get_dialog.return_value = {
        "channel_id": 5832576119,
        "title": "Work1nw",
        "username": "Work1nw",
        "channel_type": "dm",
    }

    result = await pool.resolve_dialog_entity(client, "+1", 5832576119)

    assert result == "dm-entity"
    peer = client.get_input_entity.await_args.args[0]
    assert isinstance(peer, PeerUser)
    client.get_dialogs.assert_not_awaited()


@pytest.mark.anyio
async def test_resolve_dialog_entity_legacy_group_uses_peerchat(pool, mock_db):
    """Legacy small groups (channel_type == 'group') must resolve via PeerChat (#633)."""
    client = AsyncMock()
    client.get_input_entity = AsyncMock(return_value="chat-entity")
    client.get_dialogs = AsyncMock(return_value=[])
    mock_db.repos.dialog_cache.get_dialog.return_value = {
        "channel_id": -1234567,
        "title": "Legacy Group",
        "username": None,
        "channel_type": "group",
    }

    result = await pool.resolve_dialog_entity(client, "+1", -1234567)

    assert result == "chat-entity"
    peer = client.get_input_entity.await_args.args[0]
    assert isinstance(peer, PeerChat)
    assert peer.chat_id == 1234567
    client.get_dialogs.assert_not_awaited()


@pytest.mark.anyio
async def test_resolve_dialog_entity_channel_uses_peerchannel(pool, mock_db):
    """Regular channels still resolve via PeerChannel (#633 regression guard)."""
    client = AsyncMock()
    client.get_input_entity = AsyncMock(return_value="channel-entity")
    client.get_dialogs = AsyncMock(return_value=[])
    mock_db.repos.dialog_cache.get_dialog.return_value = {
        "channel_id": -1001234567,
        "title": "Some Channel",
        "username": "somechannel",
        "channel_type": "channel",
    }

    result = await pool.resolve_dialog_entity(client, "+1", -1001234567)

    assert result == "channel-entity"
    peer = client.get_input_entity.await_args.args[0]
    assert isinstance(peer, PeerChannel)
    assert peer.channel_id == 1001234567


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
    async def _noop() -> None:
        return None

    pool._warming_task = asyncio.create_task(_noop())
    await wait_until(lambda: pool._warming_task.done())
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
async def test_warm_all_dialogs_logs_when_preferred_phone_persist_fails(
    mock_auth, mock_db, caplog
):
    """A failed DB persist must not crash warming and must be logged (#676)."""
    pool = ClientPool(mock_auth, mock_db)

    entity = MagicMock(spec=TLChannel)
    entity.id = 12345
    dialog = MagicMock()
    dialog.entity = entity

    raw_client = AsyncMock()
    raw_client.get_dialogs = AsyncMock(return_value=[dialog])
    session = TelegramTransportSession(raw_client, disconnect_on_close=False)
    pool.clients["+7001"] = session

    mock_db.repos.channels.update_channel_preferred_phone = AsyncMock(
        side_effect=RuntimeError("db locked")
    )

    with patch.object(pool, "get_client_by_phone", return_value=(session, "+7001")):
        with caplog.at_level(logging.DEBUG, logger="src.telegram.pool_dialogs"):
            await pool.warm_all_dialogs()

    # In-memory map is still updated despite the DB failure...
    assert pool.get_phone_for_channel(12345) == "+7001"
    # ...and the failure is surfaced in the logs instead of being swallowed.
    assert any(
        "read or persist preferred_phone" in rec.message and "12345" in rec.message
        for rec in caplog.records
    )
    # The traceback is preserved via exc_info, matching collector.py (#676 review).
    assert any(rec.exc_info for rec in caplog.records if "12345" in rec.message)


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


@pytest.mark.anyio
async def test_warm_all_dialogs_skips_active_flood_wait(mock_auth, mock_db):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    mock_db.get_accounts.return_value = [
        Account(phone="+7001", session_string="s1", is_active=True, flood_wait_until=future)
    ]
    pool = ClientPool(mock_auth, mock_db)
    pool.clients["+7001"] = MagicMock()

    with patch.object(pool, "get_client_by_phone", new=AsyncMock()) as get_client:
        await pool.warm_all_dialogs()

    get_client.assert_not_awaited()


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


@pytest.mark.anyio
async def test_get_dialogs_for_phone_coalesces_parallel_refresh(mock_auth, mock_db):
    pool = ClientPool(mock_auth, mock_db)
    calls = 0

    async def fake_fetch(self, phone, include_dm, mode, cache_mode):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return [{"channel_id": 1, "title": "Coalesced", "channel_type": "channel"}]

    with patch.object(ClientPool, "_fetch_dialogs_for_phone", fake_fetch):
        first, second = await asyncio.gather(
            pool.get_dialogs_for_phone("+7001", include_dm=True, mode="full", refresh=True),
            pool.get_dialogs_for_phone("+7001", include_dm=True, mode="full", refresh=True),
        )

    assert calls == 1
    assert first == second


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

    with pytest.raises(HandledFloodWaitError):
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
# ClientPool: fetch_channel_meta account routing (#808)
# ---------------------------------------------------------------------------


def _meta_session_with_full(linked_chat_id=42):
    """Build a transport session whose fetch returns a full-channel result."""
    client = AsyncMock()
    entity = SimpleNamespace()
    full_chat = MagicMock()
    full_chat.about = "About"
    full_chat.linked_chat_id = linked_chat_id
    full_result = MagicMock()
    full_result.full_chat = full_chat
    session = TelegramTransportSession(client, disconnect_on_close=False)
    return session, entity, full_result


@pytest.mark.anyio
async def test_fetch_channel_meta_routes_to_preferred_phone_from_map(pool):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    pool.register_channel_phone(123, "+7001")
    pool.mark_dialogs_fetched("+7001")

    by_phone = AsyncMock(return_value=(session, "+7001"))
    avail = AsyncMock()
    with patch.object(pool, "get_client_by_phone", by_phone):
        with patch.object(pool, "get_available_client", avail):
            with patch.object(session, "resolve_entity", return_value=entity):
                with patch.object(session, "invoke_request", return_value=full_result):
                    result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    by_phone.assert_awaited_once_with("+7001")
    avail.assert_not_awaited()


@pytest.mark.anyio
async def test_fetch_channel_meta_routes_to_preferred_phone_from_db(pool, mock_db):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    pool.mark_dialogs_fetched("+7001")
    mock_db.repos.channels.get_preferred_phone.return_value = "+7001"

    by_phone = AsyncMock(return_value=(session, "+7001"))
    avail = AsyncMock()
    with patch.object(pool, "get_client_by_phone", by_phone):
        with patch.object(pool, "get_available_client", avail):
            with patch.object(session, "resolve_entity", return_value=entity):
                with patch.object(session, "invoke_request", return_value=full_result):
                    result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    by_phone.assert_awaited_once_with("+7001")
    avail.assert_not_awaited()


@pytest.mark.anyio
async def test_fetch_channel_meta_waits_for_warm_when_unmapped(pool, mock_db):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    pool.mark_dialogs_fetched("+7001")
    mock_db.repos.channels.get_preferred_phone.return_value = None

    async def _warm(timeout=30.0):
        pool.register_channel_phone(123, "+7001")

    with patch.object(pool, "is_warming", return_value=True):
        with patch.object(pool, "wait_for_warm", AsyncMock(side_effect=_warm)) as waiter:
            with patch.object(
                pool, "get_client_by_phone", AsyncMock(return_value=(session, "+7001"))
            ):
                with patch.object(session, "resolve_entity", return_value=entity):
                    with patch.object(session, "invoke_request", return_value=full_result):
                        result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    waiter.assert_awaited_once()


@pytest.mark.anyio
async def test_fetch_channel_meta_falls_back_to_available_client(pool, mock_db):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    mock_db.repos.channels.get_preferred_phone.return_value = None

    avail = AsyncMock(return_value=(session, "+7001"))
    with patch.object(pool, "is_warming", return_value=False):
        with patch.object(pool, "get_available_client", avail):
            with patch.object(session, "resolve_entity", return_value=entity):
                with patch.object(session, "invoke_request", return_value=full_result):
                    result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    avail.assert_awaited_once()


@pytest.mark.anyio
async def test_fetch_channel_meta_unresolved_entity_logs_debug_not_warning(pool, caplog):
    session = TelegramTransportSession(AsyncMock(), disconnect_on_close=False)
    pool.clients["+7001"] = session

    with patch.object(pool, "get_available_client", return_value=(session, "+7001")):
        with patch.object(
            session,
            "resolve_entity",
            side_effect=ValueError("Could not find the input entity for PeerChannel"),
        ):
            with caplog.at_level(logging.DEBUG):
                result = await pool.fetch_channel_meta(123, "channel")

    assert result is None
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("entity unresolved" in r.getMessage() for r in caplog.records)


@pytest.mark.anyio
async def test_fetch_channel_meta_registers_phone_on_success(pool, mock_db):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    mock_db.repos.channels.get_preferred_phone.return_value = None

    with patch.object(pool, "is_warming", return_value=False):
        with patch.object(pool, "get_available_client", return_value=(session, "+7001")):
            with patch.object(session, "resolve_entity", return_value=entity):
                with patch.object(session, "invoke_request", return_value=full_result):
                    result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    assert pool.get_phone_for_channel(123) == "+7001"
    mock_db.repos.channels.update_channel_preferred_phone.assert_awaited_once_with(
        123, "+7001"
    )


@pytest.mark.anyio
async def test_fetch_channel_meta_does_not_persist_when_preferred_exists(pool, mock_db):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    pool.mark_dialogs_fetched("+7001")
    mock_db.repos.channels.get_preferred_phone.return_value = "+7001"

    with patch.object(
        pool, "get_client_by_phone", AsyncMock(return_value=(session, "+7001"))
    ):
        with patch.object(session, "resolve_entity", return_value=entity):
            with patch.object(session, "invoke_request", return_value=full_result):
                result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    mock_db.repos.channels.update_channel_preferred_phone.assert_not_awaited()


@pytest.mark.anyio
async def test_fetch_channel_meta_clears_stale_preferred_on_channel_private(pool, mock_db):
    session = TelegramTransportSession(AsyncMock(), disconnect_on_close=False)
    pool.clients["+7001"] = session
    pool.register_channel_phone(123, "+7001")
    pool.mark_dialogs_fetched("+7001")
    mock_db.repos.channels.get_preferred_phone.return_value = "+7001"

    with patch.object(
        pool, "get_client_by_phone", AsyncMock(return_value=(session, "+7001"))
    ):
        with patch.object(
            session, "resolve_entity", side_effect=ChannelPrivateError(request=None)
        ):
            result = await pool.fetch_channel_meta(123, "channel")

    assert result is None
    assert pool.get_phone_for_channel(123) is None
    mock_db.repos.channels.update_channel_preferred_phone.assert_awaited_once_with(
        123, None
    )


@pytest.mark.anyio
async def test_fetch_channel_meta_prewarms_when_dialogs_not_fetched(pool):
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7001"] = session
    pool.register_channel_phone(123, "+7001")
    # dialogs NOT marked fetched → must warm first

    with patch.object(
        pool, "get_client_by_phone", AsyncMock(return_value=(session, "+7001"))
    ):
        with patch.object(session, "warm_dialog_cache", AsyncMock()) as warm:
            with patch.object(session, "resolve_entity", return_value=entity):
                with patch.object(session, "invoke_request", return_value=full_result):
                    result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    warm.assert_awaited()
    assert pool.is_dialogs_fetched("+7001")


@pytest.mark.anyio
async def test_fetch_channel_meta_clears_stale_preferred_on_unresolved(pool, mock_db):
    # A stored preferred phone that can no longer resolve the entity is stale
    # (membership change) — ValueError path must clear it like ChannelPrivateError.
    session = TelegramTransportSession(AsyncMock(), disconnect_on_close=False)
    pool.clients["+7001"] = session
    pool.register_channel_phone(123, "+7001")
    pool.mark_dialogs_fetched("+7001")
    mock_db.repos.channels.get_preferred_phone.return_value = "+7001"

    with patch.object(
        pool, "get_client_by_phone", AsyncMock(return_value=(session, "+7001"))
    ):
        with patch.object(
            session,
            "resolve_entity",
            side_effect=ValueError("Could not find the input entity"),
        ):
            result = await pool.fetch_channel_meta(123, "channel")

    assert result is None
    assert pool.get_phone_for_channel(123) is None
    mock_db.repos.channels.update_channel_preferred_phone.assert_awaited_once_with(
        123, None
    )


@pytest.mark.anyio
async def test_fetch_channel_meta_unresolved_keeps_map_on_available_client(pool, mock_db):
    # On the available-client (non-preferred) path a ValueError just means no
    # warmed account sees the channel — must NOT erase a good mapping on a guess.
    session = TelegramTransportSession(AsyncMock(), disconnect_on_close=False)
    pool.clients["+7001"] = session
    # No mapping for channel 123 → routing falls back to get_available_client.
    mock_db.repos.channels.get_preferred_phone.return_value = None
    clear = MagicMock()

    with patch.object(pool, "is_warming", return_value=False):
        with patch.object(pool, "clear_channel_phone", clear):
            with patch.object(
                pool, "get_available_client", AsyncMock(return_value=(session, "+7001"))
            ):
                with patch.object(
                    session,
                    "resolve_entity",
                    side_effect=ValueError("Could not find the input entity"),
                ):
                    result = await pool.fetch_channel_meta(123, "channel")

    assert result is None
    clear.assert_not_called()
    mock_db.repos.channels.update_channel_preferred_phone.assert_not_awaited()


@pytest.mark.anyio
async def test_fetch_channel_meta_updates_db_when_fallback_account_differs(pool, mock_db):
    # DB preferred is "+7001" but unavailable (flood) → fall back to "+7002" and
    # succeed. The DB must be corrected to the account that actually worked,
    # not left pointing at the stale preferred (Claude review #1 on #809).
    session, entity, full_result = _meta_session_with_full()
    pool.clients["+7002"] = session
    mock_db.repos.channels.get_preferred_phone.return_value = "+7001"

    with patch.object(pool, "is_warming", return_value=False):
        with patch.object(pool, "get_client_by_phone", AsyncMock(return_value=None)):
            with patch.object(
                pool, "get_available_client", AsyncMock(return_value=(session, "+7002"))
            ):
                with patch.object(session, "resolve_entity", return_value=entity):
                    with patch.object(
                        session, "invoke_request", return_value=full_result
                    ):
                        result = await pool.fetch_channel_meta(123, "channel")

    assert result is not None
    assert pool.get_phone_for_channel(123) == "+7002"
    mock_db.repos.channels.update_channel_preferred_phone.assert_awaited_once_with(
        123, "+7002"
    )


@pytest.mark.anyio
async def test_fetch_channel_meta_keeps_db_preferred_when_map_stale(pool, mock_db):
    # In-memory map points at "+7001" (stale) but DB preferred is a different
    # valid account "+7002". A failure on "+7001" must NOT erase the good DB
    # value (Codex review P2 on #809).
    session = TelegramTransportSession(AsyncMock(), disconnect_on_close=False)
    pool.clients["+7001"] = session
    pool.register_channel_phone(123, "+7001")
    pool.mark_dialogs_fetched("+7001")
    mock_db.repos.channels.get_preferred_phone.return_value = "+7002"

    with patch.object(
        pool, "get_client_by_phone", AsyncMock(return_value=(session, "+7001"))
    ):
        with patch.object(
            session, "resolve_entity", side_effect=ChannelPrivateError(request=None)
        ):
            result = await pool.fetch_channel_meta(123, "channel")

    assert result is None
    # in-memory map cleared (it pointed at the failed account)...
    assert pool.get_phone_for_channel(123) is None
    # ...but the valid DB preferred is left intact.
    mock_db.repos.channels.update_channel_preferred_phone.assert_not_awaited()


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
async def test_resolve_channel_forbidden_is_not_gone_even_with_signal_gone(pool):
    """Regression (#858 review): ChannelForbidden is an access error for the resolving
    account, NOT a deletion. Even with signal_gone=True it must return None (not the
    {"gone": True} sentinel), so refresh-types never deactivates a live private channel
    just because an arbitrary account can't see it."""
    client = AsyncMock()
    cf = ChannelForbidden(id=1, access_hash=1, title="Forbidden", broadcast=False)
    client.get_entity = AsyncMock(return_value=cf)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.resolve_channel("@forbidden", signal_gone=True)
    assert result is None  # NOT {"gone": True}


@pytest.mark.anyio
async def test_resolve_channel_username_invalid_is_gone_with_signal_gone(pool):
    """A genuinely definitive not-found (invalid/unoccupied username) DOES return the
    gone sentinel under signal_gone — that path is unchanged by the #858 fix."""
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=UsernameInvalidError("inv"))
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        result = await pool.resolve_channel("@invalid", signal_gone=True)
    assert result == {"gone": True}


@pytest.mark.anyio
async def test_resolve_channel_stale_username_falls_back_to_numeric_id(pool):
    """#858 review: a live channel that dropped/renamed its @username must NOT be
    deactivated. When username resolution is gone but the numeric id resolves to a
    live entity, the live dict is returned (refresh-types leaves the channel active).
    Stored channel_id is bare-positive, so the fallback is the bare id."""
    client = AsyncMock()
    live = SimpleNamespace(id=123, title="Renamed", broadcast=True, megagroup=False,
                           gigagroup=False, forum=False, monoforum=False, scam=False,
                           fake=False, restricted=False)
    seen = {}

    async def fake_get_entity(peer, *a, **kw):
        # username peer raises gone; numeric PeerChannel resolves to the live entity
        if isinstance(peer, str):
            raise UsernameNotOccupiedError("nope")
        seen["peer"] = peer
        return live

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    # Live numeric resolution routes through the owning account (#875 review).
    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value="+7001")), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )), \
         patch.object(pool, "get_client_by_phone", AsyncMock(return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         ))):
        result = await pool.resolve_channel(
            "@oldname", signal_gone=True, numeric_fallback="123"
        )
    assert result is not None
    assert result.get("gone") is None
    assert result["channel_id"] == 123
    # The numeric fallback builds the correct bare PeerChannel.
    assert isinstance(seen["peer"], PeerChannel)
    assert seen["peer"].channel_id == 123


@pytest.mark.anyio
async def test_resolve_channel_gone_by_both_confirmed_via_owner_account(pool):
    """A truly deleted channel: username peer raises UsernameNotOccupiedError, and the
    numeric PeerChannel raises the REAL Telethon error (a plain ValueError) ON THE OWNING
    ACCOUNT → the second definitive not-found yields the gone sentinel, so the dead channel
    still gets deactivated. The numeric miss is only trusted as gone when it runs on the
    account that owns the channel (#875 review)."""
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        if isinstance(peer, str):
            raise UsernameNotOccupiedError("nope")
        # A numeric PeerChannel that no longer exists raises a plain ValueError.
        raise ValueError("Could not find the input entity for PeerChannel(channel_id=123)")

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value="+7001")), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )), \
         patch.object(pool, "get_client_by_phone", AsyncMock(return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         ))):
        result = await pool.resolve_channel(
            "@oldname", signal_gone=True, numeric_fallback="123"
        )
    assert result == {"gone": True}


@pytest.mark.anyio
async def test_resolve_channel_numeric_miss_without_owner_is_review(pool):
    """#875 redesign (the core fix): when NO owning account is known, a numeric-peer
    ValueError is just an arbitrary account's local cache-miss, NOT proof of deletion.
    It must NOT deactivate (never gone) — and rather than a silent skip it surfaces the
    review sentinel so the uncertain channel is quarantined for human review."""
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        if isinstance(peer, str):
            raise UsernameNotOccupiedError("nope")
        raise ValueError("Could not find the input entity for PeerChannel(channel_id=123)")

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    # No owner known → the numeric retry runs on an arbitrary account and must NOT confirm gone.
    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value=None)), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )):
        result = await pool.resolve_channel(
            "@oldname", signal_gone=True, numeric_fallback="123"
        )
    assert result == {"review": True, "reason": "numeric_unresolved"}  # NOT {"gone": True}


@pytest.mark.anyio
async def test_resolve_channel_review_when_owner_unavailable(pool):
    """#875 redesign: when the owning account is known but currently unavailable
    (flood/disconnected), we cannot trustworthily confirm deletion → quarantine for
    review (uncertain), never gone and never a silent skip."""
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        if isinstance(peer, str):
            raise UsernameNotOccupiedError("nope")
        raise ValueError("Could not find the input entity")

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value="+7001")), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )), \
         patch.object(pool, "get_client_by_phone", AsyncMock(return_value=None)):
        result = await pool.resolve_channel(
            "@oldname", signal_gone=True, numeric_fallback="123"
        )
    assert result == {"review": True, "reason": "numeric_unresolved"}  # owner unavailable → review


@pytest.mark.anyio
async def test_resolve_channel_numeric_access_denied_is_not_gone(pool):
    """#858 review follow-up: username gone but the numeric peer raises ChannelPrivateError
    (access denied, NOT deleted) → must return None, never gone. Deactivating here would
    drop a live channel that is simply not accessible from this account."""
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        if isinstance(peer, str):
            raise UsernameNotOccupiedError("nope")
        raise ChannelPrivateError("private")

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value="+7001")), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )), \
         patch.object(pool, "get_client_by_phone", AsyncMock(return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         ))):
        result = await pool.resolve_channel(
            "@oldname", signal_gone=True, numeric_fallback="123"
        )
    assert result is None


@pytest.mark.anyio
async def test_resolve_channel_purely_numeric_unresolved_is_review(pool):
    """#875 redesign: a channel with no @username resolves only by numeric id. A bare
    numeric identifier (numeric_fallback == identifier) has no username-gone signal to
    escalate from, so its first lookup routes through the owning account. With no owner
    known the numeric miss is uncertain → review (quarantine), never an auto-deactivation.
    A username-less channel can therefore only be deactivated via manual review-confirm."""
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        raise ValueError("Could not find the input entity for PeerChannel(channel_id=123)")

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value=None)), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )):
        result = await pool.resolve_channel(
            "123", signal_gone=True, numeric_fallback="123"
        )
    assert result == {"review": True, "reason": "numeric_unresolved"}


@pytest.mark.anyio
async def test_resolve_channel_purely_numeric_gone_via_owner(pool):
    """#875 redesign: a username-less channel CAN still auto-deactivate when its known
    owning account confirms the not-found — that is a trusted deletion signal, not a
    cache-miss."""
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        raise ValueError("Could not find the input entity for PeerChannel(channel_id=123)")

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value="+7001")), \
         patch.object(pool, "get_client_by_phone", AsyncMock(return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         ))):
        result = await pool.resolve_channel(
            "123", signal_gone=True, numeric_fallback="123"
        )
    assert result == {"gone": True}


@pytest.mark.anyio
@pytest.mark.parametrize(
    "ident,expected_peer_id",
    # Bare-positive stays as-is; a real MTProto marked id (-100<bare>, i.e. the bare id
    # plus the 1e12 marker) is stripped back to the bare id. A short value like "-100123"
    # is NOT a marked id (markers are 1e12+) and must be left untouched (#875 review):
    # bare_channel_id only strips genuine markers, unlike the old naive 3-char slice.
    [("123", 123), ("-1000000000123", 123)],
)
async def test_resolve_channel_strips_bot_api_prefix(pool, ident, expected_peer_id):
    """#858 review follow-up (framing A): a numeric identifier builds a PeerChannel with the
    Bot-API -100 prefix stripped (via bare_channel_id), so a -100<bare> input hits the
    correct peer rather than PeerChannel(100<bare>)."""
    seen = {}
    entity = SimpleNamespace(id=expected_peer_id, title="Ch", broadcast=True, megagroup=False,
                             gigagroup=False, forum=False, monoforum=False, scam=False,
                             fake=False, restricted=False)
    client = AsyncMock()

    async def fake_get_entity(peer, *a, **kw):
        seen["peer"] = peer
        return entity

    client.get_entity = AsyncMock(side_effect=fake_get_entity)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "get_available_client", return_value=(
        TelegramTransportSession(client, disconnect_on_close=False), "+7001"
    )):
        await pool.resolve_channel(ident)
    assert isinstance(seen["peer"], PeerChannel)
    assert seen["peer"].channel_id == expected_peer_id


@pytest.mark.anyio
async def test_resolve_channel_no_fallback_when_identifier_is_already_numeric(pool):
    """When the channel has no username the identifier already IS the numeric id
    (fallback == identifier → no redundant retry). A numeric peer that no longer resolves
    raises the real Telethon ValueError; with NO owning account known this miss is just a
    cache-miss on an arbitrary account → review (quarantine), never an auto-gone (#875
    redesign). Deactivation of a username-less channel only happens via manual
    review-confirm or an owner-confirmed not-found."""
    client = AsyncMock()
    client.get_entity = AsyncMock(
        side_effect=ValueError("Could not find the input entity for PeerChannel(channel_id=123)")
    )
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    with patch.object(pool, "_owner_phone_for", AsyncMock(return_value=None)), \
         patch.object(pool, "get_available_client", return_value=(
            TelegramTransportSession(client, disconnect_on_close=False), "+7001"
         )):
        result = await pool.resolve_channel(
            "123", signal_gone=True, numeric_fallback="123"
        )
    assert result == {"review": True, "reason": "numeric_unresolved"}
    # identifier == fallback → resolved exactly once, no redundant retry.
    # (Telethon warms the dialog cache once on the first ValueError, then retries the same
    # peer — both calls are the same numeric resolution, so this is one logical attempt.)
    assert client.get_entity.await_count >= 1


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
async def test_remove_client_disconnect_timeout_marks_client_removed(pool, monkeypatch):
    async def _hung_disconnect():
        await asyncio.Event().wait()

    monkeypatch.setattr("src.telegram.pool_lifecycle.REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC", 0.01)
    client = AsyncMock()
    client.disconnect = AsyncMock(side_effect=_hung_disconnect)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool._in_use.add("+7001")
    pool._dialogs_fetched.add("+7001")

    await asyncio.wait_for(pool.remove_client("+7001"), timeout=0.1)

    client.disconnect.assert_awaited_once()
    assert "+7001" not in pool.clients
    assert "+7001" not in pool._in_use
    assert "+7001" not in pool._dialogs_fetched


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
async def test_get_forum_topics_channel_invalid_returns_empty_without_traceback(pool, caplog):
    client = AsyncMock()
    entity = SimpleNamespace()
    session = TelegramTransportSession(client, disconnect_on_close=False)
    pool.clients["+7001"] = session

    with (
        patch.object(pool, "get_available_client", return_value=(session, "+7001")),
        patch.object(session, "resolve_entity", new=AsyncMock(return_value=entity)),
        patch.object(
            session,
            "fetch_forum_topics",
            new=AsyncMock(side_effect=ChannelInvalidError(request=None)),
        ),
        caplog.at_level(logging.INFO, logger="src.telegram.pool_dialogs"),
    ):
        result = await pool.get_forum_topics(1)

    assert result == []
    assert "get_forum_topics unavailable for channel 1" in caplog.text
    assert "Traceback" not in caplog.text


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
async def test_set_stats_all_running_updates_running_properties():
    pool = make_mock_pool()
    collector = Collector(pool, MagicMock(), SchedulerConfig())

    collector.set_stats_all_running(True)

    assert collector.is_running is True
    assert collector.is_stats_running is True

    collector.set_stats_all_running(False)

    assert collector.is_running is False
    assert collector.is_stats_running is False


@pytest.mark.anyio
async def test_available_stats_worker_count_uses_available_stats_clients():
    pool = make_mock_pool(
        clients={"+7001": object(), "+7002": object(), "+7003": object()},
        available_stats_client_count=AsyncMock(return_value=1),
    )
    collector = Collector(pool, MagicMock(), SchedulerConfig(stats_worker_count=3))

    assert await collector.available_stats_worker_count() == 1


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
    entity = SimpleNamespace(id=123, title="Test", date=None)
    full_result = MagicMock()
    full_result.full_chat.participants_count = 100

    client = AsyncMock()
    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(transport_session, "+7001")))
    db = MagicMock()
    db.save_channel_stats = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.repos.channels.set_channel_type = AsyncMock()
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
    entity = SimpleNamespace(id=123, title="Test", date=None)
    full_result = MagicMock()
    full_result.full_chat.participants_count = 50

    client = AsyncMock()
    transport_session = TelegramTransportSession(client, disconnect_on_close=False)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(transport_session, "+7001")))
    db = MagicMock()
    db.save_channel_stats = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.repos.channels.set_channel_type = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())

    async def _stream_timeout(*a, **kw):
        if kw.get("_never_yield"):
            yield None
        raise asyncio.TimeoutError()

    with patch.object(transport_session, "resolve_entity", return_value=entity):
        with patch.object(transport_session, "fetch_full_channel", return_value=full_result):
            with patch.object(transport_session, "stream_messages", return_value=_stream_timeout()):
                result = await collector._collect_channel_stats(channel)

    assert result is not None
    assert result.subscriber_count == 50


@pytest.mark.anyio
async def test_collect_stats_updates_channel_type():
    channel = Channel(channel_id=123, username="test", channel_type=None)
    entity = SimpleNamespace(id=123, title="Test Channel", date=None, broadcast=True, megagroup=False,
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
    db.repos.channels.set_channel_type = AsyncMock()
    db.repos.channels.update_channel_created_at = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel_stats(channel)

    db.set_channel_type.assert_called_once_with(123, "channel")


# ---------------------------------------------------------------------------
# Collector: collect_all_stats with AllStatsClientsFloodedError
# Missing lines: 1237-1242, 1247-1250
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_collect_all_stats_defers_on_flood_without_sleep(monkeypatch):
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channels = AsyncMock(return_value=[Channel(channel_id=1, title="Ch")])
    db.get_latest_stats_for_all = AsyncMock(return_value={})

    config = SchedulerConfig()
    collector = Collector(pool, db, config)

    sleep = AsyncMock()
    monkeypatch.setattr("src.telegram.collector_mixins.stats.asyncio.sleep", sleep)

    next_at = datetime.now(timezone.utc) + timedelta(hours=1)

    call_count = {"n": 0}

    async def _collect_stats(channel):
        call_count["n"] += 1
        raise AllStatsClientsFloodedError(retry_after_sec=3600, next_available_at=next_at)

    collector._collect_channel_stats = _collect_stats

    stats = await collector.collect_all_stats()
    assert call_count["n"] == 1
    assert stats["channels"] == 0
    assert stats["errors"] == 0
    assert stats["remaining"] == 1
    assert stats["limited"] is True
    assert stats["flood_wait_until"] == next_at.isoformat()
    sleep.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_all_stats_respects_max_channels():
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channels = AsyncMock(
        return_value=[
            Channel(channel_id=1, title="Ch1"),
            Channel(channel_id=2, title="Ch2"),
            Channel(channel_id=3, title="Ch3"),
        ]
    )
    db.get_latest_stats_for_all = AsyncMock(return_value={})

    collector = Collector(pool, db, SchedulerConfig(stats_all_worker_count=1))
    seen: list[int] = []

    async def _collect_stats(channel):
        seen.append(channel.channel_id)
        return ChannelStats(channel_id=channel.channel_id, subscriber_count=10)

    collector._collect_channel_stats = _collect_stats

    stats = await collector.collect_all_stats(max_channels=2)
    assert seen == [1, 2]
    assert stats["channels"] == 2
    assert stats["errors"] == 0
    assert stats["remaining"] == 1
    assert stats["limited"] is True
    assert stats["total"] == 3
    assert stats["max_channels"] == 2


@pytest.mark.anyio
async def test_collect_all_stats_prioritizes_channels_without_or_stale_stats():
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new = datetime(2026, 2, 1, tzinfo=timezone.utc)
    pool = make_mock_pool()
    db = MagicMock()
    db.get_channels = AsyncMock(
        return_value=[
            Channel(channel_id=1, title="Fresh"),
            Channel(channel_id=2, title="Missing"),
            Channel(channel_id=3, title="Stale"),
        ]
    )
    db.get_latest_stats_for_all = AsyncMock(
        return_value={
            1: ChannelStats(channel_id=1, subscriber_count=10, collected_at=new),
            3: ChannelStats(channel_id=3, subscriber_count=10, collected_at=old),
        }
    )

    collector = Collector(pool, db, SchedulerConfig(stats_all_worker_count=1))
    seen: list[int] = []

    async def _collect_stats(channel):
        seen.append(channel.channel_id)
        return ChannelStats(channel_id=channel.channel_id, subscriber_count=10)

    collector._collect_channel_stats = _collect_stats

    stats = await collector.collect_all_stats(max_channels=2)
    assert seen == [2, 3]
    assert stats["remaining"] == 1
    assert stats["limited"] is True


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
async def test_collect_channel_logs_when_clearing_preferred_phone_fails(caplog):
    """A failed DB clear during phone invalidation must be logged, not swallowed (#676)."""
    channel = Channel(id=7, channel_id=123, title="Test", preferred_phone="+7001")
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=(client, "+7001")),
        get_client_by_phone=AsyncMock(side_effect=[(client, "+7001"), None]),
    )
    pool.get_phone_for_channel = MagicMock(return_value=None)
    pool.clear_channel_phone = MagicMock()
    pool.connected_phones = MagicMock(return_value={"+7001", "+7002"})

    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=channel)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.set_channel_active = AsyncMock()
    db.repos.channels.update_channel_preferred_phone = AsyncMock(
        side_effect=RuntimeError("db locked")
    )

    collector = Collector(pool, db, SchedulerConfig())
    with caplog.at_level(logging.WARNING, logger="src.telegram.collector"):
        result = await collector._collect_channel(channel)

    # Collection still degrades gracefully (no crash, channel deactivated)...
    assert result == 0
    db.set_channel_active.assert_awaited_once_with(7, False)
    # ...and the swallowed DB write is now visible in the logs.
    assert any(
        "failed to clear stale preferred_phone" in rec.message and "123" in rec.message
        for rec in caplog.records
    )


@pytest.mark.anyio
async def test_collect_channel_logs_when_persisting_rediscovered_phone_fails(caplog):
    """A failed DB persist of the rediscovered phone must be logged, not swallowed (#676)."""
    channel = Channel(id=7, channel_id=123, title="Test", preferred_phone="+7001")
    client = AsyncMock()
    # Numeric resolve fails so the bad phone is invalidated and a rediscovery is attempted.
    client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool = make_mock_pool(
        # Second iteration: the preferred phone is unavailable AND no other
        # account is free either, so the picker's #1245 fallback to
        # get_available_client() also yields nothing — the loop then legitimately
        # raises NoActiveCollectionClientsError. (When a fallback account *is*
        # free, #1245 rotates to it instead of raising; that branch is covered by
        # test_acquire_falls_back_when_preferred_phone_unavailable.)
        get_available_client=AsyncMock(return_value=None),
        get_client_by_phone=AsyncMock(side_effect=[
            (client, "+7001"),  # initial resolve attempt on the preferred phone
            None,  # second iteration: preferred phone unavailable -> #1245 fallback
        ]),
    )
    pool.get_phone_for_channel = MagicMock(return_value="+7001")
    pool.clear_channel_phone = MagicMock()
    pool.register_channel_phone = MagicMock()
    pool.connected_phones = MagicMock(return_value={"+7001", "+7002"})

    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=channel)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.set_channel_active = AsyncMock()
    db.repos.channels.update_channel_preferred_phone = AsyncMock(
        side_effect=RuntimeError("db locked")
    )

    collector = Collector(pool, db, SchedulerConfig())
    with patch.object(collector, "_discover_phone_for_channel", AsyncMock(return_value="+7002")):
        with caplog.at_level(logging.WARNING, logger="src.telegram.collector"):
            # Iteration 1 rediscovers +7002 (logging the failed persist), then
            # `continue`. Iteration 2 finds the preferred phone unavailable and the
            # #1245 fallback empty, so the loop ultimately raises. We only care that
            # the persist failure was logged before that.
            with pytest.raises(NoActiveCollectionClientsError):
                await collector._collect_channel(channel)

    # The pool learned the rediscovered phone even though the DB write failed...
    pool.register_channel_phone.assert_called_once_with(123, "+7002")
    # ...and the failure is logged rather than silently dropped.
    assert any(
        "failed to persist rediscovered preferred_phone" in rec.message
        and "123" in rec.message
        and "+7002" in rec.message
        for rec in caplog.records
    )


@pytest.mark.anyio
async def test_transient_owner_flood_does_not_deactivate_private_channel(caplog):
    """#1245 regression (cycle-review BLOCK): a private single-owner channel whose
    only member account is transiently flood-waited must NOT be deactivated, and
    its preferred_phone must NOT be cleared, just because the #1245 fallback routed
    the numeric resolve to a non-member account.

    Scenario (empirically the data-loss path the review flagged):
      1. Private channel (no username), preferred_phone="+7001" — the sole member.
      2. "+7001" is transiently flooded → get_client_by_phone("+7001") returns None.
      3. #1245 falls back to get_available_client() → "+7002", a NON-member.
      4. Numeric PeerChannel resolve on "+7002" raises ValueError (not a member).

    Pre-Fix-B, step 4 was read as "stale preferred" → clear preferred_phone +
    set_channel_active(False): a temporary owner flood turned into permanent
    data loss. Fix B guards clear/deactivate on "resolve ran on the channel's OWN
    preferred phone" — a miss on a fallback account keeps the channel active and
    its preferred_phone intact, skipping just this pass.
    """
    channel = Channel(id=42, channel_id=555, title="Owner Flooded", preferred_phone="+7001")
    fallback_client = AsyncMock()
    # Non-member fallback account cannot resolve the numeric PeerChannel.
    fallback_client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool = make_mock_pool(
        # Owner "+7001" is transiently flooded → None; #1245 fallback picks "+7002".
        get_client_by_phone=AsyncMock(return_value=None),
        get_available_client=AsyncMock(return_value=(fallback_client, "+7002")),
    )
    # preferred_phone lives on the channel row; the in-memory map is empty.
    pool.get_phone_for_channel = MagicMock(return_value=None)
    pool.clear_channel_phone = MagicMock()
    pool.register_channel_phone = MagicMock()
    pool.connected_phones = MagicMock(return_value={"+7001", "+7002"})

    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=channel)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.set_channel_active = AsyncMock()
    db.repos.channels.update_channel_preferred_phone = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    with caplog.at_level(logging.WARNING, logger="src.telegram.collector"):
        result = await collector._collect_channel(channel)

    # This pass collects nothing (owner flooded), but the channel survives intact.
    assert result == 0
    # The live channel must NOT be deactivated by a fallback-account miss.
    db.set_channel_active.assert_not_awaited()
    # preferred_phone must be preserved, not cleared, in both pool and DB.
    pool.clear_channel_phone.assert_not_called()
    db.repos.channels.update_channel_preferred_phone.assert_not_awaited()
    # No rediscovery is attempted off a non-member miss (would rewrite preferred).
    pool.register_channel_phone.assert_not_called()


@pytest.mark.anyio
async def test_kicked_owner_account_still_deactivates_when_no_rediscovery(caplog):
    """Fix B companion: the legitimate deactivation path must survive.

    When the numeric resolve fails ON THE CHANNEL'S OWN preferred phone (the
    account was really kicked / the channel is gone) and no other connected
    account can resolve it either, the channel must STILL be deactivated. Fix B
    only suppresses clear/deactivate for a miss on a *fallback* account
    (phone != preferred); a miss on the preferred phone itself is genuine and
    must flow through to set_channel_active(False).

    Guards Fix B from over-reaching: a mutation making the guard fire on
    phone == preferred would silently keep dead channels active — this test
    catches that by asserting the deactivation still happens.
    """
    channel = Channel(id=99, channel_id=777, title="Kicked", preferred_phone="+7001")
    client = AsyncMock()
    # Resolve on the OWN preferred account fails → genuine "account kicked / gone".
    client.get_entity = AsyncMock(side_effect=ValueError("not found"))

    pool = make_mock_pool(
        # The preferred phone IS available; the resolve on it is what fails.
        get_client_by_phone=AsyncMock(return_value=(client, "+7001")),
        get_available_client=AsyncMock(return_value=(client, "+7001")),
    )
    pool.get_phone_for_channel = MagicMock(return_value=None)
    pool.clear_channel_phone = MagicMock()
    pool.register_channel_phone = MagicMock()
    pool.connected_phones = MagicMock(return_value={"+7001"})

    db = MagicMock()
    db.get_channel_by_channel_id = AsyncMock(return_value=channel)
    db.get_channel_stats = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value=None)
    db.set_channel_active = AsyncMock()
    db.repos.channels.update_channel_preferred_phone = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    # No other account can resolve it → rediscovery finds nothing → deactivate.
    with patch.object(collector, "_discover_phone_for_channel", AsyncMock(return_value=None)):
        with caplog.at_level(logging.WARNING, logger="src.telegram.collector"):
            result = await collector._collect_channel(channel)

    assert result == 0
    # A genuine miss on the OWN preferred phone still deactivates the dead channel.
    db.set_channel_active.assert_awaited_once_with(99, False)


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
