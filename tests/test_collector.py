import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError, UsernameNotOccupiedError
from telethon.tl.types import InputPeerChannel, PeerChannel

from src.config import SchedulerConfig
from src.models import Channel, ChannelStats, CollectionTaskStatus, Message, StatsAllTaskPayload
from src.telegram.backends import TelegramTransportSession
from src.telegram.collector import (
    AllCollectionClientsFloodedError,
    Collector,
    NoActiveCollectionClientsError,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)
from tests.helpers import AsyncIterEmpty as _AsyncIterEmpty
from tests.helpers import AsyncIterMessages as _AsyncIterMessages
from tests.helpers import FakeTelethonClient, make_mock_message, make_mock_pool, make_mock_reactions


@pytest.mark.anyio
async def test_collect_no_channels(db):
    pool = make_mock_pool()
    config = SchedulerConfig()
    collector = Collector(pool, db, config)
    stats = await collector.collect_all_channels()
    assert stats["channels"] == 0
    assert stats["messages"] == 0


@pytest.mark.anyio
async def test_collect_no_clients(db):
    ch = Channel(channel_id=-100123, title="Test")
    await db.add_channel(ch)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))

    config = SchedulerConfig()
    collector = Collector(pool, db, config)
    stats = await collector.collect_all_channels()
    assert stats["channels"] == 0
    assert stats["messages"] == 0
    assert stats["errors"] == 1


@pytest.mark.anyio
async def test_collect_all_skips_filtered_channels(db):
    await db.add_channel(Channel(channel_id=-100124, title="Filtered"))
    await db.add_channel(Channel(channel_id=-100125, title="Normal"))
    await db.set_channels_filtered_bulk([(-100124, "low_uniqueness")])

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    stats = await collector.collect_all_channels()
    assert stats["channels"] == 0
    assert stats["messages"] == 0
    assert stats["errors"] == 1


@pytest.mark.anyio
async def test_collect_all_invalid_min_subscribers_setting_falls_back_to_zero(db):
    await db.set_setting("min_subscribers_filter", "broken")
    ch = Channel(channel_id=-100124, title="Normal")
    await db.add_channel(ch)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    stats = await collector.collect_all_channels()
    assert stats["channels"] == 0
    assert stats["errors"] == 1


@pytest.mark.anyio
async def test_collect_single_channel_raises_all_clients_flooded(db):
    ch = Channel(channel_id=-100124, title="Flooded")
    await db.add_channel(ch)

    next_available_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=None),
        get_stats_availability=AsyncMock(
            return_value=SimpleNamespace(
                state="all_flooded",
                retry_after_sec=120,
                next_available_at_utc=next_available_at,
            )
        ),
    )

    collector = Collector(pool, db, SchedulerConfig())

    with pytest.raises(AllCollectionClientsFloodedError) as exc:
        await collector.collect_single_channel(ch)

    assert exc.value.retry_after_sec == 120
    assert exc.value.next_available_at == next_available_at


@pytest.mark.anyio
async def test_collect_single_channel_raises_no_active_clients(db):
    ch = Channel(channel_id=-100125, title="No Clients")
    await db.add_channel(ch)

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=None),
        get_stats_availability=AsyncMock(
            return_value=SimpleNamespace(
                state="no_connected_active",
                retry_after_sec=None,
                next_available_at_utc=None,
            )
        ),
    )

    collector = Collector(pool, db, SchedulerConfig())

    with pytest.raises(NoActiveCollectionClientsError):
        await collector.collect_single_channel(ch)


@pytest.mark.anyio
async def test_collect_single_channel_skips_filtered(db):
    """collect_single_channel returns 0 immediately for filtered channels."""
    ch = Channel(
        channel_id=-100130,
        title="Filtered",
        is_filtered=True,
        filter_flags="non_cyrillic",
    )
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector.collect_single_channel(ch)
    assert count == 0
    # Pool should never be touched
    pool.get_available_client.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_uses_peer_channel_without_username(db):
    """_collect_channel falls back to PeerChannel when no username."""
    ch = Channel(channel_id=1970788983, title="Test Channel")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel(ch)

    call_arg = mock_client.get_entity.call_args[0][0]
    assert isinstance(call_arg, PeerChannel)
    assert call_arg.channel_id == 1970788983


@pytest.mark.anyio
async def test_collect_channel_uses_username_when_available(db):
    """_collect_channel uses cache-only PeerChannel before live username resolve."""
    ch = Channel(channel_id=1970788983, title="Test Channel", username="test_chan")
    await db.add_channel(ch)

    input_peer = InputPeerChannel(channel_id=1970788983, access_hash=123)
    mock_client = FakeTelethonClient(cached_input_entity_resolver=lambda _arg: input_peer)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel(ch)

    call_arg = mock_client.session.get_input_entity.call_args[0][0]
    assert isinstance(call_arg, PeerChannel)
    assert call_arg.channel_id == 1970788983
    mock_client.get_entity.assert_not_awaited()
    mock_client.get_input_entity.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_positive_id_end_to_end(db):
    """End-to-end: collect_all_channels falls back to live InputPeer username resolve."""
    ch = Channel(channel_id=1970788983, title="Positive ID Channel", username="my_chan")
    await db.add_channel(ch)

    input_peer = InputPeerChannel(channel_id=1970788983, access_hash=123)
    mock_client = FakeTelethonClient(
        input_entity_resolver=lambda arg: input_peer if arg == "my_chan" else ValueError("cache miss"),
        cached_input_entity_resolver=lambda _arg: ValueError("cache miss"),
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()

    assert stats["channels"] == 1
    call_arg = mock_client.get_input_entity.call_args[0][0]
    assert call_arg == "my_chan"


@pytest.mark.anyio
async def test_collect_channel_long_username_resolve_flood_sets_backoff_without_rotation(db):
    ch = Channel(
        channel_id=1970788984,
        title="Long Username Flood",
        username="long_flood",
        last_collected_id=5,
    )
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    flood_err = FloodWaitError(request=None, capture=7200)
    raw_client1 = FakeTelethonClient(entity_resolver=lambda _arg: flood_err)
    raw_client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    session1 = TelegramTransportSession(
        raw_client1,
        disconnect_on_close=False,
        phone="+7001",
        pool=pool,
    )
    session2 = TelegramTransportSession(
        raw_client2,
        disconnect_on_close=False,
        phone="+7002",
        pool=pool,
    )
    pool.get_available_client = AsyncMock(
        side_effect=[
            (session1, "+7001"),
            (session2, "+7002"),
        ]
    )
    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
    )

    with pytest.raises(UsernameResolveFloodWaitDeferredError):
        await collector._collect_channel(stored)

    pool.report_flood.assert_awaited_once_with("+7001", 7200)
    pool.get_available_client.assert_awaited_once()
    raw_client2.get_input_entity.assert_not_awaited()
    # Long resolve floods are honored for Telegram's full window so the worker
    # does not retry before the account is usable again.
    remaining = collector._get_resolve_username_backoff_remaining_sec()
    assert 7100 < remaining <= 7200


@pytest.mark.anyio
async def test_collect_channel_username_resolve_flood_does_not_rotate(db):
    ch = Channel(
        channel_id=1970788985,
        title="Short Username Flood",
        username="short_flood",
        last_collected_id=5,
    )
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    flood_err = FloodWaitError(request=None, capture=120)
    raw_client1 = FakeTelethonClient(entity_resolver=lambda _arg: flood_err)
    raw_client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    session1 = TelegramTransportSession(
        raw_client1,
        disconnect_on_close=False,
        phone="+7001",
        pool=pool,
    )
    session2 = TelegramTransportSession(
        raw_client2,
        disconnect_on_close=False,
        phone="+7002",
        pool=pool,
    )
    pool.get_available_client = AsyncMock(
        side_effect=[
            (session1, "+7001"),
            (session2, "+7002"),
        ]
    )
    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
    )

    # #552: a 120s flood is below the global-backoff threshold (300s) — the
    # channel is skipped for this run without freezing all accounts and
    # without rotating to a second account.
    result = await collector._collect_channel(stored)

    assert result == 0
    pool.report_flood.assert_awaited_once_with("+7001", 120)
    assert pool.get_available_client.await_count == 1
    raw_client2.get_input_entity.assert_not_awaited()
    assert collector._get_resolve_username_backoff_remaining_sec() == 0


@pytest.mark.anyio
async def test_collect_channel_defers_when_resolve_rate_limited(db):
    """#551: when the per-account resolve limiter is exhausted, the live
    get_input_entity call must not fire; the caller must defer the task instead
    of completing it with zero collected messages."""
    from src.telegram.rate_limiter import ResolveRateLimiter

    ch = Channel(
        channel_id=1970788990,
        title="Rate Limited",
        username="rate_limited",
        last_collected_id=5,
    )
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    raw_client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    session = TelegramTransportSession(
        raw_client,
        disconnect_on_close=False,
        phone="+7001",
        pool=pool,
    )
    pool.get_available_client = AsyncMock(return_value=(session, "+7001"))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    # Exhaust the shared pool limiter for +7001 before collection runs.
    pool._resolve_rate_limiter = ResolveRateLimiter(
        max_calls=1, window_sec=60.0, jitter_sec=0.0
    )
    assert pool._resolve_rate_limiter.try_acquire("+7001") == 0.0

    with pytest.raises(UsernameResolveRateLimitedError) as exc_info:
        await collector._collect_channel(stored)

    assert exc_info.value.phone == "+7001"
    assert 0 < exc_info.value.retry_after_sec <= 60
    raw_client.get_input_entity.assert_not_awaited()
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_cache_only_defers_on_backoff_miss(db):
    """#552: while a global resolve backoff is active, a channel whose InputPeer
    is not cached runs in cache-only mode — it raises a defer signal without
    ever calling the live resolve API."""
    ch = Channel(
        channel_id=1970788986,
        title="Backoff Active",
        username="backoff_active",
        last_collected_id=5,
    )
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    raw_client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    session = TelegramTransportSession(
        raw_client, disconnect_on_close=False, phone="+7001", pool=pool
    )
    pool.get_available_client = AsyncMock(return_value=(session, "+7001"))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    pool.set_resolve_username_backoff(600)

    with pytest.raises(UsernameResolveRateLimitedError) as exc_info:
        await collector._collect_channel(stored)

    assert exc_info.value.phone == "+7001"
    raw_client.get_input_entity.assert_not_awaited()
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_all_channels_continues_cache_only_during_backoff(db):
    """#552: a global resolve backoff no longer aborts the whole run. Channels
    that miss the cache are deferred while the run continues — cache-resolvable
    channels keep collecting on subsequent accounts."""
    await db.add_channel(
        Channel(channel_id=1970788987, title="Backoff Active 1", username="backoff_active_1")
    )
    await db.add_channel(
        Channel(channel_id=1970788988, title="Backoff Active 2", username="backoff_active_2")
    )

    raw1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    raw2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    s1 = TelegramTransportSession(raw1, disconnect_on_close=False, phone="+7001", pool=pool)
    s2 = TelegramTransportSession(raw2, disconnect_on_close=False, phone="+7002", pool=pool)
    pool.get_available_client = AsyncMock(side_effect=[(s1, "+7001"), (s2, "+7002")])
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    pool.set_resolve_username_backoff(600)

    stats = await collector.collect_all_channels()

    # Run did NOT stop on the backoff; both channels were processed (deferred).
    assert stats["errors"] == 0
    assert stats["messages"] == 0
    assert stats["deferred"] == 2
    raw1.get_input_entity.assert_not_awaited()
    raw2.get_input_entity.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_all_channels_continues_after_mid_run_resolve_flood(db):
    """#552: when the FIRST channel of a run triggers a long resolve flood mid-run,
    the run must NOT abort — the backoff it just set makes every subsequent channel
    resolve cache-only, so the run continues instead of bricking on the triggering
    channel. Regression guard for the break→continue fix (the pre-set-backoff test
    never reaches this path)."""
    await db.add_channel(
        Channel(channel_id=1970788991, title="Trigger Flood", username="trigger_flood")
    )
    await db.add_channel(
        Channel(channel_id=1970788992, title="After Flood", username="after_flood")
    )

    # Channel 1's live resolve raises a 7200s flood (> 300s threshold) → sets the
    # global backoff and raises UsernameResolveFloodWaitDeferredError mid-run.
    flood_err = FloodWaitError(request=None, capture=7200)
    raw1 = FakeTelethonClient(entity_resolver=lambda _arg: flood_err)
    # Channel 2 would resolve fine live, but the active backoff routes it cache-only,
    # so its live get_input_entity must never be awaited.
    raw2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    s1 = TelegramTransportSession(raw1, disconnect_on_close=False, phone="+7001", pool=pool)
    s2 = TelegramTransportSession(raw2, disconnect_on_close=False, phone="+7002", pool=pool)
    pool.get_available_client = AsyncMock(side_effect=[(s1, "+7001"), (s2, "+7002")])
    collector = Collector(
        pool, db, SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10)
    )

    stats = await collector.collect_all_channels()

    # The run reached channel 2 (get_available_client awaited twice) — it did NOT
    # break on channel 1's flood.
    assert pool.get_available_client.await_count == 2
    # The triggering channel is counted as deferred, not as an error.
    assert stats["errors"] == 0
    assert stats.get("deferred", 0) >= 1
    # Backoff is active for Telegram's full FloodWait window.
    remaining = collector._get_resolve_username_backoff_remaining_sec()
    assert 7100 < remaining <= 7200
    # Single source of truth (#785): the collector reads the pool's backoff, no
    # separate collector-local copy.
    assert remaining == pool.get_resolve_username_backoff_remaining_sec()
    # Channel 2 was routed cache-only — no live resolve fired.
    raw2.get_input_entity.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_defensive_backoff_when_guard_returns_none(db):
    """#785: if the pool's backoff is None after a long resolve FloodWait, the
    collector sets it defensively rather than letting all subsequent channels
    fire blind live resolves. Regression guard for the guard→collector race
    where ``_record_resolve_username_flood`` runs but the backoff is already
    expired/cleared by the time the collector reads it back."""
    ch = Channel(
        channel_id=1970788995,
        title="Defensive Backoff",
        username="defensive_backoff",
        last_collected_id=5,
    )
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    flood_err = FloodWaitError(request=None, capture=55919)
    raw_client = FakeTelethonClient(entity_resolver=lambda _arg: flood_err)
    pool = make_mock_pool()
    # Initialise all ResolveGuardMixin state so MagicMock auto-fabrication
    # does not interfere with the mixin's datetime comparisons.
    pool._resolve_ramp_up_until_utc = None
    pool._resolve_ramp_up_last_call_utc = None
    pool._resolve_ramp_up_min_interval_sec = 5.0
    pool._db = SimpleNamespace(set_setting=AsyncMock())
    session = TelegramTransportSession(
        raw_client,
        disconnect_on_close=False,
        phone="+7001",
        pool=pool,
    )
    pool.get_available_client = AsyncMock(return_value=(session, "+7001"))

    # Simulate the pool having lost its backoff: get_resolve_username_backoff_until
    # always returns None even though a long flood was just raised. The collector
    # must call set_resolve_username_backoff defensively.
    pool.get_resolve_username_backoff_until = lambda: None

    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
    )

    with pytest.raises(UsernameResolveFloodWaitDeferredError):
        await collector._collect_channel(stored)

    # The defensive set_resolve_username_backoff call must have been made with
    # the flood_wait_sec value (55919 > 300 threshold). Verify by reading the
    # raw internal state (get_resolve_username_backoff_until is overridden to
    # always return None, so we read _resolve_username_backoff_until_utc directly).
    assert pool._resolve_username_backoff_until_utc is not None
    remaining = (
        pool._resolve_username_backoff_until_utc - datetime.now(timezone.utc)
    ).total_seconds()
    assert remaining > 0, "defensive backoff must be active"
    assert remaining <= 55919
    assert pool._db.set_setting.await_count >= 1
    key, value = pool._db.set_setting.await_args.args
    assert key == "resolve_username_backoff_until_utc"
    assert datetime.fromisoformat(value) == pool._resolve_username_backoff_until_utc


@pytest.mark.anyio
async def test_collect_no_username_channel_fetches_dialogs_once(db):
    """For no-username channels get_dialogs() is called once per process to warm entity cache."""
    ch = Channel(channel_id=123, title="No Username")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_dialogs = AsyncMock(return_value=[])
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    # First collection — cache is cold, get_dialogs() must be called
    await collector.collect_all_channels()
    mock_client.get_dialogs.assert_awaited_once()

    # Second collection — cache is warm, get_dialogs() must NOT be called again
    mock_client.get_dialogs.reset_mock()
    await collector.collect_all_channels()
    mock_client.get_dialogs.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_falls_back_to_username_on_cache_miss(db):
    """When cache-only lookups fail, fall back to live username InputPeer."""
    ch = Channel(channel_id=1970788983, title="Test", username="agipdoom")
    await db.add_channel(ch)

    input_peer = InputPeerChannel(channel_id=1970788983, access_hash=123)
    mock_client = FakeTelethonClient(
        input_entity_resolver=lambda arg: input_peer if arg == "agipdoom" else ValueError("cache miss"),
        cached_input_entity_resolver=lambda _arg: ValueError("cache miss"),
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel(ch)

    mock_client.get_input_entity.assert_awaited_once_with("agipdoom")
    mock_client.get_entity.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_uses_cached_username_only_when_channel_id_matches(db):
    ch = Channel(channel_id=1970788983, title="Test", username="agipdoom")
    await db.add_channel(ch)

    wrong_cached_peer = InputPeerChannel(channel_id=111, access_hash=1)
    live_peer = InputPeerChannel(channel_id=1970788983, access_hash=2)

    def _cached(arg):
        if isinstance(arg, PeerChannel):
            raise ValueError("numeric cache miss")
        if arg == "agipdoom":
            return wrong_cached_peer
        raise ValueError("cache miss")

    mock_client = FakeTelethonClient(
        input_entity_resolver=lambda arg: live_peer if arg == "agipdoom" else ValueError("live miss"),
        cached_input_entity_resolver=_cached,
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel(ch)

    assert mock_client.session.get_input_entity.call_count == 2
    mock_client.get_input_entity.assert_awaited_once_with("agipdoom")


@pytest.mark.anyio
async def test_collect_channel_marks_username_changed_when_numeric_fallback_succeeds(db):
    ch = Channel(channel_id=1970788983, title="Old Title", username="old_name")
    channel_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(channel_id)
    assert stored is not None

    fallback_entity = SimpleNamespace(username="new_name", title="New Title")
    client = FakeTelethonClient(
        entity_resolver=lambda arg: (
            UsernameNotOccupiedError(request=None) if arg == "old_name" else fallback_entity
        ),
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    count = await collector._collect_channel(stored)

    assert count == 0
    updated = await db.get_channel_by_channel_id(stored.channel_id)
    assert updated is not None
    assert updated.username == "new_name"
    assert updated.title == "New Title"
    assert updated.is_filtered is True
    assert "username_changed" in updated.filter_flags


@pytest.mark.anyio
async def test_collect_channel_no_username_no_cache_skips_without_error(db):
    """Channel with no username and empty cache -> skipped/deactivated, 0 messages."""
    ch = Channel(channel_id=1970788983, title="Private Group")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=ValueError("Could not find the input entity"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()
    assert stats["errors"] == 0
    assert stats["messages"] == 0
    updated = await db.get_channel_by_channel_id(ch.channel_id)
    assert updated is not None
    assert updated.is_active is False


@pytest.mark.anyio
async def test_collect_private_group_uses_map_phone(db):
    """When channel→phone map has an entry, get_client_by_phone is used directly."""
    ch = Channel(channel_id=1877929309, title="Private Group")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(
        clients={"+7001": object()},
        get_client_by_phone=AsyncMock(return_value=(mock_client, "+7001")),
    )
    pool._channel_phone_map[1877929309] = "+7001"

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()

    assert stats["errors"] == 0
    pool.get_available_client.assert_not_awaited()
    pool.get_client_by_phone.assert_awaited_once_with("+7001")


@pytest.mark.anyio
async def test_collect_private_group_discovers_access_phone(db):
    """When channel not in map, _discover_phone_for_channel finds the right phone
    and registers it so the next iteration uses it directly."""
    ch = Channel(channel_id=1877929309, title="Private Group")
    await db.add_channel(ch)

    # Phone "+7000" doesn't have access; "+7001" does.
    mock_client_7000 = AsyncMock()
    mock_client_7000.get_entity = AsyncMock(
        side_effect=ValueError("Could not find the input entity")
    )
    mock_client_7001 = AsyncMock()
    mock_client_7001.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client_7001.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    call_count = 0

    async def get_client_by_phone_side_effect(phone):
        nonlocal call_count
        call_count += 1
        if phone == "+7001":
            return (mock_client_7001, "+7001")
        return None

    pool = make_mock_pool(
        clients={"+7000": object(), "+7001": object()},
        get_available_client=AsyncMock(return_value=(mock_client_7000, "+7000")),
        get_client_by_phone=AsyncMock(side_effect=get_client_by_phone_side_effect),
    )

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()

    assert stats["errors"] == 0
    # "+7001" must have been registered into the map
    assert pool._channel_phone_map.get(1877929309) == "+7001"


@pytest.mark.anyio
async def test_collect_private_group_without_resolvable_entity_deactivates_without_error(db):
    ch = Channel(channel_id=1877929309, title="Private Group")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_dialogs = AsyncMock(return_value=[])
    mock_client.get_entity = AsyncMock(side_effect=ValueError("Could not find the input entity"))

    pool = make_mock_pool(
        clients={"+7000": object()},
        get_available_client=AsyncMock(return_value=(mock_client, "+7000")),
    )

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()

    assert stats["errors"] == 0
    channel = await db.get_channel_by_channel_id(1877929309)
    assert channel is not None
    assert channel.is_active is False


@pytest.mark.anyio
async def test_collect_all_dialogs_timeout(db):
    """Hanging get_dialogs() must not block collection (30s timeout)."""
    ch = Channel(channel_id=123, title="Test", username="test")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_dialogs = AsyncMock(side_effect=asyncio.TimeoutError)
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()

    # Collection should complete despite dialogs timeout
    assert stats["channels"] == 1
    assert stats["messages"] == 0


def _make_mock_message(msg_id, text=None, media=None, sender_id=None):
    """Helper to create a mock Telethon message."""
    return make_mock_message(msg_id, text=text, media=media, sender_id=sender_id)


@pytest.mark.anyio
async def test_get_media_type_photo():
    from telethon.tl.types import MessageMediaPhoto

    msg = SimpleNamespace(media=MessageMediaPhoto())
    assert Collector._get_media_type(msg) == "photo"


@pytest.mark.anyio
async def test_get_media_type_none():
    msg = SimpleNamespace(media=None)
    assert Collector._get_media_type(msg) is None


@pytest.mark.anyio
async def test_get_media_type_document_video():
    from telethon.tl.types import DocumentAttributeVideo, MessageMediaDocument

    attr = DocumentAttributeVideo(duration=10, w=100, h=100, round_message=False)
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)

    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "video"


@pytest.mark.anyio
async def test_get_media_type_sticker():
    from telethon.tl.types import (
        DocumentAttributeSticker,
        InputStickerSetEmpty,
        MessageMediaDocument,
    )

    attr = DocumentAttributeSticker(alt="", stickerset=InputStickerSetEmpty())
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)

    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "sticker"


@pytest.mark.anyio
async def test_get_media_type_voice():
    from telethon.tl.types import DocumentAttributeAudio, MessageMediaDocument

    attr = DocumentAttributeAudio(duration=10, voice=True)
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)

    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "voice"


@pytest.mark.anyio
async def test_get_media_type_poll():
    from telethon.tl.types import MessageMediaPoll

    msg = SimpleNamespace(media=MessageMediaPoll(poll=None, results=None))
    assert Collector._get_media_type(msg) == "poll"


# --- _extract_reactions tests ---


@pytest.mark.anyio
async def test_extract_reactions_none():
    """No reactions attr → None."""
    msg = SimpleNamespace(reactions=None)
    assert Collector._extract_reactions(msg) is None


@pytest.mark.anyio
async def test_extract_reactions_empty_results():
    """reactions exists but results is empty → None."""
    msg = SimpleNamespace(reactions=SimpleNamespace(results=[]))
    assert Collector._extract_reactions(msg) is None


@pytest.mark.anyio
async def test_extract_reactions_single_emoji():
    import json

    msg = SimpleNamespace(reactions=make_mock_reactions([("👍", 5)]))
    result = Collector._extract_reactions(msg)
    assert result is not None
    parsed = json.loads(result)
    assert parsed == [{"emoji": "👍", "count": 5}]


@pytest.mark.anyio
async def test_extract_reactions_multiple():
    import json

    msg = SimpleNamespace(reactions=make_mock_reactions([("👍", 5), ("❤️", 3)]))
    result = Collector._extract_reactions(msg)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert parsed[0] == {"emoji": "👍", "count": 5}
    assert parsed[1] == {"emoji": "❤️", "count": 3}


@pytest.mark.anyio
async def test_extract_reactions_custom_emoji():
    import json

    msg = SimpleNamespace(reactions=make_mock_reactions([(12345678, 2)]))
    result = Collector._extract_reactions(msg)
    parsed = json.loads(result)
    assert parsed == [{"emoji": "custom:12345678", "count": 2}]


@pytest.mark.anyio
async def test_classify_message_service_joined_by_link():
    from telethon.tl.types import MessageActionChatJoinedByLink

    msg = SimpleNamespace(
        action=MessageActionChatJoinedByLink(inviter_id=42),
        sender_id=100,
        post=False,
        from_id=None,
    )
    assert Collector._get_message_kind(msg) == "service"
    assert Collector._get_service_action_raw(msg) == "MessageActionChatJoinedByLink"
    assert Collector._get_service_action_semantic(msg) == "join"
    assert Collector._get_sender_kind(msg) == "user"


@pytest.mark.anyio
async def test_collect_channel_collects_media_without_text(db):
    """Collector should collect messages without text (media-only)."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=5)
    await db.add_channel(ch)

    mock_messages = [
        _make_mock_message(10, text=None),  # media without text
        _make_mock_message(11, text="hello"),
    ]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_messages))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    count = await collector._collect_channel(ch)

    assert count == 2  # Both messages collected


@pytest.mark.anyio
async def test_backfill_uses_no_limit(db):
    """First run (last_collected_id==0) should use limit=None."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=0)
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    await collector._collect_channel(ch)

    # Verify limit=None was passed (backfill)
    call_kwargs = mock_client.iter_messages.call_args
    assert call_kwargs[1].get("limit") is None or call_kwargs.kwargs.get("limit") is None


@pytest.mark.anyio
async def test_incremental_uses_no_limit(db):
    """Subsequent runs (last_collected_id>0) should use limit=None (all new messages)."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=50)
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    await collector._collect_channel(ch)

    call_kwargs = mock_client.iter_messages.call_args
    assert call_kwargs[1].get("limit") is None or call_kwargs.kwargs.get("limit") is None


@pytest.mark.anyio
async def test_backfill_batch_flush(db):
    """During backfill, messages should be flushed in batches of 500."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=0)
    await db.add_channel(ch)

    # Create 600 mock messages to trigger at least one flush
    mock_msgs = [_make_mock_message(i, text=f"msg {i}") for i in range(1, 601)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    # side_effect returns a fresh iterator per call: first for precheck, then for main loop
    mock_client.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: _AsyncIterMessages(mock_msgs)
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    count = await collector._collect_channel(ch)

    assert count == 600

    # Verify messages are in DB
    messages, total = await db.search_messages(limit=700)
    assert total == 600


@pytest.mark.anyio
async def test_progress_callback_invoked_on_batch_flush(db):
    """progress_callback is called after each batch flush and final flush."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=0)
    await db.add_channel(ch)

    # 600 msgs → flush at 500 (cb=500), remaining 100 in finally (cb=600)
    mock_msgs = [_make_mock_message(i, text=f"msg {i}") for i in range(1, 601)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    # side_effect returns a fresh iterator per call: first for precheck, then for main loop
    mock_client.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: _AsyncIterMessages(mock_msgs)
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    progress_cb = AsyncMock()

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    count = await collector._collect_channel(ch, progress_callback=progress_cb)

    assert count == 600
    assert progress_cb.await_count == 2
    progress_cb.assert_any_await(500)
    progress_cb.assert_any_await(600)


@pytest.mark.anyio
async def test_persist_progress_log_includes_channel_username(db, caplog):
    ch = Channel(channel_id=-100133, title="Named Log", username="named_log", last_collected_id=0)
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages([_make_mock_message(1, text="msg 1")]))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    caplog.set_level("INFO", logger="src.telegram.collector")
    count = await collector._collect_channel(stored, force=True)

    assert count == 1
    assert "Channel -100133 (@named_log): persisted 1 messages, total 1" in caplog.text


@pytest.mark.anyio
async def test_collect_channel_hanging_stream_times_out_and_releases_client(db):
    ch = Channel(channel_id=-100134, title="Hanging Stream")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    async def _hanging_stream(*_args, **_kwargs):
        await asyncio.Event().wait()
        yield  # pragma: no cover - keeps this as an async generator

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_hanging_stream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0.02)
    collector = Collector(pool, db, config)

    count = await asyncio.wait_for(
        collector._collect_channel(stored, force=True),
        timeout=0.2,
    )

    assert count == 0
    pool.release_client.assert_awaited_with("+7000")
    updated = await db.get_channel_by_pk(ch_id)
    assert updated is not None
    assert updated.last_collected_id == 0


@pytest.mark.anyio
async def test_collect_channel_hanging_stream_close_times_out_and_releases_client(db, monkeypatch):
    ch = Channel(channel_id=-100137, title="Hanging Close")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    class HangingCloseStream:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()
            raise StopAsyncIteration

        async def aclose(self):
            await asyncio.Event().wait()

    monkeypatch.setattr("src.telegram.collector.STREAM_CLEANUP_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr("src.telegram.backends.STREAM_ITERATOR_CLOSE_TIMEOUT_SEC", 0.01)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=HangingCloseStream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7003")))
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0.01)
    collector = Collector(pool, db, config)

    count = await asyncio.wait_for(
        collector._collect_channel(stored, force=True),
        timeout=0.2,
    )

    assert count == 0
    pool.release_client.assert_awaited_with("+7003")


@pytest.mark.anyio
async def test_collect_channel_abandoned_stream_read_retires_client(db, monkeypatch):
    ch = Channel(channel_id=-100139, title="Dirty Stream Client")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    class SlowCloseStream:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()
            raise StopAsyncIteration

        async def aclose(self):
            await asyncio.Event().wait()

    monkeypatch.setattr("src.telegram.collector.STREAM_CLEANUP_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr("src.telegram.backends.STREAM_ITERATOR_CLOSE_TIMEOUT_SEC", 0.05)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=SlowCloseStream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7005")))
    pool.remove_client = AsyncMock()
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0.01)
    collector = Collector(pool, db, config)

    count = await asyncio.wait_for(
        collector._collect_channel(stored, force=True),
        timeout=0.3,
    )
    await asyncio.sleep(0.06)

    assert count == 0
    pool.remove_client.assert_awaited_with("+7005")
    pool.release_client.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_dirty_client_remove_timeout_releases_lease(db, monkeypatch):
    ch = Channel(channel_id=-100140, title="Dirty Remove Timeout")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    class SlowCloseStream:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()
            raise StopAsyncIteration

        async def aclose(self):
            await asyncio.Event().wait()

    async def _hung_remove(_phone):
        await asyncio.Event().wait()

    monkeypatch.setattr("src.telegram.collector.STREAM_CLEANUP_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr("src.telegram.backends.STREAM_ITERATOR_CLOSE_TIMEOUT_SEC", 0.05)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=SlowCloseStream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7006")))
    pool.remove_client = AsyncMock(side_effect=_hung_remove)
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0.01)
    collector = Collector(pool, db, config)

    count = await asyncio.wait_for(
        collector._collect_channel(stored, force=True),
        timeout=0.3,
    )
    await asyncio.sleep(0.06)

    assert count == 0
    pool.remove_client.assert_awaited_with("+7006")
    pool.release_client.assert_awaited_with("+7006")


@pytest.mark.anyio
async def test_collect_channel_cancels_pending_stream_read_on_shutdown(db):
    ch = Channel(channel_id=-100138, title="Cancelled Stream")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    class CancellableStream:
        def __init__(self):
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()

        def __aiter__(self):
            return self

        async def __anext__(self):
            self.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()

    stream = CancellableStream()
    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=stream)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7004")))
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=30)
    collector = Collector(pool, db, config)

    task = asyncio.create_task(collector._collect_channel(stored, force=True))
    await asyncio.wait_for(stream.started.wait(), timeout=0.2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.wait_for(stream.cancelled.wait(), timeout=0.2)
    pool.release_client.assert_awaited_with("+7004")


@pytest.mark.anyio
async def test_collect_channel_slow_but_alive_stream_not_aborted(db):
    """A healthy channel that streams post-by-post must NOT be aborted.

    The idle-timeout caps the wait for the *next* post, not the whole channel.
    Here each post arrives faster than the idle limit, but the channel as a
    whole takes longer than the limit — it must still collect every message.
    Regression guard: the old per-channel timeout killed large live channels.
    """
    ch = Channel(channel_id=-100135, title="Slow But Alive")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    # 5 posts, ~0.02s apart → ~0.1s total, well past the 0.05s idle limit,
    # but no single gap exceeds it.
    async def _slow_stream(*_args, **_kwargs):
        for i in range(1, 6):
            await asyncio.sleep(0.02)
            yield _make_mock_message(i, text=f"msg {i}")

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_slow_stream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7001")))
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0.05)
    collector = Collector(pool, db, config)

    count = await asyncio.wait_for(
        collector._collect_channel(stored, force=True),
        timeout=2.0,
    )

    assert count == 5
    pool.release_client.assert_awaited_with("+7001")


@pytest.mark.anyio
async def test_collect_channel_zero_timeout_disables_abort(db):
    """`collection_stream_timeout_sec=0` disables the idle abort entirely.

    With the timeout off, even a slow stream collects fully and nothing is cut.
    """
    ch = Channel(channel_id=-100136, title="Timeout Disabled")
    ch_id = await db.add_channel(ch)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    async def _slow_stream(*_args, **_kwargs):
        for i in range(1, 4):
            await asyncio.sleep(0.01)
            yield _make_mock_message(i, text=f"msg {i}")

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_slow_stream())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7002")))
    config = SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0)
    collector = Collector(pool, db, config)

    count = await asyncio.wait_for(
        collector._collect_channel(stored, force=True),
        timeout=2.0,
    )

    assert count == 3
    pool.release_client.assert_awaited_with("+7002")


@pytest.mark.anyio
async def test_incremental_batch_flushes_to_avoid_huge_final_flush(db):
    """Incremental collection should flush large channels before the final flush."""
    ch = Channel(channel_id=-100130, title="Test", username="test130", last_collected_id=50)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 50)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    mock_msgs = [_make_mock_message(i, text=f"msg {i}") for i in range(51, 1251)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    original_insert = db.insert_messages_batch
    batch_sizes: list[int] = []

    async def insert_spy(batch):
        batch_sizes.append(len(batch))
        return await original_insert(batch)

    db.insert_messages_batch = AsyncMock(side_effect=insert_spy)  # type: ignore[method-assign]

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    count = await collector._collect_channel(stored)

    updated = await db.get_channel_by_pk(ch_id)
    assert updated is not None
    assert count == 1200
    assert batch_sizes == [500, 500, 200]
    assert updated.last_collected_id == 1250


@pytest.mark.anyio
async def test_flush_verifies_persisted_ids_in_chunks(db, monkeypatch):
    """Persist verification should chunk SQL IN parameters under SQLite's limit."""
    monkeypatch.setattr("src.telegram.collector.MESSAGE_FLUSH_BATCH_SIZE", 1001)
    monkeypatch.setattr("src.telegram.collector.PERSISTED_ID_VERIFY_CHUNK_SIZE", 400)

    ch = Channel(channel_id=-100131, title="Test", username="test131", last_collected_id=50)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 50)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    mock_msgs = [_make_mock_message(i, text=f"msg {i}") for i in range(51, 1052)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    original_execute = db.execute
    verify_param_lengths: list[int] = []

    async def execute_spy(sql, params=()):
        if "message_id IN" in sql:
            verify_param_lengths.append(len(params) - 1)
        return await original_execute(sql, params)

    db.execute = AsyncMock(side_effect=execute_spy)  # type: ignore[method-assign]

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    count = await collector._collect_channel(stored)

    updated = await db.get_channel_by_pk(ch_id)
    assert updated is not None
    assert count == 1001
    assert verify_param_lengths == [400, 400, 201]
    assert max(verify_param_lengths) <= 400
    assert updated.last_collected_id == 1051


@pytest.mark.anyio
async def test_collect_channel_does_not_advance_last_id_when_flush_fails(db):
    ch = Channel(channel_id=-100126, title="Test", username="test", last_collected_id=5)
    await db.add_channel(ch)
    await db.update_channel_last_id(-100126, 5)
    stored = next(c for c in await db.get_channels() if c.channel_id == -100126)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(
        return_value=_AsyncIterMessages([_make_mock_message(10, text="msg 10")])
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    db.insert_messages_batch = AsyncMock(return_value=0)  # type: ignore[method-assign]

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    count = await collector._collect_channel(stored)

    updated = next(c for c in await db.get_channels() if c.channel_id == -100126)
    assert count == 0
    assert updated.last_collected_id == 5


@pytest.mark.anyio
async def test_full_backfill_does_not_rewind_existing_cursor(db):
    """A full old-history scan must not lower the durable incremental cursor."""
    ch = Channel(channel_id=-100132, title="Backfill", username="backfill", last_collected_id=500)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 500)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    mock_msgs = [_make_mock_message(i, text=f"old msg {i}") for i in (10, 20)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    count = await collector.collect_single_channel(stored, full=True, force=True)

    updated = await db.get_channel_by_pk(ch_id)
    assert updated is not None
    assert count == 2
    assert updated.last_collected_id == 500
    assert mock_client.iter_messages.call_args.kwargs["min_id"] == 0


@pytest.mark.anyio
async def test_backfill_does_not_send_notification_queries(db):
    from src.models import SearchQuery

    ch = Channel(channel_id=-100128, title="Test", username="test128", last_collected_id=0)
    await db.add_channel(ch)
    repo = db.repos.search_queries
    await repo.add(SearchQuery(query="urgent", notify_on_collect=True))

    mock_msgs = [_make_mock_message(i, text=f"urgent msg {i}") for i in range(1, 3)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: _AsyncIterMessages(mock_msgs)
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    notifier = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0), notifier)
    count = await collector._collect_channel(ch)

    assert count == 2
    notifier.notify.assert_not_awaited()


@pytest.mark.anyio
async def test_incremental_collection_sends_notification_queries(db):
    from src.models import SearchQuery

    ch = Channel(channel_id=-100129, title="Test", username="test129", last_collected_id=10)
    await db.add_channel(ch)
    repo = db.repos.search_queries
    await repo.add(SearchQuery(query="urgent", notify_on_collect=True))

    mock_msgs = [_make_mock_message(11, text="urgent update")]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    notifier = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0), notifier)
    count = await collector._collect_channel(ch)

    assert count == 1
    notifier.notify.assert_awaited_once()


@pytest.mark.anyio
async def test_incremental_collection_sends_notifications_before_idle_timeout_return(db):
    from src.models import SearchQuery

    ch = Channel(channel_id=-100141, title="Test", username="test141", last_collected_id=10)
    await db.add_channel(ch)
    repo = db.repos.search_queries
    await repo.add(SearchQuery(query="urgent", notify_on_collect=True))

    class OneThenHangStream:
        def __init__(self, msg):
            self.msg = msg
            self.sent = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.sent:
                self.sent = True
                return self.msg
            await asyncio.Event().wait()
            raise StopAsyncIteration

    mock_msg = _make_mock_message(11, text="urgent update")
    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=OneThenHangStream(mock_msg))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    notifier = AsyncMock()

    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, collection_stream_timeout_sec=0.01),
        notifier,
    )
    count = await asyncio.wait_for(collector._collect_channel(ch), timeout=0.3)

    assert count == 1
    notifier.notify.assert_awaited_once()


@pytest.mark.anyio
async def test_first_run_detects_topics_without_retaining_messages(db):
    """First-run forum-topic detection runs via the saw_topic_message flag (#633).

    On first-run we no longer accumulate every Message object in memory, so topic
    detection must rely on the running flag rather than scanning a retained list.
    """
    ch = Channel(channel_id=-100199, title="Forum", username="forum199", last_collected_id=0)
    await db.add_channel(ch)

    topic_msg = make_mock_message(1, text="topic msg")
    topic_msg.reply_to = SimpleNamespace(forum_topic=True, reply_to_top_id=42, reply_to_msg_id=None)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(side_effect=lambda *a, **kw: _AsyncIterMessages([topic_msg]))

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=(mock_client, "+7000")),
        get_forum_topics=AsyncMock(return_value=[]),
    )

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    count = await collector._collect_channel(ch)

    assert count == 1
    pool.get_forum_topics.assert_awaited_once_with(-100199)


@pytest.mark.anyio
async def test_first_run_skips_topic_lookup_without_topic_messages(db):
    """No topic messages → no forum-topic lookup (saw_topic_message stays False) (#633)."""
    ch = Channel(channel_id=-100200, title="Plain", username="plain200", last_collected_id=0)
    await db.add_channel(ch)

    mock_msgs = [make_mock_message(i, text=f"msg {i}") for i in range(1, 4)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(side_effect=lambda *a, **kw: _AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=(mock_client, "+7000")),
        get_forum_topics=AsyncMock(return_value=[]),
    )

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    count = await collector._collect_channel(ch)

    assert count == 3
    pool.get_forum_topics.assert_not_awaited()


@pytest.mark.anyio
async def test_collect_channel_retries_after_short_flood_wait(db):
    ch = Channel(channel_id=-100171, title="Retry", username="retry", last_collected_id=5)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 5)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
    )

    call_index = {"idx": 0}

    def _iter_messages_factory(*_args, **_kwargs):
        idx = call_index["idx"]
        call_index["idx"] += 1
        if idx == 0:

            async def _generator():
                yield _make_mock_message(6, text="msg 6")
                raise FloodWaitError(request=None, capture=3)

            return _generator()
        return _AsyncIterMessages([_make_mock_message(7, text="msg 7")])

    client.iter_messages = MagicMock(side_effect=_iter_messages_factory)
    count = await collector._collect_channel(stored)

    assert count == 2
    updated = await db.get_channel_by_channel_id(stored.channel_id)
    assert updated is not None
    assert updated.last_collected_id == 7
    pool.report_flood.assert_awaited_once()


@pytest.mark.anyio
async def test_collect_channel_waits_when_all_clients_transient_flooded(db, monkeypatch):
    ch = Channel(channel_id=-100174, title="TransientAllFlood", username="transientall", last_collected_id=5)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 5)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("src.telegram.flood_wait.asyncio.sleep", fake_sleep)

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    async def _flooding_generator():
        yield _make_mock_message(6, text="msg 6")
        raise FloodWaitError(request=None, capture=3)

    client1.iter_messages = MagicMock(return_value=_flooding_generator())
    client2.iter_messages = MagicMock(return_value=_AsyncIterMessages([_make_mock_message(7, text="msg 7")]))

    pool = make_mock_pool(
        get_available_client=AsyncMock(
            side_effect=[
                (client1, "+70001"),
                None,
                (client2, "+70002"),
            ]
        ),
        get_stats_availability=AsyncMock(
            return_value=SimpleNamespace(
                state="all_flooded",
                retry_after_sec=3,
                next_available_at_utc=datetime.now(timezone.utc) + timedelta(seconds=3),
            )
        ),
    )
    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
    )

    count = await collector._collect_channel(stored)

    assert count == 2
    assert sleeps == [4.0]
    pool.report_flood.assert_awaited_once()


@pytest.mark.anyio
async def test_collect_channel_rotates_on_long_flood_wait(db):
    """FloodWait > max_flood_wait_sec still rotates to the next available account."""
    ch = Channel(channel_id=-100172, title="LongFlood", username="longflood", last_collected_id=5)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 5)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    call_index = {"idx": 0}

    def _iter_messages_factory(*_args, **_kwargs):
        idx = call_index["idx"]
        call_index["idx"] += 1
        if idx == 0:

            async def _generator():
                yield _make_mock_message(6, text="msg 6")
                raise FloodWaitError(request=None, capture=600)  # > max_flood_wait_sec=10

            return _generator()
        return _AsyncIterMessages([_make_mock_message(7, text="msg 7")])

    client1.iter_messages = MagicMock(side_effect=_iter_messages_factory)
    client2.iter_messages = MagicMock(side_effect=_iter_messages_factory)

    pool = make_mock_pool(
        get_available_client=AsyncMock(
            side_effect=[
                (client1, "+70001"),  # first iteration
                (client2, "+70002"),  # retry after flood
            ]
        )
    )
    notifier = AsyncMock()
    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
        notifier,
    )
    count = await collector._collect_channel(stored)

    assert count == 2
    updated = await db.get_channel_by_channel_id(stored.channel_id)
    assert updated is not None
    assert updated.last_collected_id == 7
    pool.report_flood.assert_awaited_once()
    # Notification should mention rotation, not "skipped"
    notifier.notify.assert_awaited_once()
    assert "rotating" in notifier.notify.call_args[0][0]


@pytest.mark.anyio
async def test_collect_channel_long_flood_all_accounts_flooded_returns(db):
    """FloodWait > max_flood_wait_sec with no other account available surfaces unavailability."""
    ch = Channel(channel_id=-100173, title="AllFlood", username="allflood", last_collected_id=5)
    ch_id = await db.add_channel(ch)
    await db.update_channel_last_id(ch.channel_id, 5)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    call_index = {"idx": 0}

    def _iter_messages_factory(*_args, **_kwargs):
        idx = call_index["idx"]
        call_index["idx"] += 1
        if idx == 0:

            async def _generator():
                yield _make_mock_message(6, text="msg 6")
                raise FloodWaitError(request=None, capture=600)  # > max_flood_wait_sec=10

            return _generator()
        return _AsyncIterMessages([])

    client1.iter_messages = MagicMock(side_effect=_iter_messages_factory)

    pool = make_mock_pool(
        get_available_client=AsyncMock(
            side_effect=[
                (client1, "+70001"),  # first iteration
                None,  # all accounts flooded on retry
            ]
        ),
        get_stats_availability=AsyncMock(
            return_value=SimpleNamespace(
                state="all_flooded",
                retry_after_sec=600,
                next_available_at_utc=datetime.now(timezone.utc) + timedelta(minutes=10),
            )
        ),
    )
    collector = Collector(
        pool,
        db,
        SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10),
    )
    with pytest.raises(AllCollectionClientsFloodedError):
        await collector._collect_channel(stored)

    updated = await db.get_channel_by_channel_id(stored.channel_id)
    assert updated is not None
    assert updated.last_collected_id == 6
    pool.report_flood.assert_awaited_once()


@pytest.mark.anyio
async def test_collection_queue_skips_filtered_channel(db):
    """CollectionQueue worker skips channels that become filtered after enqueue."""
    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100140, title="Will Be Filtered")
    await db.add_channel(ch)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    queue = CollectionQueue(collector, db)

    # Mark channel as filtered after adding
    await db.set_channels_filtered_bulk([(-100140, "low_uniqueness")])

    # Get the stored channel with its PK
    channels = await db.get_channels(include_filtered=True)
    stored_ch = next(c for c in channels if c.channel_id == -100140)

    task_id = await queue.enqueue(stored_ch)

    # Wait for worker to process
    await asyncio.sleep(0.5)

    task = await db.get_collection_task(task_id)
    assert task.status == "cancelled"

    await queue.shutdown()


@pytest.mark.anyio
async def test_connection_error_triggers_reconnect_and_requeue(db):
    """ConnectionError during collection triggers reconnect and re-enqueues the task."""
    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100170, title="Reconnect Test")
    await db.add_channel(ch)
    channels = await db.get_channels()
    stored_ch = next(c for c in channels if c.channel_id == -100170)

    call_count = 0

    async def _collect_side_effect(channel, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Cannot send requests while disconnected")
        return 5

    pool = make_mock_pool()
    pool.reconnect_phone = AsyncMock(return_value=True)
    pool.clients = {"+1234": MagicMock()}

    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(side_effect=_collect_side_effect)

    queue = CollectionQueue(collector, db)
    task_id = await queue.enqueue(stored_ch)
    await asyncio.sleep(1.0)

    task = await db.get_collection_task(task_id)
    assert task.status == "completed"
    assert call_count == 2
    pool.reconnect_phone.assert_awaited_once_with("+1234")

    await queue.shutdown()


@pytest.mark.anyio
async def test_connection_error_no_retry_after_max(db):
    """Second ConnectionError after retry marks task as FAILED."""
    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100171, title="No Retry Test")
    await db.add_channel(ch)
    channels = await db.get_channels()
    stored_ch = next(c for c in channels if c.channel_id == -100171)

    pool = make_mock_pool()
    pool.reconnect_phone = AsyncMock(return_value=True)
    pool.clients = {"+1234": MagicMock()}

    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(
        side_effect=ConnectionError("Cannot send requests while disconnected")
    )

    queue = CollectionQueue(collector, db)
    task_id = await queue.enqueue(stored_ch)
    await asyncio.sleep(1.0)

    task = await db.get_collection_task(task_id)
    assert task.status == "failed"

    await queue.shutdown()


@pytest.mark.anyio
async def test_connection_error_reconnect_fails(db):
    """When reconnect fails, task is marked as FAILED immediately."""
    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100172, title="Reconnect Fail Test")
    await db.add_channel(ch)
    channels = await db.get_channels()
    stored_ch = next(c for c in channels if c.channel_id == -100172)

    pool = make_mock_pool()
    pool.reconnect_phone = AsyncMock(return_value=False)
    pool.clients = {"+1234": MagicMock()}

    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(
        side_effect=ConnectionError("Cannot send requests while disconnected")
    )

    queue = CollectionQueue(collector, db)
    task_id = await queue.enqueue(stored_ch)
    await asyncio.sleep(0.5)

    task = await db.get_collection_task(task_id)
    assert task.status == "failed"

    await queue.shutdown()


@pytest.mark.anyio
async def test_enqueue_all_channels_uses_incremental_queue_tasks(db):
    from src.collection_queue import CollectionQueue
    from src.services.collection_service import CollectionService

    await db.add_channel(
        Channel(
            channel_id=-100161,
            title="Incremental Channel",
            username="incremental_ch",
            last_collected_id=42,
        )
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(return_value=3)
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    service = CollectionService(db, collector, queue)
    result = await service.enqueue_all_channels()

    assert result.queued_count == 1

    task = (await db.get_collection_tasks(limit=1))[0]
    assert task.payload == {"force": True, "full": False}

    await queue._run_worker()

    collector.collect_single_channel.assert_awaited_once()
    _, kwargs = collector.collect_single_channel.await_args
    assert kwargs["force"] is True
    assert kwargs["full"] is False
    assert kwargs["progress_callback"] is not None

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_force_tasks_default_to_incremental(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(Channel(channel_id=-100162, title="Manual Channel", username="manual_ch"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(return_value=1)
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    stored_ch = next(c for c in await db.get_channels() if c.channel_id == -100162)
    task_id = await queue.enqueue(stored_ch, force=True)

    task = await db.get_collection_task(task_id)
    assert task is not None
    assert task.payload == {"full": False, "force": True}

    await queue._run_worker()

    collector.collect_single_channel.assert_awaited_once()
    _, kwargs = collector.collect_single_channel.await_args
    assert kwargs["force"] is True
    assert kwargs["full"] is False

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_explicit_full_tasks_keep_full_collection(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(Channel(channel_id=-100262, title="Full Channel", username="full_ch"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(return_value=1)
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    stored_ch = next(c for c in await db.get_channels() if c.channel_id == -100262)
    task_id = await queue.enqueue(stored_ch, force=True, full=True)

    task = await db.get_collection_task(task_id)
    assert task is not None
    assert task.payload == {"full": True, "force": True}

    await queue._run_worker()

    collector.collect_single_channel.assert_awaited_once()
    _, kwargs = collector.collect_single_channel.await_args
    assert kwargs["force"] is True
    assert kwargs["full"] is True

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_reschedules_when_all_clients_flooded(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(Channel(channel_id=-100163, title="Flood Wait", username="flood_wait_ch"))

    collector = Collector(make_mock_pool(), db, SchedulerConfig())
    next_available_at = datetime.now(timezone.utc)
    collector.collect_single_channel = AsyncMock(
        side_effect=AllCollectionClientsFloodedError(
            retry_after_sec=180,
            next_available_at=next_available_at,
        )
    )
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    stored_ch = next(c for c in await db.get_channels() if c.channel_id == -100163)
    task_id = await queue.enqueue(stored_ch, force=True)

    await queue._run_worker()

    task = await db.get_collection_task(task_id)
    assert task is not None
    assert task.status == CollectionTaskStatus.PENDING
    assert task.run_after is not None
    assert task.note is not None
    assert "Flood Wait" in task.note

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_fails_when_no_active_clients(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(Channel(channel_id=-100164, title="No Clients", username="no_clients_ch"))

    collector = Collector(make_mock_pool(), db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(
        side_effect=NoActiveCollectionClientsError("No active connected clients")
    )
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    stored_ch = next(c for c in await db.get_channels() if c.channel_id == -100164)
    task_id = await queue.enqueue(stored_ch, force=True)

    await queue._run_worker()

    task = await db.get_collection_task(task_id)
    assert task is not None
    assert task.status == CollectionTaskStatus.PENDING
    assert task.error is None
    assert task.note == "Отложено: нет подключённых активных аккаунтов для сбора."
    assert task.run_after is not None

    await queue.shutdown()


@pytest.mark.anyio
async def test_requeue_startup_tasks(db):
    """requeue_startup_tasks re-enqueues pending tasks that survived a restart."""
    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100160, title="Pending Channel", username="pending_ch")
    await db.add_channel(ch)

    # Simulate a task that was created before restart (pending, never processed)
    task_id = await db.create_collection_task(
        -100160, "Pending Channel", channel_username="pending_ch"
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    queue = CollectionQueue(collector, db)

    count = await queue.requeue_startup_tasks()
    assert count == 1

    # Wait for worker to process (will fail — no client, but status transitions)
    await asyncio.sleep(0.5)

    task = await db.get_collection_task(task_id)
    # Task was picked up but deferred because no client is currently connected.
    assert task.status == CollectionTaskStatus.PENDING
    assert task.note == "Отложено: нет подключённых активных аккаунтов для сбора."
    assert task.run_after is not None

    await queue.shutdown()


@pytest.mark.anyio
async def test_requeue_startup_tasks_preserves_incremental_flag(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(
        Channel(
            channel_id=-100163,
            title="Pending Incremental",
            username="pending_incremental",
            last_collected_id=15,
        )
    )
    task_id = await db.create_collection_task(
        -100163,
        "Pending Incremental",
        channel_username="pending_incremental",
        payload={"force": True, "full": False},
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(return_value=2)
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    count = await queue.requeue_startup_tasks()

    assert count == 1

    await queue._run_worker()

    collector.collect_single_channel.assert_awaited_once()
    _, kwargs = collector.collect_single_channel.await_args
    assert kwargs["force"] is True
    assert kwargs["full"] is False

    task = await db.get_collection_task(task_id)
    assert task is not None
    assert task.status == "completed"

    await queue.shutdown()


@pytest.mark.anyio
async def test_requeue_startup_tasks_cancels_orphaned(db):
    """requeue_startup_tasks cancels tasks whose channel was deleted."""
    from src.collection_queue import CollectionQueue

    # Create a task for a channel that doesn't exist in the channels table
    task_id = await db.create_collection_task(-100999, "Ghost Channel")

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    queue = CollectionQueue(collector, db)

    count = await queue.requeue_startup_tasks()
    assert count == 0

    task = await db.get_collection_task(task_id)
    assert task.status == "cancelled"


@pytest.mark.anyio
async def test_requeue_startup_tasks_defers_future_run_after(db):
    """Startup tasks with run_after in the future go to _delayed_requeues, not the main queue."""
    from datetime import datetime, timedelta, timezone

    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100170, title="Future Task", username="future_ch")
    await db.add_channel(ch)

    task_id = await db.create_collection_task(
        -100170, "Future Task", channel_username="future_ch"
    )
    run_after = datetime.now(tz=timezone.utc) + timedelta(minutes=5)
    await db.repos.tasks.reschedule_collection_task(task_id, run_after=run_after)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    queue = CollectionQueue(collector, db)

    count = await queue.requeue_startup_tasks()
    assert count == 1
    assert queue._queue.empty(), "Future-dated task should not be in the main queue"
    assert len(queue._delayed_requeues) == 1, "Future-dated task should be in _delayed_requeues"

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_cancels_deleted_channel(db):
    from src.collection_queue import CollectionQueue

    ch = Channel(channel_id=-100141, title="Will Be Deleted")
    await db.add_channel(ch)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    stored_ch = next(c for c in await db.get_channels() if c.channel_id == -100141)
    task_id = await queue.enqueue(stored_ch)
    await db.delete_channel(stored_ch.id)

    await queue._run_worker()

    task = await db.get_collection_task(task_id)
    _messages, total = await db.search_messages(limit=10)
    assert task is not None
    assert task.status == "cancelled"
    assert task.note == "Канал удалён до начала сбора."
    assert total == 0

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_clear_pending_tasks_removes_db_rows_and_queue_items(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(Channel(channel_id=-100171, title="Queued One", username="queued_one"))
    await db.add_channel(Channel(channel_id=-100172, title="Queued Two", username="queued_two"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    channels = await db.get_channels()
    first = next(ch for ch in channels if ch.channel_id == -100171)
    second = next(ch for ch in channels if ch.channel_id == -100172)
    first_task_id = await queue.enqueue(first)
    second_task_id = await queue.enqueue(second, force=True)

    running_id = await db.create_collection_task(-100173, "Running")
    await db.update_collection_task(running_id, CollectionTaskStatus.RUNNING)
    await db.create_stats_task(StatsAllTaskPayload(channel_ids=[-100171]))

    deleted = await queue.clear_pending_tasks()

    assert deleted == 2
    assert queue._queue.empty()
    assert await db.get_collection_task(first_task_id) is None
    assert await db.get_collection_task(second_task_id) is None
    assert (await db.get_collection_task(running_id)).status == CollectionTaskStatus.RUNNING

    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_queue_skips_task_deleted_after_dequeue(db):
    from src.collection_queue import CollectionQueue

    await db.add_channel(
        Channel(channel_id=-100174, title="Deleted After Dequeue", username="deleted")
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())
    collector.collect_single_channel = AsyncMock(return_value=1)
    queue = CollectionQueue(collector, db)
    queue._ensure_worker = lambda: None

    stored = next(ch for ch in await db.get_channels() if ch.channel_id == -100174)
    task_id = await queue.enqueue(stored)
    await db.delete_pending_channel_tasks()

    await queue._run_worker()

    collector.collect_single_channel.assert_not_called()
    assert await db.get_collection_task(task_id) is None

    await queue.shutdown()


@pytest.mark.anyio
async def test_collect_all_stats_skips_filtered(db):
    """collect_all_stats should skip filtered channels."""
    await db.add_channel(Channel(channel_id=-100150, title="Filtered"))
    await db.add_channel(Channel(channel_id=-100151, title="Normal"))
    await db.set_channels_filtered_bulk([(-100150, "low_uniqueness")])

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    stats = await collector.collect_all_stats()
    # Only 1 channel (Normal), and it will error because no client
    assert stats["channels"] == 0
    assert stats["errors"] == 1


@pytest.mark.anyio
async def test_collect_all_stats_defers_when_resolve_rate_limited(db):
    from src.telegram.rate_limiter import ResolveRateLimiter

    await db.add_channel(Channel(channel_id=1970788993, title="Stats Resolve 1", username="stats_1"))
    await db.add_channel(Channel(channel_id=1970788994, title="Stats Resolve 2", username="stats_2"))

    raw_client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    pool = make_mock_pool()
    session = TelegramTransportSession(
        raw_client,
        disconnect_on_close=False,
        phone="+7001",
        pool=pool,
    )
    pool.get_available_client = AsyncMock(return_value=(session, "+7001"))
    pool._resolve_rate_limiter = ResolveRateLimiter(
        max_calls=1,
        window_sec=60.0,
        jitter_sec=0.0,
    )
    assert pool._resolve_rate_limiter.try_acquire("+7001") == 0.0

    collector = Collector(
        pool,
        db,
        SchedulerConfig(
            delay_between_channels_sec=0,
            delay_between_requests_sec=0,
            stats_all_worker_count=1,
        ),
    )

    stats = await collector.collect_all_stats(max_channels=2)

    assert stats["channels"] == 0
    assert stats["errors"] == 0
    assert stats["limited"] is True
    assert stats["remaining"] == 2
    assert 0 < stats["resolve_username_retry_after_sec"] <= 60
    raw_client.get_entity.assert_not_awaited()


# ---------------------------------------------------------------------------
# Pre-filter: subscriber_ratio tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_prefilter_broadcast_low_ratio(db):
    """Broadcast channel with ratio < 1.0 is filtered before iter_messages."""
    ch = Channel(
        channel_id=-100200,
        title="Spam Channel",
        channel_type="channel",
        last_collected_id=62000,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100200, subscriber_count=156))
    # Insert 62000 fake messages so COUNT(*) = 62000, ratio = 156/62000 ≈ 0.0025 < 1.0
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100200,
                message_id=i,
                text="x",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(1, 201)
        ]
    )
    # 156 / 200 = 0.78 < 1.0 → should be filtered

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # iter_messages must NOT be called (pre-filtered)
    mock_client.iter_messages.assert_not_called()

    # Channel must be marked as filtered
    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100200)
    assert stored.is_filtered is True
    assert "low_subscriber_ratio" in stored.filter_flags


@pytest.mark.anyio
async def test_prefilter_supergroup_low_ratio(db):
    """Supergroup with ratio < 0.02 is filtered before iter_messages."""
    ch = Channel(
        channel_id=-100201,
        title="Noisy Chat",
        channel_type="supergroup",
        last_collected_id=10000,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100201, subscriber_count=100))
    # Insert 10000 messages so COUNT(*) = 10000, ratio = 100/10000 = 0.01 < 0.02
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100201,
                message_id=i,
                text="x",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(1, 10001)
        ]
    )

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    mock_client.iter_messages.assert_not_called()

    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100201)
    assert stored.is_filtered is True


@pytest.mark.anyio
async def test_prefilter_supergroup_pass_ratio(db):
    """Supergroup with ratio >= 0.02 continues collection."""
    ch = Channel(
        channel_id=-100202,
        title="Good Chat",
        channel_type="supergroup",
        last_collected_id=1000,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100202, subscriber_count=50))
    # Insert 1000 messages so COUNT(*) = 1000, ratio = 50/1000 = 0.05 >= 0.02
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100202,
                message_id=i,
                text="x",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(1, 1001)
        ]
    )

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    # Collection continues (0 messages, but iter_messages for collection was called)
    assert count == 0
    mock_client.iter_messages.assert_called_once()

    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100202)
    assert stored.is_filtered is False


@pytest.mark.anyio
async def test_prefilter_no_stats_skips_check(db):
    """No stats (subscriber_count=None) → collection continues without filtering."""
    ch = Channel(
        channel_id=-100203,
        title="Unknown Channel",
        channel_type="channel",
        last_collected_id=5000,
    )
    await db.add_channel(ch)
    # No stats saved

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # Collection iter_messages was called (not pre-filtered)
    mock_client.iter_messages.assert_called_once()

    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100203)
    assert stored.is_filtered is False


@pytest.mark.anyio
async def test_prefilter_uses_message_count(db):
    """Pre-filter uses real COUNT(*) from DB, not last_collected_id."""
    ch = Channel(
        channel_id=-100204,
        title="Established Channel",
        channel_type="supergroup",
        last_collected_id=500,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100204, subscriber_count=5))
    # Insert 500 messages so COUNT(*) = 500, ratio = 5/500 = 0.01 < 0.02 → filtered
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100204,
                message_id=i,
                text="x",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(1, 501)
        ]
    )

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # iter_messages must never be called (pre-filtered by message_count)
    mock_client.iter_messages.assert_not_called()


@pytest.mark.anyio
async def test_prefilter_skips_when_no_messages(db):
    """First run (message_count=0) → pre-filter skipped, collection proceeds."""
    ch = Channel(
        channel_id=-100205,
        title="New Channel",
        channel_type="supergroup",
        last_collected_id=0,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100205, subscriber_count=1))
    # No messages in DB → message_count = 0 → pre-filter skipped

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # iter_messages called twice: once for cross-dupe precheck, once for actual collection
    assert mock_client.iter_messages.call_count == 2


@pytest.mark.anyio
async def test_prefilter_skipped_when_force(db):
    """force=True → pre-filter skipped; channel filter state not changed."""
    ch = Channel(
        channel_id=-100206,
        title="Forced Channel",
        channel_type="supergroup",
        last_collected_id=500,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100206, subscriber_count=5))
    # 5 / 500 = 0.01 < 0.02 — would be filtered without force
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100206,
                message_id=i,
                text="x",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(1, 501)
        ]
    )

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch, force=True)

    assert count == 0
    # Collection proceeds (iter_messages called), channel NOT marked filtered
    mock_client.iter_messages.assert_called_once()

    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100206)
    assert stored.is_filtered is False


@pytest.mark.anyio
async def test_precheck_skipped_when_force_and_first_run(db):
    """force=True + first_run (last_collected_id=0) → precheck пропускается."""
    ch = Channel(
        channel_id=-100207,
        title="Force First Run",
        channel_type="supergroup",
        last_collected_id=0,
    )
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    await collector._collect_channel(ch, force=True)

    # Precheck пропущен при force=True — iter_messages вызывается только для основного сбора
    assert mock_client.iter_messages.call_count == 1


@pytest.mark.anyio
async def test_get_entity_timeout_returns_zero(db):
    """get_input_entity hanging → TimeoutError → _collect_channel returns 0."""
    ch = Channel(channel_id=-100400, title="Hanging Channel", username="hang_chan")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_input_entity = AsyncMock(side_effect=asyncio.TimeoutError)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)
    assert count == 0


@pytest.mark.anyio
async def test_precheck_timeout_skips_check(db):
    """Precheck hanging → TimeoutError → collection continues with 0 precheck sample."""
    ch = Channel(
        channel_id=-100401, title="Slow Precheck", username="slow_chan", last_collected_id=0
    )
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    # Patch _precheck_sample to simulate timeout
    collector._precheck_sample = AsyncMock(side_effect=asyncio.TimeoutError)

    count = await collector._collect_channel(ch)

    # Collection should continue despite precheck timeout
    assert count == 0
    # Main iter_messages (for actual collection) should still be called
    mock_client.iter_messages.assert_called_once()


@pytest.mark.anyio
async def test_post_collection_low_uniqueness_marks_filtered(db):
    """First run with 100 identical messages → channel marked is_filtered=True, messages kept."""
    ch = Channel(channel_id=-100300, title="Spam Channel", username="spam", last_collected_id=0)
    await db.add_channel(ch)

    # 100 messages with the same long text → uniqueness ratio = 1/100 = 1% < 30%
    spam_text = "КУПИ КРИПТУ СЕЙЧАС! Уникальное предложение только сегодня!"
    mock_msgs = [_make_mock_message(i, text=spam_text) for i in range(1, 101)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: _AsyncIterMessages(mock_msgs)
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)

    count = await collector._collect_channel(ch)

    assert count == 100

    # Channel must be marked as filtered with low_uniqueness
    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100300)
    assert stored.is_filtered is True
    assert "low_uniqueness" in stored.filter_flags

    # Messages must still be in DB (purge is a separate action)
    cur = await db.execute("SELECT COUNT(*) as cnt FROM messages WHERE channel_id = ?", (-100300,))
    row = await cur.fetchone()
    assert row["cnt"] == 100


# ---------------------------------------------------------------------------
# Username-changed handling
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_precheck_detects_cross_channel_spam(db):
    """Precheck marks a first-run channel as cross_channel_spam on 80%+ sample overlap."""
    # Existing channel with known messages in DB
    existing_ch = Channel(channel_id=-100500, title="Existing Source")
    await db.add_channel(existing_ch)
    spam_texts = [f"Спам-рассылка номер {i}, достаточно длинный текст для теста" for i in range(8)]
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100500,
                message_id=i + 1,
                text=spam_texts[i],
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(8)
        ]
    )

    # New channel (first run)
    ch = Channel(
        channel_id=-100501,
        title="New Spam Channel",
        username="new_spam",
        last_collected_id=0,
    )
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)

    # Precheck returns all 8 prefixes — all exist in channel -100500
    sample_prefixes = [t[:100] for t in spam_texts]
    collector._precheck_sample = AsyncMock(return_value=sample_prefixes)

    count = await collector._collect_channel(ch)

    assert count == 0
    # Main iter_messages must NOT be called (pre-filtered)
    mock_client.iter_messages.assert_not_called()

    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100501)
    assert stored.is_filtered is True
    assert "cross_channel_spam" in stored.filter_flags


@pytest.mark.anyio
async def test_username_changed_marks_filtered(db):
    """Username lookup fails, PeerChannel fallback succeeds → filtered with username_changed."""
    ch = Channel(channel_id=3645212410, title="Old Title", username="raketa_nanobanana4")
    await db.add_channel(ch)
    ch = (await db.get_channels())[0]

    fallback_entity = SimpleNamespace(username="new_username", title="New Title")

    async def _get_entity(arg):
        return fallback_entity

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=_get_entity)
    mock_client.get_input_entity = AsyncMock(
        side_effect=ValueError('No user has "raketa_nanobanana4" as username')
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    result = await collector._collect_channel(ch)

    assert result == 0
    stored = await db.get_channel_by_channel_id(3645212410)
    assert stored is not None
    assert stored.username == "new_username"
    assert stored.title == "New Title"
    assert stored.is_filtered is True
    assert "username_changed" in stored.filter_flags


@pytest.mark.anyio
async def test_username_not_found_deactivates(db):
    """Both username and PeerChannel lookups fail → channel deactivated, returns 0."""
    ch = Channel(channel_id=3645212410, title="Old Title", username="gone_username")
    await db.add_channel(ch)
    ch = (await db.get_channels())[0]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=ValueError("No user has username"))
    mock_client.get_input_entity = AsyncMock(side_effect=ValueError("No user has username"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    result = await collector._collect_channel(ch)

    assert result == 0
    stored = await db.get_channel_by_pk(ch.id)
    assert stored is not None
    assert stored.is_active is False
