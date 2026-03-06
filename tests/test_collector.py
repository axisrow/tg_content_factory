import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from telethon.tl.types import PeerChannel

from src.config import SchedulerConfig
from src.models import Channel, ChannelStats
from src.telegram.collector import Collector
from tests.helpers import AsyncIterEmpty as _AsyncIterEmpty
from tests.helpers import make_mock_pool


@pytest.mark.asyncio
async def test_collect_no_channels(db):
    pool = make_mock_pool()
    config = SchedulerConfig()
    collector = Collector(pool, db, config)
    stats = await collector.collect_all_channels()
    assert stats["channels"] == 0
    assert stats["messages"] == 0


@pytest.mark.asyncio
async def test_collect_no_clients(db):
    ch = Channel(channel_id=-100123, title="Test")
    await db.add_channel(ch)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))

    config = SchedulerConfig()
    collector = Collector(pool, db, config)
    stats = await collector.collect_all_channels()
    assert stats["channels"] == 1
    assert stats["messages"] == 0


@pytest.mark.asyncio
async def test_collect_all_skips_filtered_channels(db):
    await db.add_channel(Channel(channel_id=-100124, title="Filtered"))
    await db.add_channel(Channel(channel_id=-100125, title="Normal"))
    await db.set_channels_filtered_bulk([(-100124, "low_uniqueness")])

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    stats = await collector.collect_all_channels()
    assert stats["channels"] == 1
    assert stats["messages"] == 0


@pytest.mark.asyncio
async def test_collect_single_channel_skips_filtered(db):
    """collect_single_channel returns 0 immediately for filtered channels."""
    ch = Channel(
        channel_id=-100130, title="Filtered",
        is_filtered=True, filter_flags="non_cyrillic",
    )
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector.collect_single_channel(ch)
    assert count == 0
    # Pool should never be touched
    pool.get_available_client.assert_not_awaited()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_collect_channel_uses_username_when_available(db):
    """_collect_channel resolves by username when it is stored."""
    ch = Channel(channel_id=1970788983, title="Test Channel", username="test_chan")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel(ch)

    call_arg = mock_client.get_entity.call_args[0][0]
    assert call_arg == "test_chan"


@pytest.mark.asyncio
async def test_collect_positive_id_end_to_end(db):
    """End-to-end: collect_all_channels with username resolves by username."""
    ch = Channel(channel_id=1970788983, title="Positive ID Channel", username="my_chan")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_all_channels()

    assert stats["channels"] == 1
    call_arg = mock_client.get_entity.call_args[0][0]
    assert call_arg == "my_chan"


@pytest.mark.asyncio
async def test_collect_all_prefetches_dialogs(db):
    """collect_all_channels must call get_dialogs() to populate entity cache."""
    ch = Channel(channel_id=123, title="Test", username="test")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_dialogs = AsyncMock(return_value=[])
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector.collect_all_channels()

    mock_client.get_dialogs.assert_awaited_once()


@pytest.mark.asyncio
async def test_collect_channel_falls_back_to_username_on_cache_miss(db):
    """When PeerChannel fails (entity not cached), fall back to username."""
    ch = Channel(channel_id=1970788983, title="Test", username="agipdoom")
    await db.add_channel(ch)

    mock_entity = MagicMock()
    mock_client = AsyncMock()

    async def _get_entity(arg):
        if isinstance(arg, str):
            return mock_entity
        raise ValueError("Could not find the input entity")

    mock_client.get_entity = AsyncMock(side_effect=_get_entity)
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    await collector._collect_channel(ch)

    # Username-first: should resolve by username directly
    mock_client.get_entity.assert_awaited_once_with("agipdoom")


@pytest.mark.asyncio
async def test_collect_channel_no_username_no_cache_reports_error(db):
    """Channel with no username and empty cache -> error logged, 0 messages."""
    ch = Channel(channel_id=1970788983, title="Private Group")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(
        side_effect=ValueError("Could not find the input entity")
    )

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    # Should not crash — collect_all_channels catches exceptions
    stats = await collector.collect_all_channels()
    assert stats["errors"] == 1
    assert stats["messages"] == 0


@pytest.mark.asyncio
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
    return SimpleNamespace(
        id=msg_id,
        text=text,
        media=media,
        sender_id=sender_id,
        sender=None,
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


class _AsyncIterMessages:
    """Async iterator yielding provided messages."""

    def __init__(self, messages):
        self._messages = list(messages)
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg


@pytest.mark.asyncio
async def test_get_media_type_photo():
    from telethon.tl.types import MessageMediaPhoto

    msg = SimpleNamespace(media=MessageMediaPhoto())
    assert Collector._get_media_type(msg) == "photo"


@pytest.mark.asyncio
async def test_get_media_type_none():
    msg = SimpleNamespace(media=None)
    assert Collector._get_media_type(msg) is None


@pytest.mark.asyncio
async def test_get_media_type_document_video():
    from telethon.tl.types import DocumentAttributeVideo, MessageMediaDocument

    attr = DocumentAttributeVideo(duration=10, w=100, h=100, round_message=False)
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)

    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "video"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_media_type_voice():
    from telethon.tl.types import DocumentAttributeAudio, MessageMediaDocument

    attr = DocumentAttributeAudio(duration=10, voice=True)
    doc = SimpleNamespace(attributes=[attr])
    media = MessageMediaDocument(document=doc)

    msg = SimpleNamespace(media=media)
    assert Collector._get_media_type(msg) == "voice"


@pytest.mark.asyncio
async def test_get_media_type_poll():
    from telethon.tl.types import MessageMediaPoll

    msg = SimpleNamespace(media=MessageMediaPoll(poll=None, results=None))
    assert Collector._get_media_type(msg) == "poll"


@pytest.mark.asyncio
async def test_collect_channel_collects_media_without_text(db):
    """Collector should collect messages without text (media-only)."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=5)
    await db.add_channel(ch)

    mock_messages = [
        _make_mock_message(10, text=None),   # media without text
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


@pytest.mark.asyncio
async def test_backfill_uses_no_limit(db):
    """First run (last_collected_id==0) should use limit=None."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=0)
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(messages_per_channel=100, delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    await collector._collect_channel(ch)

    # Verify limit=None was passed (backfill)
    call_kwargs = mock_client.iter_messages.call_args
    assert call_kwargs[1].get("limit") is None or call_kwargs.kwargs.get("limit") is None


@pytest.mark.asyncio
async def test_incremental_uses_configured_limit(db):
    """Subsequent runs (last_collected_id>0) should use configured limit."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=50)
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(messages_per_channel=200, delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    await collector._collect_channel(ch)

    call_kwargs = mock_client.iter_messages.call_args
    assert call_kwargs[1].get("limit") == 200 or call_kwargs.kwargs.get("limit") == 200


@pytest.mark.asyncio
async def test_backfill_batch_flush(db):
    """During backfill, messages should be flushed in batches of 500."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=0)
    await db.add_channel(ch)

    # Create 600 mock messages to trigger at least one flush
    mock_msgs = [_make_mock_message(i, text=f"msg {i}") for i in range(1, 601)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    count = await collector._collect_channel(ch)

    assert count == 600

    # Verify messages are in DB
    messages, total = await db.search_messages(limit=700)
    assert total == 600


@pytest.mark.asyncio
async def test_progress_callback_invoked_on_batch_flush(db):
    """progress_callback is called after each batch flush and final flush."""
    ch = Channel(channel_id=-100123, title="Test", username="test", last_collected_id=0)
    await db.add_channel(ch)

    # 600 msgs → flush at 500 (cb=500), remaining 100 in finally (cb=600)
    mock_msgs = [_make_mock_message(i, text=f"msg {i}") for i in range(1, 601)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_msgs))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    progress_cb = AsyncMock()

    config = SchedulerConfig(delay_between_requests_sec=0)
    collector = Collector(pool, db, config)
    count = await collector._collect_channel(ch, progress_callback=progress_cb)

    assert count == 600
    assert progress_cb.await_count == 2
    progress_cb.assert_any_await(500)
    progress_cb.assert_any_await(600)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


# ---------------------------------------------------------------------------
# Pre-filter: subscriber_ratio tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prefilter_broadcast_low_ratio(db):
    """Broadcast channel with ratio < 1.0 is filtered before iter_messages."""
    ch = Channel(
        channel_id=-100200, title="Spam Channel",
        channel_type="channel", last_collected_id=62000,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100200, subscriber_count=156))

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # iter_messages must NOT be called for collection (last_collected_id > 0, no probe needed)
    mock_client.iter_messages.assert_not_called()

    # Channel must be marked as filtered
    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100200)
    assert stored.is_filtered is True
    assert "low_subscriber_ratio" in stored.filter_flags


@pytest.mark.asyncio
async def test_prefilter_supergroup_low_ratio(db):
    """Supergroup with ratio < 0.02 is filtered before iter_messages."""
    ch = Channel(
        channel_id=-100201, title="Noisy Chat",
        channel_type="supergroup", last_collected_id=10000,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100201, subscriber_count=100))
    # ratio = 100 / 10000 = 0.01 < 0.02

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


@pytest.mark.asyncio
async def test_prefilter_supergroup_pass_ratio(db):
    """Supergroup with ratio >= 0.02 continues collection."""
    ch = Channel(
        channel_id=-100202, title="Good Chat",
        channel_type="supergroup", last_collected_id=1000,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100202, subscriber_count=50))
    # ratio = 50 / 1000 = 0.05 >= 0.02

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


@pytest.mark.asyncio
async def test_prefilter_no_stats_skips_check(db):
    """No stats (subscriber_count=None) → collection continues without filtering."""
    ch = Channel(
        channel_id=-100203, title="Unknown Channel",
        channel_type="channel", last_collected_id=5000,
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


@pytest.mark.asyncio
async def test_prefilter_uses_last_collected_id(db):
    """last_collected_id > 0 → no extra iter_messages(limit=1) probe."""
    ch = Channel(
        channel_id=-100204, title="Established Channel",
        channel_type="supergroup", last_collected_id=500,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100204, subscriber_count=5))
    # ratio = 5 / 500 = 0.01 < 0.02 → filtered

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterEmpty())

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # iter_messages must never be called (last_collected_id used as latest_msg_id)
    mock_client.iter_messages.assert_not_called()


@pytest.mark.asyncio
async def test_prefilter_first_run_probes_latest_msg_id(db):
    """First run (last_collected_id=0) → latest_msg_id via iter_messages(limit=1)."""
    ch = Channel(
        channel_id=-100205, title="New Channel",
        channel_type="supergroup", last_collected_id=0,
    )
    await db.add_channel(ch)
    await db.save_channel_stats(ChannelStats(channel_id=-100205, subscriber_count=1))
    # After probe: latest_msg_id=1000, ratio = 1/1000 = 0.001 < 0.02 → filtered

    probe_msg = SimpleNamespace(
        id=1000, text=None, media=None, sender_id=None, sender=None,
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    call_count = 0

    def _iter_messages_side_effect(entity, **kwargs):
        nonlocal call_count
        call_count += 1
        if kwargs.get("limit") == 1:
            return _AsyncIterMessages([probe_msg])
        return _AsyncIterEmpty()

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=SimpleNamespace())
    mock_client.iter_messages = MagicMock(side_effect=_iter_messages_side_effect)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig())

    count = await collector._collect_channel(ch)

    assert count == 0
    # iter_messages called exactly once (the probe with limit=1)
    assert call_count == 1
    mock_client.iter_messages.assert_called_once()
    call_kwargs = mock_client.iter_messages.call_args
    assert call_kwargs.kwargs.get("limit") == 1 or call_kwargs[1].get("limit") == 1

    channels = await db.get_channels(include_filtered=True)
    stored = next(c for c in channels if c.channel_id == -100205)
    assert stored.is_filtered is True
