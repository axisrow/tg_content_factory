import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FloodWaitError

from src.models import Channel, ChannelStats, Message, SearchQuery
from src.services.notification_matcher import NotificationMatcher
from src.telegram.collector import AllStatsClientsFloodedError, Collector, NoActiveStatsClientsError


@pytest.fixture
def mock_pool():
    pool = AsyncMock()
    pool.get_available_client = AsyncMock()
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    return pool


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    db.get_channels = AsyncMock(return_value=[])
    db.get_channel_by_channel_id = AsyncMock()
    db.get_channel_by_pk = AsyncMock()
    db.insert_messages_batch = AsyncMock()
    db.execute = AsyncMock()
    db.set_channels_filtered_bulk = AsyncMock()
    db.update_channel_last_id = AsyncMock()
    db.update_channel_meta = AsyncMock()
    db.set_channel_active = AsyncMock()
    db.get_channel_stats = AsyncMock(return_value=[])
    db.filter_repo.count_matching_prefixes_in_other_channels = AsyncMock(return_value=0)
    db.get_notification_queries = AsyncMock(return_value=[])
    db.get_forum_topics = AsyncMock(return_value=[])
    db.upsert_forum_topics = AsyncMock()
    db.save_channel_stats = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.create_rename_event = AsyncMock()
    return db


@pytest.fixture
def collector(mock_pool, mock_db):
    from src.config import SchedulerConfig

    return Collector(mock_pool, mock_db, SchedulerConfig())


@pytest.mark.asyncio
async def test_collect_channel_flood_wait_retry(collector, mock_pool, mock_db):
    channel = Channel(id=1, channel_id=123, title="Ch", last_collected_id=10)
    mock_db.get_channel_by_pk.return_value = channel

    client = AsyncMock()
    mock_pool.get_available_client.return_value = (client, "+7999")

    # First call to iter_messages raises FloodWait, second succeeds
    async def mock_iter(*args, **kwargs):
        if not hasattr(mock_iter, "called"):
            mock_iter.called = True
            raise FloodWaitError(5)
        m = MagicMock(id=11, text="msg", date=datetime.now(timezone.utc))
        m.sender.first_name = "First"
        m.sender.last_name = "Last"
        yield m

    client.iter_messages = mock_iter
    client.get_entity.return_value = MagicMock(id=123)

    # Mock flush batch to return True
    mock_db.execute.return_value.fetchall.return_value = [{"message_id": 11}]

    res = await collector.collect_single_channel(channel)
    assert res == 1
    mock_pool.report_flood.assert_called_once()


@pytest.mark.asyncio
async def test_collect_channel_pre_filter_min_subs(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, title="Ch")
    mock_db.get_setting.return_value = "100"  # min_subscribers_filter
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=50)]

    # Ensure client is available for internal calls before pre-filter
    mock_pool.get_available_client.return_value = (AsyncMock(), "+7999")

    res = await collector.collect_single_channel(channel)
    assert res == 0
    mock_db.set_channels_filtered_bulk.assert_called_with([(123, "low_subscriber_manual")])


@pytest.mark.asyncio
async def test_collect_channel_pre_filter_ratio(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, title="Ch", channel_type="channel")
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=10)]
    # Mock message count query
    mock_db.execute.return_value.fetchone.return_value = [100]  # 10 subs / 100 msgs = 0.1 ratio

    mock_pool.get_available_client.return_value = (AsyncMock(), "+7999")

    res = await collector.collect_single_channel(channel)
    assert res == 0
    mock_db.set_channels_filtered_bulk.assert_called_with([(123, "low_subscriber_ratio")])


@pytest.mark.asyncio
async def test_flush_batch_persistence_error(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, title="Ch")
    client = AsyncMock()
    mock_pool.get_available_client.return_value = (client, "+7999")
    client.get_entity.return_value = MagicMock(id=123)

    async def mock_iter(*args, **kwargs):
        m = MagicMock(id=11, text="msg", date=datetime.now(timezone.utc))
        m.sender.first_name = "First"
        m.sender.last_name = "Last"
        yield m

    client.iter_messages = mock_iter

    # Mock execute to return empty list (persistence failed)
    mock_db.execute.return_value.fetchall.return_value = []

    res = await collector._collect_channel(channel)
    assert res == 0  # persisted_max_msg_id not updated, stop_due_to_persistence_error = True


@pytest.mark.asyncio
async def test_notification_queries_logic(collector, mock_db):
    notifier = AsyncMock()
    collector._notifier = notifier
    collector._notification_matcher = NotificationMatcher(notifier)
    sq = SearchQuery(id=1, query="test", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    # Need messages with actual text and channel_username for link
    msgs = [
        Message(
            channel_id=1, message_id=42, text="This is a test message",
            date=datetime.now(), channel_username="mychan",
        )
    ]
    await collector._check_notification_queries(msgs)
    notifier.notify.assert_called_once()
    call_text = notifier.notify.call_args[0][0]
    assert "https://t.me/mychan/42" in call_text


@pytest.mark.asyncio
async def test_notification_queries_private_channel_link(collector, mock_db):
    """Private channel (no username) should produce t.me/c/ link."""
    notifier = AsyncMock()
    collector._notifier = notifier
    collector._notification_matcher = NotificationMatcher(notifier)
    sq = SearchQuery(id=1, query="hello", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    msgs = [
        Message(channel_id=-1001234567890, message_id=99, text="hello world", date=datetime.now())
    ]
    await collector._check_notification_queries(msgs)
    notifier.notify.assert_called_once()
    call_text = notifier.notify.call_args[0][0]
    assert "https://t.me/c/1234567890/99" in call_text


@pytest.mark.asyncio
async def test_fts_query_matches_logic():
    from src.services.notification_matcher import _fts_query_matches

    assert _fts_query_matches("(apple OR orange) AND fruit", "I love apple fruit")
    assert not _fts_query_matches("(apple OR orange) AND fruit", "I love apple juice")
    assert _fts_query_matches("simple", "Simple query test")

    # FTS5 wildcard support
    assert _fts_query_matches("apple*", "I love apples")
    assert _fts_query_matches("(apple* OR orange*) AND fruit*", "fresh apple fruits")
    assert not _fts_query_matches("apple*", "I love bananas")


@pytest.mark.asyncio
async def test_collect_channel_stats_flooded_error(collector, mock_pool):
    channel = Channel(channel_id=123)
    mock_pool.get_available_client.return_value = None

    from src.telegram.client_pool import StatsClientAvailability

    mock_pool.get_stats_availability = AsyncMock(
        return_value=StatsClientAvailability(
            state="all_flooded", retry_after_sec=10, next_available_at_utc=datetime.now()
        )
    )

    with pytest.raises(AllStatsClientsFloodedError):
        await collector._collect_channel_stats(channel)


@pytest.mark.asyncio
async def test_collect_all_stats_no_clients(collector, mock_db):
    mock_db.get_channels.return_value = [Channel(channel_id=123)]
    with patch.object(collector, "_collect_channel_stats", side_effect=NoActiveStatsClientsError):
        res = await collector.collect_all_stats()
        assert res["errors"] == 1


@pytest.mark.asyncio
async def test_collect_channel_entity_timeout(collector, mock_pool):
    channel = Channel(channel_id=123, username="user")
    client = AsyncMock()
    mock_pool.get_available_client.return_value = (client, "+7999")
    client.get_entity.side_effect = asyncio.TimeoutError()

    res = await collector._collect_channel(channel)
    assert res == 0


@pytest.mark.asyncio
async def test_collect_channel_username_changed(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, username="old_user")
    client = AsyncMock()
    mock_pool.get_available_client.return_value = (client, "+7999")

    # Fails by username, succeeds by numeric ID
    client.get_entity.side_effect = [
        ValueError(),
        MagicMock(id=123, username="new_user", title="New"),
    ]

    res = await collector._collect_channel(channel)
    assert res == 0
    mock_db.update_channel_meta.assert_called_once()
    # Both username and title differ from DB → both sticky flags set (sorted alphabetically).
    mock_db.set_channels_filtered_bulk.assert_called_with(
        [(123, "title_changed,username_changed")]
    )
