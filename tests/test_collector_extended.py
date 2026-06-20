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
    # FakeClientPool inherits the real ResolveGuardMixin (#785), so live-username
    # resolves run through the production guard path instead of auto-fabricated
    # AsyncMock children.
    from tests.helpers import make_mock_pool

    pool = make_mock_pool()
    pool.get_available_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    pool.wait_for_warm = AsyncMock()
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
    # No persistent dedup store in these unit tests — exercise the in-memory
    # matching path directly (the dedup ledger has its own dedicated tests).
    db.repos.notified_messages = None
    return db


@pytest.fixture
def collector(mock_pool, mock_db):
    from src.config import SchedulerConfig

    return Collector(mock_pool, mock_db, SchedulerConfig())


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_collect_channel_pre_filter_min_subs(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, title="Ch")
    mock_db.get_setting.return_value = "100"  # min_subscribers_filter
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=50)]

    # Ensure client is available for internal calls before pre-filter
    mock_pool.get_available_client.return_value = (AsyncMock(), "+7999")

    res = await collector.collect_single_channel(channel)
    assert res == 0
    mock_db.set_channels_filtered_bulk.assert_called_with([(123, "low_subscriber_manual")])


@pytest.mark.anyio
async def test_collect_channel_pre_filter_ratio(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, title="Ch", channel_type="channel")
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=10)]
    # Mock message count query
    mock_db.execute.return_value.fetchone.return_value = [100]  # 10 subs / 100 msgs = 0.1 ratio

    mock_pool.get_available_client.return_value = (AsyncMock(), "+7999")

    res = await collector.collect_single_channel(channel)
    assert res == 0
    mock_db.set_channels_filtered_bulk.assert_called_with([(123, "low_subscriber_ratio")])


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


class _LedgerStore:
    """Minimal notified_messages stand-in tracking recorded sends + has_any."""

    def __init__(self, seeded_channels=()):
        self.recorded: set[tuple[int, int, int]] = set()
        self._seeded = set(seeded_channels)

    async def filter_unnotified(self, query_id, channel_id, message_ids):
        return {mid for mid in message_ids if (query_id, channel_id, mid) not in self.recorded}

    async def record(self, query_id, channel_id, message_ids):
        for mid in message_ids:
            self.recorded.add((query_id, channel_id, mid))

    async def has_any(self, channel_ids):
        cids = set(channel_ids)
        return bool(self._seeded & cids) or any(cid in cids for (_q, cid, _m) in self.recorded)


@pytest.mark.anyio
async def test_empty_ledger_does_not_replay_backlog(collector, mock_db):
    """Regression (#850 review): on the first pass after the ledger table is created it is
    empty, so the 24h backlog must NOT be replayed — otherwise every already-delivered
    historical match is re-sent as a duplicate burst. With an empty ledger only freshly
    collected messages are matched; the backlog rescan is skipped entirely."""
    notifier = AsyncMock()
    collector._notifier = notifier
    sq = SearchQuery(id=1, query="hit", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    store = _LedgerStore()  # empty ledger -> has_any() is False
    mock_db.repos.notified_messages = store
    # A backlog message that would match if replayed — must NOT be fetched/sent.
    backlog_msg = Message(channel_id=1, message_id=1, text="old hit", date=datetime.now())
    get_recent = AsyncMock(return_value=[backlog_msg])
    mock_db.repos.messages.get_recent_for_channels = get_recent

    fresh = [Message(channel_id=1, message_id=50, text="fresh hit", date=datetime.now(), channel_username="c")]
    await collector._check_notification_queries(fresh)

    # Backlog rescan skipped on empty ledger, so get_recent_for_channels was never called...
    get_recent.assert_not_awaited()
    # ...and only the single fresh message produced a notification (no historical replay).
    assert notifier.notify.await_count == 1


@pytest.mark.anyio
async def test_seeded_ledger_replays_backlog_for_retry(collector, mock_db):
    """Once the ledger has rows for a channel, the backlog rescan runs so a previously
    failed send is retried (dedup keeps it from duplicating already-sent ones)."""
    notifier = AsyncMock()
    collector._notifier = notifier
    sq = SearchQuery(id=1, query="hit", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    store = _LedgerStore(seeded_channels={1})  # ledger already has rows for channel 1
    mock_db.repos.notified_messages = store
    backlog_msg = Message(channel_id=1, message_id=7, text="retry hit", date=datetime.now(), channel_username="c")
    get_recent = AsyncMock(return_value=[backlog_msg])
    mock_db.repos.messages.get_recent_for_channels = get_recent

    fresh = [Message(channel_id=1, message_id=50, text="fresh hit", date=datetime.now(), channel_username="c")]
    await collector._check_notification_queries(fresh)

    get_recent.assert_awaited_once()


@pytest.mark.anyio
async def test_fts_query_matches_logic():
    from src.services.notification_matcher import _fts_query_matches

    assert _fts_query_matches("(apple OR orange) AND fruit", "I love apple fruit")
    assert not _fts_query_matches("(apple OR orange) AND fruit", "I love apple juice")
    assert _fts_query_matches("simple", "Simple query test")

    # FTS5 wildcard support
    assert _fts_query_matches("apple*", "I love apples")
    assert _fts_query_matches("(apple* OR orange*) AND fruit*", "fresh apple fruits")
    assert not _fts_query_matches("apple*", "I love bananas")


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_collect_all_stats_no_clients(collector, mock_db):
    mock_db.get_channels.return_value = [Channel(channel_id=123)]
    with patch.object(collector, "_collect_channel_stats", side_effect=NoActiveStatsClientsError):
        res = await collector.collect_all_stats()
        assert res["errors"] == 1


@pytest.mark.anyio
async def test_collect_channel_entity_timeout(collector, mock_pool):
    channel = Channel(channel_id=123, username="user")
    client = AsyncMock()
    mock_pool.get_available_client.return_value = (client, "+7999")
    client.get_input_entity = AsyncMock(side_effect=asyncio.TimeoutError())

    res = await collector._collect_channel(channel)
    assert res == 0


@pytest.mark.anyio
async def test_collect_channel_username_changed(collector, mock_pool, mock_db):
    channel = Channel(channel_id=123, username="old_user")
    client = AsyncMock()
    mock_pool.get_available_client.return_value = (client, "+7999")

    # Fails by username, succeeds by numeric ID
    client.get_input_entity = AsyncMock(side_effect=ValueError())
    client.get_entity = AsyncMock(return_value=MagicMock(id=123, username="new_user", title="New"))

    res = await collector._collect_channel(channel)
    assert res == 0
    mock_db.update_channel_meta.assert_called_once()
    # Both username and title differ from DB → both sticky flags set (sorted alphabetically).
    mock_db.set_channels_filtered_bulk.assert_called_with(
        [(123, "title_changed,username_changed")]
    )


# --- Tests for Collector._handle_meta_change_review (the unified helper) ---


@pytest.mark.anyio
async def test_handle_meta_change_review_no_change(collector, mock_db):
    channel = Channel(channel_id=777, username="same", title="Same", filter_flags="")
    changed = await collector._handle_meta_change_review(
        channel, "same", "Same", log_prefix="Channel"
    )
    assert changed is False
    mock_db.update_channel_meta.assert_not_called()
    mock_db.set_channels_filtered_bulk.assert_not_called()
    mock_db.create_rename_event.assert_not_called()


@pytest.mark.anyio
async def test_handle_meta_change_review_username_only(collector, mock_db):
    channel = Channel(channel_id=777, username="old", title="Same", filter_flags="")
    changed = await collector._handle_meta_change_review(
        channel, "new", "Same", log_prefix="Channel"
    )
    assert changed is True
    mock_db.update_channel_meta.assert_called_once_with(777, username="new", title="Same")
    mock_db.set_channels_filtered_bulk.assert_called_once_with([(777, "username_changed")])
    mock_db.create_rename_event.assert_called_once()


@pytest.mark.anyio
async def test_handle_meta_change_review_title_only(collector, mock_db):
    channel = Channel(channel_id=777, username="same", title="Old", filter_flags="")
    changed = await collector._handle_meta_change_review(
        channel, "same", "New", log_prefix="Stats"
    )
    assert changed is True
    mock_db.set_channels_filtered_bulk.assert_called_once_with([(777, "title_changed")])
    mock_db.create_rename_event.assert_called_once()


@pytest.mark.anyio
async def test_handle_meta_change_review_preserves_existing_flags(collector, mock_db):
    # Channel already has an unrelated filter reason; the helper must preserve it.
    channel = Channel(
        channel_id=777,
        username="old",
        title="Old",
        filter_flags="cross_channel_spam,suspicious_username",
    )
    changed = await collector._handle_meta_change_review(
        channel, "new", "New", log_prefix="Channel"
    )
    assert changed is True
    # Sorted merge of existing + meta flags.
    mock_db.set_channels_filtered_bulk.assert_called_once_with(
        [(777, "cross_channel_spam,suspicious_username,title_changed,username_changed")]
    )


# --- Tests for Collector._resolve_channel_entity (#923 extraction) ---


@pytest.mark.anyio
async def test_resolve_channel_entity_username_flood_keeps_resolve_username_operation(collector):
    """A FloodWait during the username resolve must surface as
    RESOLVE_USERNAME_OPERATION, NOT exc.info.operation — the subtle label the
    inline block preserved before the #923 extraction."""
    from src.telegram.collector import RESOLVE_USERNAME_OPERATION
    from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError

    channel = Channel(channel_id=123, title="Ch", username="somech")
    info = FloodWaitInfo(
        operation="some_other_op",
        phone="+7999",
        wait_seconds=42,
        next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        detail="flood",
    )
    collector._resolve_channel_input_entity = AsyncMock(side_effect=HandledFloodWaitError(info))

    outcome = await collector._resolve_channel_entity(
        channel, MagicMock(), "+7999", 123, False, set()
    )
    assert outcome.entity is None
    assert outcome.flood_wait_sec == 42
    assert outcome.flood_wait_operation == RESOLVE_USERNAME_OPERATION


@pytest.mark.anyio
async def test_resolve_channel_entity_numeric_flood_uses_exc_operation(collector, mock_pool):
    """For the numeric (no-username) resolve, the flood operation comes from the
    exception, unlike the username path."""
    from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError

    channel = Channel(channel_id=123, title="Ch")  # no username
    info = FloodWaitInfo(
        operation="numeric_op",
        phone="+7999",
        wait_seconds=7,
        next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        detail="flood",
    )
    mock_pool.resolve_entity_with_warm = AsyncMock(side_effect=HandledFloodWaitError(info))

    outcome = await collector._resolve_channel_entity(
        channel, MagicMock(), "+7999", 123, False, set()
    )
    assert outcome.flood_wait_sec == 7
    assert outcome.flood_wait_operation == "numeric_op"


@pytest.mark.anyio
async def test_resolve_channel_entity_rate_limit_rotates(collector):
    """A resolve rate-limit with a free account to rotate to yields a 'retry'
    outcome and records the attempted phone."""
    from src.telegram.collector import UsernameResolveRateLimitedError

    channel = Channel(channel_id=123, title="Ch", username="somech")
    collector._resolve_channel_input_entity = AsyncMock(
        side_effect=UsernameResolveRateLimitedError("+7999", 10)
    )
    collector._can_rotate_resolve = AsyncMock(return_value=True)
    attempted: set[str] = set()

    outcome = await collector._resolve_channel_entity(
        channel, MagicMock(), "+7999", 123, False, attempted
    )
    assert outcome.action == "retry"
    assert "+7999" in attempted


@pytest.mark.anyio
async def test_resolve_channel_entity_numeric_rediscover_retry(collector, mock_pool, mock_db):
    """Numeric resolve ValueError → preferred_phone cleared, rediscovered on
    another account → 'retry' with the updated channel."""
    channel = Channel(id=1, channel_id=123, title="Ch", preferred_phone="+7999")
    mock_pool.resolve_entity_with_warm = AsyncMock(side_effect=ValueError("bad peer"))
    mock_pool.get_phone_for_channel = MagicMock(return_value=None)
    mock_pool.clear_channel_phone = MagicMock()
    mock_pool.register_channel_phone = MagicMock()
    mock_db.repos.channels.update_channel_preferred_phone = AsyncMock()
    collector._discover_phone_for_channel = AsyncMock(return_value="+7888")

    outcome = await collector._resolve_channel_entity(
        channel, MagicMock(), "+7999", 123, False, set()
    )
    assert outcome.action == "retry"
    assert outcome.channel is not None
    assert outcome.channel.preferred_phone is None
    mock_pool.register_channel_phone.assert_called_once_with(123, "+7888")


@pytest.mark.anyio
async def test_resolve_channel_entity_numeric_no_rediscovery_deactivates(collector, mock_pool, mock_db):
    """Numeric resolve ValueError with no other account able to resolve →
    'stop' outcome and the channel is deactivated."""
    channel = Channel(id=5, channel_id=123, title="Ch", preferred_phone="+7999")
    mock_pool.resolve_entity_with_warm = AsyncMock(side_effect=ValueError("bad peer"))
    mock_pool.get_phone_for_channel = MagicMock(return_value=None)
    mock_pool.clear_channel_phone = MagicMock()
    mock_db.repos.channels.update_channel_preferred_phone = AsyncMock()
    collector._discover_phone_for_channel = AsyncMock(return_value=None)

    outcome = await collector._resolve_channel_entity(
        channel, MagicMock(), "+7999", 123, False, set()
    )
    assert outcome.action == "stop"
    mock_db.set_channel_active.assert_called_once_with(5, False)
