import asyncio
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FloodWaitError
from telethon.tl import types

from tg_agent.models import Channel, ChannelStats, Message, SearchQuery
from tg_agent.services.notification_matcher import NotificationMatcher
from tg_agent.telegram.collector import AllStatsClientsFloodedError, Collector, NoActiveStatsClientsError


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
    pool.report_family_flood = AsyncMock()
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
    db.repos.channels.fetch_channels_with_tag = AsyncMock(return_value=[])
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
    from tg_agent.config import SchedulerConfig

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
            cast(Any, mock_iter).called = True
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
    # #1832: a collection-history flood scopes to its family, not the account.
    mock_pool.report_family_flood.assert_called_once()
    mock_pool.report_flood.assert_not_called()


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
    sq = cast(Any, SearchQuery)(id=1, query="test", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    # Need messages with actual text and channel_username for link
    msgs = [
        Message(
            channel_id=1, message_id=42, text="This is a test message",
            date=datetime.now(timezone.utc), channel_username="mychan",
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
    sq = cast(Any, SearchQuery)(id=1, query="hello", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    msgs = [
        Message(channel_id=-1001234567890, message_id=99, text="hello world", date=datetime.now(timezone.utc))
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
    sq = cast(Any, SearchQuery)(id=1, query="hit", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    store = _LedgerStore()  # empty ledger -> has_any() is False
    mock_db.repos.notified_messages = store
    # A backlog message that would match if replayed — must NOT be fetched/sent.
    backlog_msg = Message(channel_id=1, message_id=1, text="old hit", date=datetime.now(timezone.utc))
    get_recent = AsyncMock(return_value=[backlog_msg])
    mock_db.repos.messages.get_recent_for_channels = get_recent

    fresh = [
        Message(
            channel_id=1,
            message_id=50,
            text="fresh hit",
            date=datetime.now(timezone.utc),
            channel_username="c",
        )
    ]
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
    sq = cast(Any, SearchQuery)(id=1, query="hit", is_regex=False, is_fts=False)
    mock_db.get_notification_queries.return_value = [sq]

    store = _LedgerStore(seeded_channels={1})  # ledger already has rows for channel 1
    mock_db.repos.notified_messages = store
    backlog_msg = Message(
        channel_id=1,
        message_id=7,
        text="retry hit",
        date=datetime.now(timezone.utc),
        channel_username="c",
    )
    get_recent = AsyncMock(return_value=[backlog_msg])
    mock_db.repos.messages.get_recent_for_channels = get_recent

    fresh = [
        Message(
            channel_id=1,
            message_id=50,
            text="fresh hit",
            date=datetime.now(timezone.utc),
            channel_username="c",
        )
    ]
    await collector._check_notification_queries(fresh)

    get_recent.assert_awaited_once()


@pytest.mark.anyio
async def test_fts_query_matches_logic():
    from tg_agent.services.notification_matcher import _fts_query_matches

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

    from tg_agent.telegram.client_pool import StatsClientAvailability

    mock_pool.get_stats_availability = AsyncMock(
        return_value=StatsClientAvailability(
            state="all_flooded", retry_after_sec=10, next_available_at_utc=datetime.now(timezone.utc)
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
async def test_handle_meta_change_review_marks_unknown_legacy_row_known(collector, mock_db):
    channel = Channel(
        channel_id=777, username=None, title="Same", filter_flags="", username_state="unknown"
    )
    changed = await collector._handle_meta_change_review(
        channel, None, "Same", log_prefix="Channel"
    )
    assert changed is False
    mock_db.update_channel_meta.assert_called_once_with(777, username=None, title="Same")


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
    from telethon_floodgate import FloodWaitInfo, HandledFloodWaitError

    from tg_agent.telegram.collector import RESOLVE_USERNAME_OPERATION

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
    from telethon_floodgate import FloodWaitInfo, HandledFloodWaitError

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
    from tg_agent.telegram.collector import UsernameResolveRateLimitedError

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


class _CacheSession:
    """A session whose cache knows the channel by id, by username, or not at all."""

    def __init__(self, by_id=None, by_username=None):
        self.by_id, self.by_username = by_id, by_username

    async def resolve_cached_input_entity(self, peer):
        found = self.by_username if isinstance(peer, str) else self.by_id
        if found is None:
            raise ValueError("not cached")
        return found

    async def get_input_entity(self, peer):  # the live lookup; the pool's guard runs it
        raise AssertionError("the pool's guard is mocked out")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("by_id", "by_username", "expected"),
    [
        ("id-hit", None, "cache:id"),
        (None, MagicMock(channel_id=123), "cache:username"),
        (None, None, "live"),
    ],
)
async def test_channel_key_reports_where_it_came_from(collector, mock_pool, by_id, by_username, expected):
    """#1877: the collect line names the key's source, so a restart that lost the
    cached keys shows up as live lookups instead of passing unnoticed."""
    mock_pool.run_live_username_resolve = AsyncMock(return_value="live-hit")
    session = _CacheSession(by_id, by_username)

    _entity, source = await collector._resolve_channel_input_entity(
        session, channel_id=123, username="somech", phone="+7999"
    )

    assert source == expected
    if expected == "live":
        call = mock_pool.run_live_username_resolve.await_args
        assert call is not None and call.kwargs["channel_id"] == 123


@pytest.mark.anyio
async def test_collect_line_carries_the_key_source(collector, caplog):
    from tg_agent.telegram.collector_resolve import ResolveOutcome

    channel = Channel(id=1, channel_id=123, title="Ch", username="somech")
    collector._acquire_collection_client = AsyncMock(return_value=(MagicMock(), "+79990001122", False))
    collector._resolve_channel_entity = AsyncMock(return_value=ResolveOutcome(entity="peer", source="cache:id"))
    collector._apply_pre_collection_filters = AsyncMock(return_value=False)
    collector._release_collection_client = AsyncMock()
    caplog.set_level("INFO", logger="tg_agent.telegram.collector")

    await collector._collect_channel(channel)

    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Collecting channel")]
    assert len(lines) == 1 and lines[0].endswith("account=+79...1122, key=cache:id"), lines


@pytest.mark.anyio
async def test_username_channel_goes_to_the_account_that_holds_its_key(collector, mock_pool):
    """#1877: a username channel is collected on its saved account, which holds its
    access hash, not on whichever account is free — that one would look it up again."""
    channel = Channel(id=1, channel_id=123, title="Ch", username="somech", preferred_phone="+7888")
    mock_pool.get_available_client.return_value = (MagicMock(), "+7888")

    await collector._acquire_collection_client(channel, set())

    # Collection's own pick with the channel's account first: floods still count.
    mock_pool.get_available_client.assert_awaited_once_with(prefer="+7888")


@pytest.mark.anyio
async def test_username_channel_skips_accounts_that_failed_its_lookup(collector, mock_pool):
    channel = Channel(id=1, channel_id=123, title="Ch", username="somech", preferred_phone="+7888")
    mock_pool.get_available_client.return_value = (MagicMock(), "+7999")

    _session, phone, _cache_only = await collector._acquire_collection_client(channel, {"+7888"})

    assert phone == "+7999"
    mock_pool.get_client_by_phone.assert_not_awaited()
    mock_pool.get_available_client.assert_awaited_once_with(exclude_phones={"+7888"})


@pytest.mark.anyio
async def test_a_username_lookup_remembers_the_account(collector, mock_pool):
    """#1877: the account that resolved a username channel becomes its account, so
    the next pass goes there and finds the key in that account's cache."""
    channel = Channel(id=1, channel_id=123, title="Ch", username="somech")
    collector._resolve_channel_input_entity = AsyncMock(
        return_value=(types.InputPeerChannel(channel_id=123, access_hash=5), "live")
    )
    mock_pool.get_phone_for_channel = MagicMock(return_value=None)
    mock_pool.remember_channel_phone = AsyncMock()

    outcome = await collector._resolve_channel_entity(channel, MagicMock(), "+7999", 123, False, set())

    assert outcome.source == "live"
    mock_pool.remember_channel_phone.assert_awaited_once_with(123, "+7999", known_preferred=None, force=True)


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
async def test_resolve_channel_entity_numeric_no_rediscovery_is_quarantined(collector, mock_pool, mock_db):
    """Numeric resolve ValueError with no other account able to resolve →
        'stop' outcome and the channel is quarantined."""
    channel = Channel(id=5, channel_id=123, title="Ch", preferred_phone="+7999")
    mock_pool.resolve_entity_with_warm = AsyncMock(side_effect=ValueError("bad peer"))
    mock_pool.get_phone_for_channel = MagicMock(return_value=None)
    mock_pool.clear_channel_phone = MagicMock()
    mock_db.repos.channels.update_channel_preferred_phone = AsyncMock()
    mock_db.repos.channels.set_channel_review = AsyncMock()
    collector._discover_phone_for_channel = AsyncMock(return_value=None)

    outcome = await collector._resolve_channel_entity(
        channel, MagicMock(), "+7999", 123, False, set()
    )
    assert outcome.action == "stop"
    mock_db.set_channel_active.assert_not_awaited()
    mock_db.repos.channels.set_channel_review.assert_awaited_once_with(5, "resolve_account_unavailable")


def _vacant_username_setup(collector, mock_pool, mock_db, *, username_error, fallback_error, idle=()):
    collector._resolve_channel_input_entity = AsyncMock(side_effect=username_error)
    mock_pool.resolve_entity_with_warm = AsyncMock(side_effect=fallback_error)
    # ``idle``: active accounts out of the pool, any of which may know the channel.
    mock_db.get_account_summaries = AsyncMock(return_value=[MagicMock(phone=phone) for phone in idle])
    mock_db.repos.channels.set_channel_review = AsyncMock()
    return Channel(id=5, channel_id=123, title="Ch", username="somech")


@pytest.mark.anyio
async def test_resolve_channel_entity_vacant_username_and_unknown_id_is_terminal(collector, mock_pool, mock_db):
    """Telegram says nobody owns the name and no other account can know the
    channel: nothing recovers the row, so the verdict is terminal."""
    from telethon.errors import UsernameNotOccupiedError

    from tg_agent.telegram.collector_types import ChannelCollectionTerminalError

    channel = _vacant_username_setup(
        collector, mock_pool, mock_db,
        username_error=UsernameNotOccupiedError(request=None),
        fallback_error=ValueError("bad peer"),
    )

    with pytest.raises(ChannelCollectionTerminalError) as caught:
        await collector._resolve_channel_entity(channel, MagicMock(), "+7999", 123, False, set())

    assert caught.value.code == "UsernameNotOccupiedError"
    mock_db.repos.channels.set_channel_review.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("username_error", "fallback_error", "idle"),
    [
        # another account is out of the pool: a miss here proves nothing about it
        ("vacant", ValueError("bad peer"), ("+7002",)),
        # the numeric lookup failed for a transient reason, not a peer miss
        ("vacant", RuntimeError("network down"), ()),
        # Telethon's ValueError for a request that kept failing is no miss either
        ("vacant", ValueError("Request was unsuccessful 6 time(s)"), ()),
        # a bare cache miss is not Telegram's "nobody owns this name"
        ("cache-miss", ValueError("bad peer"), ()),
    ],
    ids=["account-not-connected", "transient-fallback", "failing-request", "cache-miss"],
)
async def test_resolve_channel_entity_inconclusive_username_miss_stays_transient(
    collector, mock_pool, mock_db, username_error, fallback_error, idle
):
    from telethon.errors import UsernameNotOccupiedError

    error = UsernameNotOccupiedError(request=None) if username_error == "vacant" else ValueError("not cached")
    channel = _vacant_username_setup(
        collector, mock_pool, mock_db, username_error=error, fallback_error=fallback_error, idle=idle
    )

    outcome = await collector._resolve_channel_entity(channel, MagicMock(), "+7999", 123, False, set())

    assert outcome.action == "stop"
    mock_db.repos.channels.set_channel_review.assert_awaited_once_with(5, "resolve_transient_error")


@pytest.mark.anyio
async def test_resolve_channel_entity_vacant_username_known_elsewhere_routes_there(collector, mock_pool, mock_db):
    """Another account knows the id: not terminal, and the next pass reads there."""
    from telethon.errors import UsernameNotOccupiedError

    channel = _vacant_username_setup(
        collector, mock_pool, mock_db,
        username_error=UsernameNotOccupiedError(request=None),
        fallback_error=ValueError("bad peer"),
    )
    collector._discover_phone_for_channel = AsyncMock(return_value="+7002")

    outcome = await collector._resolve_channel_entity(channel, MagicMock(), "+7999", 123, False, set())

    assert outcome.action == "stop"
    assert mock_pool.get_phone_for_channel(123) == "+7002"


# ------------------------------------------------------------------
# Mutation slice #5 killer tests: behaviors that survived mutmut
# ------------------------------------------------------------------


@pytest.mark.anyio
async def test_pre_filter_min_subs_boundaries(collector, mock_db):
    """min_subs filtering: exact subscriber_count must PASS (strict <), a zero
    filter (min_subs<=1 boundary) still filters a 0-subscriber channel
    (#mutation slice 5)."""
    from tg_agent.models import Channel, ChannelStats

    channel = Channel(channel_id=123, title="Ch", channel_type="channel")
    collector._maybe_auto_delete = AsyncMock()

    # subs == min_subs → strict '<' means KEEP; no filter decision recorded.
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=100)]
    mock_db.execute.return_value.fetchone.return_value = [100]  # ratio 1.0, threshold 1.0 → keep
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=100, cancel_event=None, phone="+1",
    ) is True
    mock_db.set_channels_filtered_bulk.assert_not_called()

    # min_subs=1 with 0 subscribers → filtered (kills the min_subs>1 drift).
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=0)]
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=1, cancel_event=None, phone="+1",
    ) is False
    mock_db.set_channels_filtered_bulk.assert_called_once_with([(123, "low_subscriber_manual")])
    collector._maybe_auto_delete.assert_awaited_once_with(123)


@pytest.mark.anyio
async def test_pre_filter_ratio_boundary(collector, mock_db):
    """ratio == threshold must KEEP (strict '<'), for both broadcast and chat
    thresholds (#mutation slice 5)."""
    from tg_agent.models import Channel, ChannelStats

    # broadcast: threshold 1.0, ratio 10/10 == 1.0 → keep
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=10)]
    broadcast = Channel(channel_id=123, title="Ch", channel_type="channel")
    mock_db.execute.return_value.fetchone.return_value = [10]
    assert await collector._apply_pre_collection_filters(
        broadcast, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=0, cancel_event=None, phone="+1",
    ) is True

    # chat: threshold 0.02, ratio 2/100 == 0.02 → keep
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=2)]
    chat = Channel(channel_id=123, title="Ch", channel_type="supergroup")
    mock_db.execute.return_value.fetchone.return_value = [100]
    assert await collector._apply_pre_collection_filters(
        chat, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=0, cancel_event=None, phone="+1",
    ) is True
    mock_db.set_channels_filtered_bulk.assert_not_called()


@pytest.mark.anyio
async def test_pre_filter_cross_dupe_sample_and_ratio_boundaries(collector, mock_db):
    """Cross-dupe precheck runs at exactly MIN_SAMPLE unique prefixes and filters
    at exactly RATIO; a low match ratio keeps the channel (#mutation slice 5)."""
    from tg_agent.models import Channel

    channel = Channel(channel_id=123, title="Ch")
    collector._maybe_auto_delete = AsyncMock()
    mock_db.get_channel_stats.return_value = []  # no stats → ratio stage inert
    repo_counter = mock_db.filter_repo.count_matching_prefixes_in_other_channels

    # Exactly MIN_SAMPLE=5 unique prefixes → the dupe check RUNS (>= not >).
    collector._precheck_sample = AsyncMock(return_value=["p1", "p2", "p3", "p4", "p5"])
    repo_counter.return_value = 0  # 0/5 < 0.8 → keep
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=True, force=False, min_subs=0, cancel_event=None, phone="+1",
    ) is True
    repo_counter.assert_awaited_once_with(123, ["p1", "p2", "p3", "p4", "p5"])

    # Exactly RATIO=0.8: 8/10 matches → filtered as cross_channel_spam.
    collector._precheck_sample = AsyncMock(return_value=[f"q{i}" for i in range(10)])
    repo_counter.return_value = 8
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=True, force=False, min_subs=0, cancel_event=None, phone="+1",
    ) is False
    mock_db.set_channels_filtered_bulk.assert_called_once_with([(123, "cross_channel_spam")])

    # 1/10 matches must NOT trip the filter (kills the '/'->'*' drift).
    mock_db.set_channels_filtered_bulk.reset_mock()
    repo_counter.return_value = 1
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=True, force=False, min_subs=0, cancel_event=None, phone="+1",
    ) is True
    mock_db.set_channels_filtered_bulk.assert_not_called()


@pytest.mark.anyio
async def test_pre_collection_filters_skip_keep_channel(collector, mock_db):
    """Keep-тег выводит канал из авто-метрик pre-фильтра (#1516): min_subs и
    ratio не применяются, сбор продолжается; без тега — фильтрует как раньше."""
    from tg_agent.models import Channel, ChannelStats

    channel = Channel(channel_id=123, title="Ch", channel_type="channel")
    collector._maybe_auto_delete = AsyncMock()
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=0)]

    # keep-тег → guard, ни min_subs, ни ratio не применяются.
    mock_db.repos.channels.fetch_channels_with_tag = AsyncMock(
        return_value=[{"pk": 1, "channel_id": 123, "filter_flags": ""}]
    )
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=100, cancel_event=None, phone="+1",
    ) is True
    mock_db.set_channels_filtered_bulk.assert_not_called()

    # Без тега → 0 подписчиков < min_subs=100 → фильтр (контраст).
    mock_db.repos.channels.fetch_channels_with_tag = AsyncMock(return_value=[])
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=100, cancel_event=None, phone="+1",
    ) is False
    mock_db.set_channels_filtered_bulk.assert_called_once_with([(123, "low_subscriber_manual")])


@pytest.mark.anyio
async def test_post_collection_low_uniqueness_skips_keep_channel(collector, mock_db):
    """Post-collection low_uniqueness тоже соблюдает контракт keep (#1516)."""
    collector._maybe_auto_delete = AsyncMock()
    mock_db.execute.return_value.fetchone.return_value = {"total": 60, "uniq": 5}  # 8.3% < порога

    mock_db.repos.channels.fetch_channels_with_tag = AsyncMock(
        return_value=[{"pk": 1, "channel_id": 55, "filter_flags": ""}]
    )
    await collector._post_collection_actions(
        55, is_first_run=True, force=False, collected_count=60, saw_topic_message=False
    )
    mock_db.set_channels_filtered_bulk.assert_not_called()

    # Без тега → low_uniqueness ставится (контраст).
    mock_db.repos.channels.fetch_channels_with_tag = AsyncMock(return_value=[])
    await collector._post_collection_actions(
        55, is_first_run=True, force=False, collected_count=60, saw_topic_message=False
    )
    mock_db.set_channels_filtered_bulk.assert_called_once_with([(55, "low_uniqueness")])


@pytest.mark.anyio
async def test_pre_filter_suppression_stops_only_when_channel_is_filtered(collector, mock_db):
    """A suppressed filter decision defers to _stop_if_channel_filtered: channel
    not in filtered state → collection proceeds (#mutation slice 5)."""
    from tg_agent.models import Channel, ChannelStats

    channel = Channel(channel_id=123, title="Ch", channel_type="channel")
    collector._maybe_auto_delete = AsyncMock()
    mock_db.get_channel_stats.return_value = [ChannelStats(channel_id=123, subscriber_count=50)]
    mock_db.execute.return_value.fetchone.return_value = [10]  # ratio 5.0 → keep
    decision = MagicMock(applied=False, suppressed=True)
    mock_db.set_channels_filtered_bulk.return_value = decision

    # Not filtered in DB → keep collecting.
    collector._stop_if_channel_filtered = AsyncMock(return_value=False)
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=100, cancel_event=None, phone="+1",
    ) is True
    collector._stop_if_channel_filtered.assert_awaited_once_with(123)

    # Channel IS filtered → stop.
    collector._stop_if_channel_filtered = AsyncMock(return_value=True)
    assert await collector._apply_pre_collection_filters(
        channel, 123, MagicMock(), MagicMock(),
        is_first_run=False, force=False, min_subs=100, cancel_event=None, phone="+1",
    ) is False


@pytest.mark.anyio
async def test_collect_all_channels_gates_stats_and_counter(collector, mock_db):
    """collect_all_channels queries active+unfiltered only, forwards the loaded
    min_subs, sums the stats, and rebalances the concurrency counter to 0
    (#mutation slice 5)."""
    from tg_agent.models import Channel

    channels = [Channel(channel_id=123, title="A"), Channel(channel_id=456, title="B")]
    mock_db.get_channels = AsyncMock(return_value=channels)
    mock_db.get_setting = AsyncMock(return_value="30")

    seen = []
    counts = []

    async def fake_collect(channel, min_subs=0):
        seen.append((channel.channel_id, min_subs))
        counts.append(collector._active_collection_count)
        return 2

    collector._collect_channel = fake_collect

    with patch("asyncio.sleep", AsyncMock()):
        stats = await collector.collect_all_channels()

    mock_db.get_channels.assert_awaited_once_with(active_only=True, include_filtered=False)
    assert seen == [(123, 30), (456, 30)]
    assert counts == [1, 1]  # incremented for the duration of the run
    assert stats == {"channels": 2, "messages": 4, "errors": 0}
    assert collector._active_collection_count == 0  # and rebalanced afterwards


# ------------------------------------------------------------------
# Mutation slice #5 killer tests: post-collection flood contract
# ------------------------------------------------------------------


@pytest.mark.anyio
async def test_post_collection_flood_transient_resolve_sleeps_and_continues(collector):
    """A transient (<60s) resolve flood sleeps it off and retries the same
    channel without touching backoff (#mutation slice 5)."""
    from tg_agent.telegram.collector_mixins.collection import RESOLVE_USERNAME_OPERATION

    channel = Channel(channel_id=123, title="Ch")
    with patch(
        "tg_agent.telegram.collector_mixins.collection.sleep_for_flood_wait_seconds", AsyncMock()
    ) as slept:
        kind, total, ch = await collector._handle_post_collection_flood(
            channel, "+1", 10, RESOLVE_USERNAME_OPERATION, set(), 5, 2
        )
    assert (kind, total) == ("continue", 5)
    assert ch is channel
    slept.assert_awaited_once()


@pytest.mark.anyio
async def test_post_collection_flood_short_resolve_returns_without_backoff(collector, mock_pool):
    """At exactly the 300s threshold the resolve flood skips just this channel —
    no backoff is recorded and the run returns (#mutation slice 5)."""
    from tg_agent.telegram.collector_mixins.collection import RESOLVE_USERNAME_OPERATION

    mock_pool.get_resolve_username_backoff_until = MagicMock(return_value=None)
    mock_pool.set_resolve_username_backoff = MagicMock()

    kind, total, _ch = await collector._handle_post_collection_flood(
        Channel(channel_id=123, title="Ch"), "+1", 300, RESOLVE_USERNAME_OPERATION, set(), 5, 2
    )
    assert (kind, total) == ("return", 5)
    mock_pool.set_resolve_username_backoff.assert_not_called()


@pytest.mark.anyio
async def test_post_collection_flood_long_resolve_sets_backoff_then_rotates(collector, mock_pool):
    """A >300s resolve flood records the defensive backoff (persisting it) and
    rotates the channel to another account, advancing the total
    (#mutation slice 5)."""
    from datetime import timedelta

    from tg_agent.telegram.collector_mixins.collection import RESOLVE_USERNAME_OPERATION

    deadline = datetime.now(timezone.utc) + timedelta(seconds=400)
    mock_pool.get_resolve_username_backoff_until = MagicMock(return_value=None)
    mock_pool.set_resolve_username_backoff = MagicMock(return_value=deadline)
    mock_pool.persist_resolve_username_backoff = AsyncMock()
    collector._can_rotate_resolve = AsyncMock(return_value=True)

    kind, total, _ch = await collector._handle_post_collection_flood(
        Channel(channel_id=123, title="Ch"), "+1", 301, RESOLVE_USERNAME_OPERATION, set(), 5, 2
    )
    assert (kind, total) == ("continue", 7)  # total + collected_count
    mock_pool.set_resolve_username_backoff.assert_called_once_with(301, phone="+1")
    mock_pool.persist_resolve_username_backoff.assert_awaited_once()


@pytest.mark.anyio
async def test_post_collection_flood_no_rotation_raises_deferred_with_future_deadline(
    collector, mock_pool
):
    """When no account can take the channel, the handler raises the deferred
    error with a FUTURE deadline — a `now - timedelta` drift would raise an
    already-expired backoff and hot-loop the channel (#mutation slice 5)."""
    from datetime import timedelta

    from telethon_floodgate import UsernameResolveFloodWaitDeferredError

    from tg_agent.telegram.collector_mixins.collection import RESOLVE_USERNAME_OPERATION

    deadline = datetime.now(timezone.utc) + timedelta(seconds=400)
    mock_pool.get_resolve_username_backoff_until = MagicMock(return_value=None)
    mock_pool.set_resolve_username_backoff = MagicMock(return_value=deadline)
    mock_pool.persist_resolve_username_backoff = AsyncMock()
    collector._can_rotate_resolve = AsyncMock(return_value=False)
    collector._next_resolve_capable_at = AsyncMock(return_value=None)
    collector._notifier = AsyncMock()

    with pytest.raises(UsernameResolveFloodWaitDeferredError) as excinfo:
        await collector._handle_post_collection_flood(
            Channel(channel_id=123, title="Ch"), "+1", 301, RESOLVE_USERNAME_OPERATION, set(), 5, 2
        )
    assert excinfo.value.next_available_at is deadline
    assert excinfo.value.wait_seconds > 300
    collector._notifier.notify.assert_awaited_once()


@pytest.mark.anyio
async def test_post_collection_flood_plain_flood_notifies_above_max_and_rereads(
    collector, mock_db
):
    """A plain collection flood at exactly max_flood_wait_sec notifies NOTHING
    (strict >), re-reads the channel by pk and continues with the advanced
    total; with no pk the run returns (#mutation slice 5)."""
    max_wait = collector._config.max_flood_wait_sec
    collector._notifier = AsyncMock()
    channel = Channel(id=7, channel_id=123, title="Ch")
    updated = Channel(id=7, channel_id=123, title="Ch", last_collected_id=99)
    mock_db.get_channel_by_pk = AsyncMock(return_value=updated)

    kind, total, ch = await collector._handle_post_collection_flood(
        channel, "+1", max_wait, None, set(), 5, 2
    )
    collector._notifier.notify.assert_not_awaited()
    assert (kind, total) == ("continue", 7)
    mock_db.get_channel_by_pk.assert_awaited_once_with(7)
    assert ch is updated

    # No pk → nothing to re-read → stop the run.
    kind, total, ch = await collector._handle_post_collection_flood(
        Channel(channel_id=123, title="Ch"), "+1", max_wait + 1, None, set(), 5, 2
    )
    assert (kind, total) == ("return", 5)


# ------------------------------------------------------------------
# Provenance stage 4 (#1312): префильтры сбора не трогают human-каналы
# ------------------------------------------------------------------


@pytest.mark.anyio
async def test_pre_filter_suppressed_on_human_channel(db, caplog):
    """Пречек min_subs на человеческом канале подавлен: флаг не ставится,
    origin не меняется, в логе видно подавление, сбор продолжается (#1312)."""
    import logging

    from tg_agent.config import SchedulerConfig

    await db.add_channel(Channel(channel_id=802, title="802", filtered_origin="human"))
    await db.execute_write(
        "INSERT INTO channel_stats (channel_id, subscriber_count) VALUES (?, ?)",
        (802, 3),
    )

    collector = Collector(MagicMock(), db, SchedulerConfig())
    collector._maybe_auto_delete = AsyncMock()

    channel = Channel(channel_id=802, title="802", channel_type="supergroup")
    with caplog.at_level(logging.INFO):
        proceed = await collector._apply_pre_collection_filters(
            channel, 802, MagicMock(), MagicMock(),
            is_first_run=False, force=False, min_subs=20, cancel_event=None, phone="+1",
        )

    assert proceed is True
    collector._maybe_auto_delete.assert_not_awaited()
    row = await db.get_channel_by_channel_id(802)
    assert row.is_filtered is False
    assert row.filtered_origin == "human"
    assert not row.filter_flags
    assert "Suppressed automatic channel decision" in caplog.text
