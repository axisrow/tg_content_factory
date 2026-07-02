"""Tests for MessagesRepository."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.messages import MessagesRepository, _normalize_date_to
from src.models import Message, SearchParams, SearchQuery


@pytest.fixture
async def channels_repo(db):
    """Create channels repository instance."""
    return ChannelsRepository(db.db, database=db)


def make_message(
    channel_id: int,
    message_id: int,
    text: str = "Test message",
    date: datetime | None = None,
    **kwargs,
) -> Message:
    """Create a test Message."""
    return Message(
        channel_id=channel_id,
        message_id=message_id,
        text=text,
        date=date or datetime.now(timezone.utc),
        **kwargs,
    )


# insert_message tests


async def test_insert_message_success(messages_repo):
    """Test inserting a message successfully."""
    msg = make_message(1, 100, "Hello world")
    result = await messages_repo.insert_message(msg)
    assert result is True


async def test_insert_message_duplicate_ignored(messages_repo):
    """Test that duplicate messages are ignored."""
    msg = make_message(1, 100, "First")
    await messages_repo.insert_message(msg)

    msg2 = make_message(1, 100, "Second")  # Same channel_id + message_id
    result = await messages_repo.insert_message(msg2)
    assert result is False

    # Verify only one message exists
    messages, total = await messages_repo.search_messages(SearchParams())
    assert total == 1
    assert messages[0].text == "First"


async def test_insert_message_with_all_fields(messages_repo):
    """Test inserting message with all optional fields."""
    msg = Message(
        channel_id=1,
        message_id=100,
        sender_id=12345,
        sender_name="John Doe",
        text="Full message",
        media_type="photo",
        topic_id=5,
        date=datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc),
    )
    result = await messages_repo.insert_message(msg)
    assert result is True

    messages, _ = await messages_repo.search_messages(SearchParams())
    assert messages[0].sender_id == 12345
    assert messages[0].sender_name == "John Doe"
    assert messages[0].media_type == "photo"
    assert messages[0].topic_id == 5


async def test_insert_message_sender_identity_round_trips(messages_repo):
    msg = Message(
        channel_id=1,
        message_id=102,
        sender_id=12345,
        sender_name="John Doe",
        sender_first_name="John",
        sender_last_name="Doe",
        sender_username="@jdoe",
        text="Identity message",
        date=datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert await messages_repo.insert_message(msg) is True

    messages, _ = await messages_repo.search_messages(SearchParams())
    stored = messages[0]
    assert stored.sender_id == 12345
    assert stored.sender_name == "John Doe"
    assert stored.sender_first_name == "John"
    assert stored.sender_last_name == "Doe"
    assert stored.sender_username == "jdoe"


async def test_insert_message_with_structured_facets(messages_repo):
    """Structured message classification fields should round-trip through storage."""
    msg = Message(
        channel_id=1,
        message_id=101,
        text="Alice joined via invite link",
        media_type="service",
        message_kind="service",
        service_action_raw="MessageActionChatJoinedByLink",
        service_action_semantic="join",
        service_action_payload_json='{"inviter_id": 42}',
        sender_kind="user",
        date=datetime(2026, 3, 16, 12, 5, 0, tzinfo=timezone.utc),
    )
    result = await messages_repo.insert_message(msg)
    assert result is True

    messages, _ = await messages_repo.search_messages(SearchParams())
    stored = next(m for m in messages if m.message_id == 101)
    assert stored.message_kind == "service"
    assert stored.service_action_raw == "MessageActionChatJoinedByLink"
    assert stored.service_action_semantic == "join"
    assert stored.service_action_payload_json == '{"inviter_id": 42}'
    assert stored.sender_kind == "user"


# insert_messages_batch tests


async def test_insert_messages_batch_empty(messages_repo):
    """Test batch insert with empty list."""
    count = await messages_repo.insert_messages_batch([])
    assert count == 0


async def test_insert_messages_batch_multiple(messages_repo):
    """Test batch inserting multiple messages."""
    messages = [
        make_message(1, 100, "Message 1"),
        make_message(1, 101, "Message 2"),
        make_message(2, 100, "Message 3"),
    ]
    count = await messages_repo.insert_messages_batch(messages)
    assert count == 3

    _, total = await messages_repo.search_messages(SearchParams())
    assert total == 3


async def test_insert_messages_batch_with_duplicates(messages_repo):
    """Test batch insert ignores duplicates."""
    # Insert first batch
    await messages_repo.insert_messages_batch([make_message(1, 100, "Original")])

    # Insert batch with duplicate
    messages = [
        make_message(1, 100, "Duplicate"),  # This should be ignored
        make_message(1, 101, "New"),
    ]
    await messages_repo.insert_messages_batch(messages)

    messages_list, total = await messages_repo.search_messages(SearchParams())
    assert total == 2
    texts = {m.text for m in messages_list}
    assert "Original" in texts
    assert "New" in texts


async def test_insert_messages_batch_refreshes_duplicate_stats(messages_repo):
    await messages_repo.insert_messages_batch(
        [
            make_message(
                1,
                100,
                "Original",
                views=10,
                forwards=1,
                reply_count=2,
                reactions_json='[{"emoji": "👍", "count": 1}]',
                detected_lang="ru",
            )
        ]
    )
    rows = await messages_repo._db.execute(
        "SELECT id FROM messages WHERE channel_id = ? AND message_id = ?",
        (1, 100),
    )
    message_db_id = (await rows.fetchone())["id"]
    await messages_repo.update_translation(message_db_id, "en", "Cached translation")

    count = await messages_repo.insert_messages_batch(
        [
            make_message(
                1,
                100,
                "Duplicate text should not replace original",
                views=25,
                forwards=3,
                reply_count=4,
                reactions_json='[{"emoji": "👍", "count": 9}]',
                detected_lang="uk",
            )
        ]
    )

    assert count == 0
    messages, total = await messages_repo.search_messages(SearchParams())
    assert total == 1
    stored = messages[0]
    assert stored.text == "Original"
    assert stored.views == 25
    assert stored.forwards == 3
    assert stored.reply_count == 4
    assert stored.reactions_json == '[{"emoji": "👍", "count": 9}]'
    assert stored.detected_lang == "uk"
    assert stored.translation_en == "Cached translation"
    cur = await messages_repo._db.execute(
        "SELECT count FROM message_reactions WHERE channel_id = ? AND message_id = ? AND emoji = ?",
        (1, 100, "👍"),
    )
    reaction = await cur.fetchone()
    assert reaction["count"] == 9


async def test_insert_messages_batch_sender_identity_round_trips(messages_repo):
    messages = [
        make_message(
            1,
            110,
            "First",
            sender_id=1,
            sender_name="Alice Smith",
            sender_first_name="Alice",
            sender_last_name="Smith",
            sender_username="alice",
        ),
        make_message(
            1,
            111,
            "Second",
            sender_id=2,
            sender_name="Bob",
            sender_first_name="Bob",
            sender_username="@bob",
        ),
    ]
    assert await messages_repo.insert_messages_batch(messages) == 2

    stored, total = await messages_repo.search_messages(SearchParams())
    assert total == 2
    by_id = {message.message_id: message for message in stored}
    assert by_id[110].sender_first_name == "Alice"
    assert by_id[110].sender_last_name == "Smith"
    assert by_id[110].sender_username == "alice"
    assert by_id[111].sender_first_name == "Bob"
    assert by_id[111].sender_last_name is None
    assert by_id[111].sender_username == "bob"


# _normalize_date_from tests


def test_normalize_date_from_none():
    """Test normalizing None date_from."""
    result = MessagesRepository._normalize_date_from(None)
    assert result is None


def test_normalize_date_from_value():
    """Test normalizing date_from with value."""
    result = MessagesRepository._normalize_date_from("2026-03-16")
    assert result == "2026-03-16"


# _normalize_date_to tests


def test_normalize_date_to_none():
    """Test normalizing None date_to."""
    result, op = MessagesRepository._normalize_date_to(None)
    assert result is None
    assert op == "<="


def test_normalize_date_to_datetime():
    """Test normalizing date_to with datetime format."""
    result, op = MessagesRepository._normalize_date_to("2026-03-16T12:00:00")
    assert result == "2026-03-16T12:00:00"
    assert op == "<="


def test_normalize_date_to_date_only():
    """Test normalizing date_to with date-only format."""
    result, op = MessagesRepository._normalize_date_to("2026-03-16")
    assert result == "2026-03-17"  # Next day for inclusive filter
    assert op == "<"


def test_normalize_date_to_module_function():
    """Test the module-level _normalize_date_to function."""
    # Module function returns (operator, value), unlike the method
    op, result = _normalize_date_to("2026-03-16")
    assert result == "2026-03-17"  # Next day for inclusive filter
    assert op == "<"


# search_messages tests


async def test_search_messages_empty(messages_repo):
    """Test searching when no messages exist."""
    messages, total = await messages_repo.search_messages(SearchParams())
    assert messages == []
    assert total == 0


async def test_search_messages_all(messages_repo):
    """Test getting all messages."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "First"),
            make_message(1, 101, "Second"),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams())
    assert len(messages) == 2
    assert total == 2


async def test_search_messages_by_channel(messages_repo):
    """Test filtering by channel_id."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Channel 1"),
            make_message(2, 100, "Channel 2"),
            make_message(1, 101, "Channel 1 again"),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(channel_id=1))
    assert len(messages) == 2
    assert total == 2
    assert all(m.channel_id == 1 for m in messages)


async def test_search_messages_by_topic(messages_repo):
    """Test filtering by topic_id."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "No topic", topic_id=None),
            make_message(1, 101, "Topic 5", topic_id=5),
            make_message(1, 102, "Topic 6", topic_id=6),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(topic_id=5))
    assert len(messages) == 1
    assert total == 1
    assert messages[0].topic_id == 5


async def test_search_messages_by_date_from(messages_repo):
    """Test filtering by date_from."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Old", date=datetime(2026, 3, 10, tzinfo=timezone.utc)),
            make_message(1, 101, "New", date=datetime(2026, 3, 16, tzinfo=timezone.utc)),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(date_from="2026-03-15"))
    assert len(messages) == 1
    assert messages[0].text == "New"


async def test_search_messages_by_date_to(messages_repo):
    """Test filtering by date_to (inclusive)."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Old", date=datetime(2026, 3, 10, tzinfo=timezone.utc)),
            make_message(1, 101, "New", date=datetime(2026, 3, 16, tzinfo=timezone.utc)),
        ]
    )

    # Should include messages up to 2026-03-10
    messages, total = await messages_repo.search_messages(SearchParams(date_to="2026-03-10"))
    assert len(messages) == 1
    assert messages[0].text == "Old"


async def test_search_messages_by_date_range(messages_repo):
    """Test filtering by date range."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Before", date=datetime(2026, 3, 5, tzinfo=timezone.utc)),
            make_message(1, 101, "In range", date=datetime(2026, 3, 10, tzinfo=timezone.utc)),
            make_message(1, 102, "After", date=datetime(2026, 3, 20, tzinfo=timezone.utc)),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(date_from="2026-03-08", date_to="2026-03-15"))
    assert len(messages) == 1
    assert messages[0].text == "In range"


async def test_search_messages_by_min_length(messages_repo):
    """Test filtering by min_length."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Short"),
            make_message(1, 101, "This is a longer message"),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(min_length=10))
    assert len(messages) == 1
    assert messages[0].text == "This is a longer message"


async def test_search_messages_by_max_length(messages_repo):
    """Test filtering by max_length."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Short"),
            make_message(1, 101, "This is a longer message"),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(max_length=10))
    assert len(messages) == 1
    assert messages[0].text == "Short"


async def test_search_messages_pagination(messages_repo):
    """Test pagination with limit and offset."""
    for i in range(10):
        await messages_repo.insert_message(make_message(1, 100 + i, f"Message {i}"))

    # First page: total is a lower bound (offset + len) since #766, has_more
    # signals the next page instead of an exact COUNT.
    page = await messages_repo.search_messages(SearchParams(limit=3, offset=0))
    messages = page.messages
    assert len(messages) == 3
    assert page.total == 3
    assert page.has_more is True

    # Second page
    messages2, _ = await messages_repo.search_messages(SearchParams(limit=3, offset=3))
    assert len(messages2) == 3

    # Verify different messages
    ids1 = {m.message_id for m in messages}
    ids2 = {m.message_id for m in messages2}
    assert ids1.isdisjoint(ids2)


async def test_search_messages_excludes_filtered_channels(messages_repo, channels_repo):
    """Test that messages from filtered channels are excluded."""
    from src.models import Channel

    await channels_repo.add_channel(Channel(channel_id=1, title="Unfiltered"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Filtered"))

    # Mark channel 2 as filtered
    channels = await channels_repo.get_channels()
    filtered_pk = next(c.id for c in channels if c.channel_id == 2)
    await channels_repo.set_channel_filtered(filtered_pk, True)

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "From unfiltered"),
            make_message(2, 100, "From filtered"),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams())
    assert total == 1
    assert messages[0].channel_id == 1


async def test_search_messages_fts(messages_repo):
    """Test FTS search."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),
            make_message(1, 101, "Goodbye universe"),
            make_message(1, 102, "Hello again"),
        ]
    )

    messages, total = await messages_repo.search_messages(SearchParams(query="hello", is_fts=True))
    assert total == 2
    assert all("hello" in m.text.lower() for m in messages)


async def test_search_messages_plain_search(messages_repo):
    """Test plain text search (non-FTS)."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),
            make_message(1, 101, "Goodbye universe"),
        ]
    )

    # Plain search should still work via FTS with quoting
    messages, total = await messages_repo.search_messages(SearchParams(query="Hello", is_fts=False))
    assert total == 1


# _build_fts_match tests


def test_build_fts_match_fts_mode():
    """Test FTS match building in FTS mode."""
    result = MessagesRepository._build_fts_match("test query", is_fts=True)
    assert result == "test query"


def test_build_fts_match_plain_mode():
    """Test FTS match building in plain mode."""
    result = MessagesRepository._build_fts_match("test query", is_fts=False)
    assert result == '"test query"'


def test_build_fts_match_escapes_quotes():
    """Test that quotes are escaped in plain mode."""
    result = MessagesRepository._build_fts_match('test "quoted" query', is_fts=False)
    assert result == '"test ""quoted"" query"'


# count_fts_matches_for_query tests


async def test_count_fts_matches_for_query(messages_repo):
    """Test counting FTS matches for a search query."""
    sq = SearchQuery(query="hello", is_fts=True)

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),
            make_message(1, 101, "Hello there"),
            make_message(1, 102, "Goodbye"),
        ]
    )

    count = await messages_repo.count_fts_matches_for_query(sq)
    assert count == 2


async def test_count_fts_matches_for_query_with_max_length(messages_repo):
    """Test counting FTS matches with max_length filter."""
    sq = SearchQuery(query="hello", is_fts=True, max_length=15)

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),  # 11 chars
            make_message(1, 101, "Hello there, this is a very long message"),  # Too long
        ]
    )

    count = await messages_repo.count_fts_matches_for_query(sq)
    assert count == 1


async def test_count_fts_matches_for_query_with_exclude_patterns(messages_repo):
    """Test counting FTS matches with exclude patterns."""
    sq = SearchQuery(query="hello", is_fts=True, exclude_patterns="spam")

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),
            make_message(1, 101, "Hello spam message"),
        ]
    )

    count = await messages_repo.count_fts_matches_for_query(sq)
    assert count == 1


async def test_count_fts_matches_for_query_with_chat_filter(messages_repo):
    """Test counting FTS matches restricted to selected chats."""
    sq = SearchQuery(query="hello", is_fts=True, chat_filter="1")

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),
            make_message(2, 101, "Hello there"),
        ]
    )

    count = await messages_repo.count_fts_matches_for_query(sq)
    assert count == 1


# get_fts_daily_stats_for_query tests


async def test_get_fts_daily_stats_for_query(messages_repo):
    """Test getting daily FTS stats for a query."""
    sq = SearchQuery(query="test", is_fts=True)

    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "test message", date=today),
            make_message(1, 101, "test again", date=today),
            make_message(1, 102, "test yesterday", date=yesterday),
        ]
    )

    stats = await messages_repo.get_fts_daily_stats_for_query(sq, days=7)
    assert len(stats) >= 1
    assert all(hasattr(s, "day") and hasattr(s, "count") for s in stats)


# get_fts_daily_stats_batch tests


async def test_get_fts_daily_stats_batch(messages_repo):
    """Test batch FTS daily stats."""
    sq1 = SearchQuery(id=1, query="hello", is_fts=True)
    sq2 = SearchQuery(id=2, query="world", is_fts=True)

    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Hello world"),
            make_message(1, 101, "Hello again"),
        ]
    )

    result = await messages_repo.get_fts_daily_stats_batch([sq1, sq2], days=7)

    assert 1 in result
    assert 2 in result


async def test_get_fts_daily_stats_batch_with_chat_filter(messages_repo):
    """Test batch FTS daily stats respects chat filters per query."""
    sq1 = SearchQuery(id=1, query="hello", is_fts=True, chat_filter="10")
    sq2 = SearchQuery(id=2, query="hello", is_fts=True, chat_filter="20")

    await messages_repo.insert_messages_batch(
        [
            make_message(10, 100, "Hello one"),
            make_message(20, 101, "Hello two"),
        ]
    )

    result = await messages_repo.get_fts_daily_stats_batch([sq1, sq2], days=7)

    assert sum(s.count for s in result[1]) == 1
    assert sum(s.count for s in result[2]) == 1


async def test_get_fts_daily_stats_batch_empty(messages_repo):
    """Test batch FTS stats with empty list."""
    result = await messages_repo.get_fts_daily_stats_batch([], days=7)
    assert result == {}


# delete_messages_for_channel tests


async def test_delete_messages_for_channel(messages_repo):
    """Test deleting all messages for a channel."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Channel 1"),
            make_message(1, 101, "Channel 1"),
            make_message(2, 100, "Channel 2"),
        ]
    )

    count = await messages_repo.delete_messages_for_channel(1)
    assert count == 2

    _, total = await messages_repo.search_messages(SearchParams())
    assert total == 1


async def test_delete_messages_for_channel_nonexistent(messages_repo):
    """Test deleting messages for non-existent channel."""
    count = await messages_repo.delete_messages_for_channel(999)
    assert count == 0


# get_stats tests


async def test_get_stats(messages_repo, channels_repo):
    """Test getting database stats."""
    from src.database.repositories.accounts import AccountsRepository
    from src.database.repositories.search_queries import SearchQueriesRepository
    from src.models import Account, Channel

    accounts_repo = AccountsRepository(messages_repo._db, database=messages_repo._database)
    queries_repo = SearchQueriesRepository(messages_repo._db, database=messages_repo._database)

    # Add some data
    await accounts_repo.add_account(Account(phone="+123", session_string="s1"))
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await messages_repo.insert_message(make_message(1, 100, "Test"))
    await queries_repo.add(SearchQuery(query="test"))

    stats = await messages_repo.get_stats()
    assert stats["accounts"] == 1
    assert stats["channels"] == 1
    assert stats["messages"] == 1
    assert stats["search_queries"] == 1


async def test_get_stats_empty(messages_repo):
    """Test getting stats from empty database."""
    stats = await messages_repo.get_stats()
    assert stats["accounts"] == 0
    assert stats["channels"] == 0
    assert stats["messages"] == 0
    assert stats["search_queries"] == 0


# get_recent_for_channels tests
# Regression: join must be m.channel_id = c.channel_id, NOT c.id.
# Using c.id would silently return 0 for any channel whose DB pk differs
# from its Telegram channel_id — which broke react pipelines dry-run-count.


async def test_get_recent_for_channels_joins_on_channel_id_not_pk(
    messages_repo, channels_repo
):
    """Ensure join uses Telegram channel_id — would fail if joined on c.id."""
    from src.models import Channel

    # Seed a few dummy channels first so the target channel gets pk > 1,
    # making the bug deterministic (pk 3 ≠ channel_id 2694960513).
    await channels_repo.add_channel(Channel(channel_id=1001, title="Dummy 1"))
    await channels_repo.add_channel(Channel(channel_id=1002, title="Dummy 2"))
    target_pk = await channels_repo.add_channel(
        Channel(channel_id=2694960513, title="Claude Code Community")
    )
    assert target_pk != 2694960513  # pk ≠ channel_id → the bug's trigger

    now = datetime.now(timezone.utc)
    await messages_repo.insert_message(
        make_message(channel_id=2694960513, message_id=1, text="fresh", date=now)
    )
    await messages_repo.insert_message(
        make_message(
            channel_id=2694960513,
            message_id=2,
            text="old",
            date=now - timedelta(hours=48),
        )
    )

    rows = await messages_repo.get_recent_for_channels([2694960513], since_hours=24)
    assert len(rows) == 1
    assert rows[0].text == "fresh"


async def test_get_recent_for_channels_empty_ids(messages_repo):
    """Empty channel_ids returns [] without hitting the DB."""
    assert await messages_repo.get_recent_for_channels([], since_hours=24) == []


# premium_search_query tag + cleanup tests


async def _premium_tag(messages_repo, channel_id: int, message_id: int) -> str | None:
    cur = await messages_repo._db.execute(
        "SELECT premium_search_query FROM messages WHERE channel_id = ? AND message_id = ?",
        (channel_id, message_id),
    )
    row = await cur.fetchone()
    return row[0] if row else None


async def test_insert_batch_tags_new_rows_with_premium_search_query(messages_repo):
    """Premium search tags freshly inserted rows with the query string."""
    msgs = [make_message(10, 1, "a"), make_message(10, 2, "b")]
    inserted = await messages_repo.insert_messages_batch(msgs, premium_search_query="тест")
    assert inserted == 2
    assert await _premium_tag(messages_repo, 10, 1) == "тест"
    assert await _premium_tag(messages_repo, 10, 2) == "тест"


async def test_insert_batch_without_query_leaves_tag_null(messages_repo):
    """A normal batch insert (no query) leaves premium_search_query NULL."""
    await messages_repo.insert_messages_batch([make_message(11, 1, "a")])
    assert await _premium_tag(messages_repo, 11, 1) is None


async def test_premium_tag_not_applied_to_pre_existing_messages(messages_repo):
    """INSERT OR IGNORE skips existing rows, so a later search never tags user data."""
    # Pre-existing user message (e.g. collected by the worker), no tag.
    await messages_repo.insert_message(make_message(12, 1, "collected by user"))
    assert await _premium_tag(messages_repo, 12, 1) is None

    # A Premium search returns the same message_id; it must NOT overwrite the row.
    inserted = await messages_repo.insert_messages_batch(
        [make_message(12, 1, "from search"), make_message(12, 2, "new from search")],
        premium_search_query="тест",
    )
    assert inserted == 1  # only the genuinely new row was inserted
    assert await _premium_tag(messages_repo, 12, 1) is None  # user row untouched
    assert await _premium_tag(messages_repo, 12, 2) == "тест"  # new row tagged


async def test_normal_collection_clears_stale_premium_tag(messages_repo):
    """A cache row becomes user data once normal collection sees the same message."""
    await messages_repo.insert_messages_batch(
        [make_message(12, 3, "from premium search")],
        premium_search_query="тест",
    )
    assert await _premium_tag(messages_repo, 12, 3) == "тест"

    inserted = await messages_repo.insert_messages_batch(
        [make_message(12, 3, "collected by worker")]
    )
    assert inserted == 0
    assert await _premium_tag(messages_repo, 12, 3) is None

    assert await messages_repo.delete_premium_search_results("тест") == 0
    cur = await messages_repo._db.execute(
        "SELECT text FROM messages WHERE channel_id = ? AND message_id = ?",
        (12, 3),
    )
    row = await cur.fetchone()
    assert row is not None


async def test_delete_premium_search_results_removes_only_tagged(messages_repo):
    """Cleanup deletes only rows tagged with the query, leaving everything else."""
    await messages_repo.insert_message(make_message(13, 1, "user data with тест inside"))
    await messages_repo.insert_messages_batch(
        [make_message(13, 2, "search hit"), make_message(14, 1, "search hit 2")],
        premium_search_query="тест",
    )
    await messages_repo.insert_messages_batch(
        [make_message(15, 1, "other search")], premium_search_query="другое"
    )

    deleted = await messages_repo.delete_premium_search_results("тест")
    assert deleted == 2

    # Tagged-by-"тест" rows gone; user row and the "другое" row survive.
    assert await _premium_tag(messages_repo, 13, 2) is None
    assert await _premium_tag(messages_repo, 14, 1) is None
    cur = await messages_repo._db.execute("SELECT channel_id, message_id FROM messages ORDER BY channel_id, message_id")
    remaining = {(r[0], r[1]) for r in await cur.fetchall()}
    assert remaining == {(13, 1), (15, 1)}


async def test_delete_premium_search_results_no_match_returns_zero(messages_repo):
    """Purging an unknown query deletes nothing and reports zero."""
    await messages_repo.insert_message(make_message(16, 1, "untagged"))
    assert await messages_repo.delete_premium_search_results("nothing") == 0
    cur = await messages_repo._db.execute("SELECT COUNT(*) FROM messages")
    assert (await cur.fetchone())[0] == 1


# search_messages page contract (#766): no exact COUNT, LIMIT N+1 / has_more


async def test_search_messages_has_more_true_when_results_exceed_limit(messages_repo):
    """LIMIT N+1 probe: more rows than the limit → has_more=True, total is a lower bound."""
    for i in range(3):
        await messages_repo.insert_message(make_message(1, 100 + i, f"hello {i}"))

    page = await messages_repo.search_messages(SearchParams(limit=2))

    assert len(page.messages) == 2
    assert page.has_more is True
    assert page.total == 2  # offset + len(messages): lower bound, not an exact COUNT


async def test_search_messages_has_more_false_on_last_page(messages_repo):
    """On the last page total == offset + len(messages) is exact."""
    for i in range(3):
        await messages_repo.insert_message(make_message(1, 100 + i, f"hello {i}"))

    page = await messages_repo.search_messages(SearchParams(limit=2, offset=2))

    assert len(page.messages) == 1
    assert page.has_more is False
    assert page.total == 3


async def test_search_messages_tuple_unpacking_still_works(messages_repo):
    """Legacy `messages, total = ...` unpacking must keep working (#766)."""
    await messages_repo.insert_message(make_message(1, 100, "hello"))

    messages, total = await messages_repo.search_messages(SearchParams())

    assert len(messages) == 1
    assert total == 1


async def test_search_messages_does_not_execute_count(messages_repo, monkeypatch):
    """The expensive COUNT(*) is gone from all three branches (#766):
    FTS, LIKE fallback and the empty-query browse."""
    for i in range(3):
        await messages_repo.insert_message(make_message(1, 100 + i, f"hello {i}"))

    executed: list[str] = []
    orig_execute = messages_repo._db.execute

    async def spy(sql, *args, **kwargs):
        executed.append(sql)
        return await orig_execute(sql, *args, **kwargs)

    monkeypatch.setattr(messages_repo._db, "execute", spy)

    if messages_repo._fts_available:
        await messages_repo.search_messages(SearchParams(query="hello", limit=2))
    monkeypatch.setattr(messages_repo, "_fts_available", False)
    await messages_repo.search_messages(SearchParams(query="hello", limit=2))
    await messages_repo.search_messages(SearchParams(limit=2))

    counts = [sql for sql in executed if "count(" in sql.lower()]
    assert not counts, f"COUNT query still executed: {counts}"
