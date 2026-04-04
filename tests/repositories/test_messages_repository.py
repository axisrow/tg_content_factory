"""Tests for MessagesRepository."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.messages import MessagesRepository, _normalize_date_to
from src.models import Message, SearchQuery


@pytest.fixture
async def channels_repo(db):
    """Create channels repository instance."""
    return ChannelsRepository(db.db)


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
    messages, total = await messages_repo.search_messages()
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
        date=datetime(2026, 3, 16, 12, 0, 0),
    )
    result = await messages_repo.insert_message(msg)
    assert result is True

    messages, _ = await messages_repo.search_messages()
    assert messages[0].sender_id == 12345
    assert messages[0].sender_name == "John Doe"
    assert messages[0].media_type == "photo"
    assert messages[0].topic_id == 5


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

    _, total = await messages_repo.search_messages()
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

    messages_list, total = await messages_repo.search_messages()
    assert total == 2
    texts = {m.text for m in messages_list}
    assert "Original" in texts
    assert "New" in texts


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
    messages, total = await messages_repo.search_messages()
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

    messages, total = await messages_repo.search_messages()
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

    messages, total = await messages_repo.search_messages(channel_id=1)
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

    messages, total = await messages_repo.search_messages(topic_id=5)
    assert len(messages) == 1
    assert total == 1
    assert messages[0].topic_id == 5


async def test_search_messages_by_date_from(messages_repo):
    """Test filtering by date_from."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Old", date=datetime(2026, 3, 10)),
            make_message(1, 101, "New", date=datetime(2026, 3, 16)),
        ]
    )

    messages, total = await messages_repo.search_messages(date_from="2026-03-15")
    assert len(messages) == 1
    assert messages[0].text == "New"


async def test_search_messages_by_date_to(messages_repo):
    """Test filtering by date_to (inclusive)."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Old", date=datetime(2026, 3, 10)),
            make_message(1, 101, "New", date=datetime(2026, 3, 16)),
        ]
    )

    # Should include messages up to 2026-03-10
    messages, total = await messages_repo.search_messages(date_to="2026-03-10")
    assert len(messages) == 1
    assert messages[0].text == "Old"


async def test_search_messages_by_date_range(messages_repo):
    """Test filtering by date range."""
    await messages_repo.insert_messages_batch(
        [
            make_message(1, 100, "Before", date=datetime(2026, 3, 5)),
            make_message(1, 101, "In range", date=datetime(2026, 3, 10)),
            make_message(1, 102, "After", date=datetime(2026, 3, 20)),
        ]
    )

    messages, total = await messages_repo.search_messages(date_from="2026-03-08", date_to="2026-03-15")
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

    messages, total = await messages_repo.search_messages(min_length=10)
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

    messages, total = await messages_repo.search_messages(max_length=10)
    assert len(messages) == 1
    assert messages[0].text == "Short"


async def test_search_messages_pagination(messages_repo):
    """Test pagination with limit and offset."""
    for i in range(10):
        await messages_repo.insert_message(make_message(1, 100 + i, f"Message {i}"))

    # First page
    messages, total = await messages_repo.search_messages(limit=3, offset=0)
    assert len(messages) == 3
    assert total == 10

    # Second page
    messages2, _ = await messages_repo.search_messages(limit=3, offset=3)
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

    messages, total = await messages_repo.search_messages()
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

    messages, total = await messages_repo.search_messages(query="hello", is_fts=True)
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
    messages, total = await messages_repo.search_messages(query="Hello", is_fts=False)
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

    _, total = await messages_repo.search_messages()
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

    accounts_repo = AccountsRepository(messages_repo._db)
    queries_repo = SearchQueriesRepository(messages_repo._db)

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
