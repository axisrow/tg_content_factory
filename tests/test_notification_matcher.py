"""Tests for src/services/notification_matcher.py"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.models import Message, SearchQuery
from src.services.notification_matcher import (
    NotificationMatcher,
    _fts_query_matches,
    _make_message_link,
)


def make_message(
    text: str,
    channel_id: int = -1001234567890,
    message_id: int = 123,
    channel_username: str | None = None,
) -> Message:
    return Message(
        channel_id=channel_id,
        message_id=message_id,
        text=text,
        channel_username=channel_username,
        date="2024-01-01T00:00:00",
    )


def make_query(
    query: str,
    sq_id: int = 1,
    is_regex: bool = False,
    is_fts: bool = False,
    exclude_patterns: str = "",
    max_length: int | None = None,
) -> SearchQuery:
    return SearchQuery(
        id=sq_id,
        query=query,
        is_regex=is_regex,
        is_fts=is_fts,
        exclude_patterns=exclude_patterns,
        max_length=max_length,
    )


# === match_and_notify tests ===


@pytest.mark.asyncio
async def test_match_and_notify_empty_messages():
    """Empty message list returns {}."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    result = await matcher.match_and_notify([], [make_query("test")])

    assert result == {}
    notifier.notify.assert_not_called()


@pytest.mark.asyncio
async def test_match_and_notify_empty_queries():
    """Empty query list returns {}."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    result = await matcher.match_and_notify([make_message("test")], [])

    assert result == {}
    notifier.notify.assert_not_called()


@pytest.mark.asyncio
async def test_match_and_notify_plain_text_match():
    """Plain text query matches message text."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("Hello world example")]
    queries = [make_query("world")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}
    notifier.notify.assert_awaited_once()


@pytest.mark.asyncio
async def test_match_and_notify_case_insensitive():
    """Matching is case-insensitive."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("HELLO WORLD")]
    queries = [make_query("hello")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.asyncio
async def test_match_and_notify_regex_query():
    """Regex query matches via re.search."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("Order #12345 confirmed")]
    queries = [make_query(r"order #\d+", is_regex=True)]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.asyncio
async def test_match_and_notify_regex_error_falls_through():
    """Invalid regex in query does not crash."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("some text")]
    # Invalid regex pattern
    queries = [make_query(r"[invalid(", is_regex=True)]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {}  # No match due to regex error


@pytest.mark.asyncio
async def test_match_and_notify_fts_query_and():
    """FTS5 AND logic - both terms required."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    # Only message with both terms should match
    messages = [
        make_message("apple only"),
        make_message("banana only"),
        make_message("apple and banana together"),
    ]
    queries = [make_query("apple AND banana", is_fts=True)]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}  # Only one message matched


@pytest.mark.asyncio
async def test_match_and_notify_fts_query_or():
    """FTS5 OR logic - any term matches."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [
        make_message("apple fruit"),
        make_message("banana fruit"),
        make_message("orange fruit"),
    ]
    # Explicit OR between terms
    queries = [make_query("apple OR banana", is_fts=True)]

    result = await matcher.match_and_notify(messages, queries)

    # apple and banana should match (2 messages)
    assert result.get(1, 0) == 2


@pytest.mark.asyncio
async def test_match_and_notify_excludes_by_pattern():
    """Messages matching exclude patterns are skipped."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [
        make_message("hello spam world"),
        make_message("hello good world"),
    ]
    queries = [make_query("hello", exclude_patterns="spam")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}  # Only non-spam message


@pytest.mark.asyncio
async def test_match_and_notify_max_length_filter():
    """Messages at/above max_length are skipped."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    short_text = "hello world"
    long_text = "hello world " * 20

    messages = [
        make_message(short_text),
        make_message(long_text),
    ]
    queries = [make_query("hello", max_length=50)]

    result = await matcher.match_and_notify(messages, queries)

    # Only short message should match
    assert result == {1: 1}


@pytest.mark.asyncio
async def test_match_and_notify_message_no_text():
    """Messages with None text are skipped."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("valid text"), Message(channel_id=-100, message_id=1, text=None, date="2024")]
    queries = [make_query("valid")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.asyncio
async def test_match_and_notify_counts_multiple_matches():
    """One query matching multiple messages counts correctly."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [
        make_message("hello one", message_id=1),
        make_message("hello two", message_id=2),
        make_message("hello three", message_id=3),
    ]
    queries = [make_query("hello")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 3}
    # Should send notification with count
    call_args = notifier.notify.call_args[0][0]
    assert "3 times" in call_args


@pytest.mark.asyncio
async def test_match_and_notify_multiple_queries():
    """Multiple queries each produce separate counts."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [
        make_message("apple fruit"),
        make_message("banana fruit"),
    ]
    queries = [
        make_query("apple", sq_id=1),
        make_query("banana", sq_id=2),
    ]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1, 2: 1}


# === _make_message_link tests ===


def test_make_message_link_with_username():
    """t.me/{username}/{message_id} when channel_username is set."""
    msg = make_message("test", channel_username="mychannel", message_id=456)
    link = _make_message_link(msg)

    assert link == "https://t.me/mychannel/456"


def test_make_message_link_without_username():
    """t.me/c/{bare_id}/{message_id} when no channel_username."""
    msg = make_message("test", channel_id=-1001234567890, message_id=789)
    link = _make_message_link(msg)

    # -1001234567890 -> bare_id = 1234567890 (strip -100 prefix)
    assert link == "https://t.me/c/1234567890/789"


# === _fts_query_matches pure function tests ===


def test_fts_query_matches_single_term():
    """Single term matching."""
    assert _fts_query_matches("hello", "hello world") is True
    assert _fts_query_matches("goodbye", "hello world") is False


def test_fts_query_matches_and():
    """AND logic requires all parts."""
    assert _fts_query_matches("hello AND world", "hello world") is True
    assert _fts_query_matches("hello AND goodbye", "hello world") is False


def test_fts_query_matches_case_insensitive():
    """FTS matching is case-insensitive."""
    assert _fts_query_matches("HELLO", "hello world") is True
