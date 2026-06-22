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
    chat_filter: str = "",
) -> SearchQuery:
    return SearchQuery(
        id=sq_id,
        query=query,
        is_regex=is_regex,
        is_fts=is_fts,
        exclude_patterns=exclude_patterns,
        max_length=max_length,
        chat_filter=chat_filter,
    )


# === match_and_notify tests ===


@pytest.mark.anyio
async def test_match_and_notify_empty_messages():
    """Empty message list returns {}."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    result = await matcher.match_and_notify([], [make_query("test")])

    assert result == {}
    notifier.notify.assert_not_called()


@pytest.mark.anyio
async def test_match_and_notify_empty_queries():
    """Empty query list returns {}."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    result = await matcher.match_and_notify([make_message("test")], [])

    assert result == {}
    notifier.notify.assert_not_called()


@pytest.mark.anyio
async def test_match_and_notify_plain_text_match():
    """Plain text query matches message text."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("Hello world example")]
    queries = [make_query("world")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}
    notifier.notify.assert_awaited_once()


@pytest.mark.anyio
async def test_match_and_notify_case_insensitive():
    """Matching is case-insensitive."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("HELLO WORLD")]
    queries = [make_query("hello")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.anyio
async def test_match_and_notify_regex_query():
    """Regex query matches via re.search."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("Order #12345 confirmed")]
    queries = [make_query(r"order #\d+", is_regex=True)]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.anyio
async def test_match_and_notify_regex_error_falls_through():
    """Invalid regex in query does not crash."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("some text")]
    # Invalid regex pattern
    queries = [make_query(r"[invalid(", is_regex=True)]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {}  # No match due to regex error


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_match_and_notify_respects_chat_filter():
    """Chat filters limit live notification matches."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [
        make_message("hello one", channel_id=1, message_id=1),
        make_message("hello two", channel_id=2, message_id=2),
    ]
    queries = [make_query("hello", chat_filter="2")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.anyio
async def test_match_and_notify_tme_s_link_chat_filter():
    """t.me/s/{username} chat filters match the referenced channel."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [
        make_message("hello one", channel_id=1, message_id=1, channel_username="public_chat"),
        make_message("hello two", channel_id=2, message_id=2, channel_username="other_chat"),
    ]
    queries = [make_query("hello", chat_filter="https://t.me/s/public_chat/123")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.anyio
async def test_match_and_notify_unknown_chat_filter_matches_nothing():
    """A non-empty unknown chat filter must not fall back to all chats."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("hello one", channel_id=1, message_id=1)]
    queries = [make_query("hello", chat_filter="missing_chat")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {}


@pytest.mark.anyio
async def test_match_and_notify_message_no_text():
    """Messages with None text are skipped."""
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)

    messages = [make_message("valid text"), Message(channel_id=-100, message_id=1, text=None, date="2024")]
    queries = [make_query("valid")]

    result = await matcher.match_and_notify(messages, queries)

    assert result == {1: 1}


@pytest.mark.anyio
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


@pytest.mark.anyio
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


def test_make_message_link_legacy_marked_id():
    """Telethon marked form -100<bare> is normalised to the bare id."""
    msg = make_message("test", channel_id=-1001234567890, message_id=789)
    link = _make_message_link(msg)

    # -1001234567890 -> bare_id = 1234567890 (strip -100 marker)
    assert link == "https://t.me/c/1234567890/789"


def test_make_message_link_bare_positive_id():
    """Bare-positive stored ids are used verbatim (storage convention)."""
    msg = make_message("test", channel_id=1234567890, message_id=789)
    link = _make_message_link(msg)

    assert link == "https://t.me/c/1234567890/789"


def test_make_message_link_bare_id_starting_with_100():
    """Regression #633-9: ids starting with 100 must NOT be truncated."""
    # 1005551782 is a real-shaped bare id; the old code stripped the leading
    # "100" and produced a broken https://t.me/c/5551782/... link.
    msg = make_message("test", channel_id=1005551782, message_id=789)
    link = _make_message_link(msg)

    assert link == "https://t.me/c/1005551782/789"


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


def test_fts_implicit_and_two_bare_terms():
    """Regression #971: bare space is FTS5 implicit-AND, not a phrase.

    Both words must be present (any position) — matching how the saved search
    actually executes via SQLite FTS5 — instead of being treated as one
    contiguous substring.
    """
    assert _fts_query_matches("apple banana", "apple here and banana there") is True
    assert _fts_query_matches("apple banana", "only apple here") is False


def test_fts_quoted_phrase_stays_contiguous():
    """A quoted phrase must still match contiguously, not as implicit-AND."""
    assert _fts_query_matches('"apple banana"', "an apple banana split") is True
    assert _fts_query_matches('"apple banana"', "apple here and banana there") is False


# === audit #838/1 dedup + retry, #836/12 display name ===


class _FakeNotifiedStore:
    def __init__(self):
        self.recorded: set[tuple[int, int, int]] = set()

    async def filter_unnotified(self, query_id, channel_id, message_ids):
        return {mid for mid in message_ids if (query_id, channel_id, mid) not in self.recorded}

    async def record(self, query_id, channel_id, message_ids):
        for mid in message_ids:
            self.recorded.add((query_id, channel_id, mid))

    async def has_any(self, channel_ids):
        cids = set(channel_ids)
        return any(cid in cids for (_q, cid, _m) in self.recorded)


@pytest.mark.anyio
async def test_dedup_skips_already_notified_message():
    notifier = AsyncMock()
    store = _FakeNotifiedStore()
    matcher = NotificationMatcher(notifier, notified_store=store)
    messages = [make_message("hello world", message_id=10)]
    queries = [make_query("world")]

    first = await matcher.match_and_notify(messages, queries)
    assert first == {1: 1}
    assert notifier.notify.await_count == 1

    # Same message on a later pass must NOT re-notify.
    second = await matcher.match_and_notify(messages, queries)
    assert second == {}
    assert notifier.notify.await_count == 1


@pytest.mark.anyio
async def test_failed_send_is_not_recorded_and_retries():
    notifier = AsyncMock()
    notifier.notify = AsyncMock(side_effect=[False, True])
    store = _FakeNotifiedStore()
    matcher = NotificationMatcher(notifier, notified_store=store)
    messages = [make_message("hello world", message_id=10)]
    queries = [make_query("world")]

    # First send fails -> not recorded, not counted.
    first = await matcher.match_and_notify(messages, queries)
    assert first == {}
    assert store.recorded == set()

    # Second pass re-presents the same message -> retried and now recorded.
    second = await matcher.match_and_notify(messages, queries)
    assert second == {1: 1}
    assert (1, -1001234567890, 10) in store.recorded


@pytest.mark.anyio
async def test_notification_uses_query_name_not_raw_query():
    notifier = AsyncMock()
    matcher = NotificationMatcher(notifier)
    sq = SearchQuery(id=1, query="прода(ю|жа)", name="Лиды на продажу", is_regex=True)
    messages = [make_message("продаю квартиру")]

    await matcher.match_and_notify(messages, [sq])

    sent_text = notifier.notify.await_args.args[0]
    assert "Лиды на продажу" in sent_text
    assert "прода(ю|жа)" not in sent_text


# === audit #838/3: dry-run uses production predicate, not FTS ===


def test_dry_run_matches_regex_query():
    from src.services.notification_matcher import dry_run_matches

    msgs = [
        make_message("продаю квартиру срочно", message_id=1),
        make_message("просто текст", message_id=2),
    ]
    regex_q = SearchQuery(id=1, query="прода(ю|жа)", is_regex=True)
    matched, total = dry_run_matches(msgs, regex_q)
    assert total == 1
    assert matched[0].message_id == 1


def test_dry_run_matches_partial_substring():
    from src.services.notification_matcher import dry_run_matches

    msgs = [
        make_message("теплоход отправляется", message_id=1),
        make_message("самолёт", message_id=2),
    ]
    # "теплох" is a partial substring (not a whole FTS token) — must still match.
    partial_q = SearchQuery(id=2, query="теплох")
    matched, total = dry_run_matches(msgs, partial_q)
    assert total == 1
    assert matched[0].message_id == 1
