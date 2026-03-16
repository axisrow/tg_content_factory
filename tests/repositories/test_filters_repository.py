"""Tests for FilterRepository."""
from __future__ import annotations

import pytest

from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.filters import FilterRepository, _has_cyrillic_udf
from src.models import Channel

_INSERT_MSG = (
    "INSERT INTO messages (channel_id, message_id, text, date)"
    " VALUES (?, ?, ?, datetime('now'))"
)
_INSERT_MSG_NO_TEXT = (
    "INSERT INTO messages (channel_id, message_id, date)"
    " VALUES (?, ?, datetime('now'))"
)
_INSERT_STATS = (
    "INSERT INTO channel_stats (channel_id, subscriber_count, collected_at)"
    " VALUES (?, ?, datetime('now'))"
)
_INSERT_STATS_TS = (
    "INSERT INTO channel_stats (channel_id, subscriber_count, collected_at)"
    " VALUES (?, ?, ?)"
)
_INSERT_STATS_NULL = (
    "INSERT INTO channel_stats (channel_id, subscriber_count, collected_at)"
    " VALUES (?, NULL, datetime('now'))"
)


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return FilterRepository(db.db)


@pytest.fixture
async def channels_repo(db):
    """Create channels repository instance."""
    return ChannelsRepository(db.db)


# _has_cyrillic_udf tests

def test_has_cyrillic_udf_none():
    """Test UDF with None input."""
    result = _has_cyrillic_udf(None)
    assert result == 0


def test_has_cyrillic_udf_empty():
    """Test UDF with empty string."""
    result = _has_cyrillic_udf("")
    assert result == 0


def test_has_cyrillic_udf_latin():
    """Test UDF with Latin text."""
    result = _has_cyrillic_udf("Hello World")
    assert result == 0


def test_has_cyrillic_udf_cyrillic():
    """Test UDF with Cyrillic text."""
    result = _has_cyrillic_udf("Привет мир")
    assert result == 1


def test_has_cyrillic_udf_mixed():
    """Test UDF with mixed text."""
    result = _has_cyrillic_udf("Hello Привет")
    assert result == 1


def test_has_cyrillic_udf_yo():
    """Test UDF with yo (ё/Ё) characters."""
    result = _has_cyrillic_udf("Всё готово")
    assert result == 1


# fetch_channels_for_analysis tests

async def test_fetch_channels_for_analysis_empty(repo, channels_repo):
    """Test fetching when no channels exist."""
    result = await repo.fetch_channels_for_analysis()
    assert result == []


async def test_fetch_channels_for_analysis_basic(repo, channels_repo):
    """Test fetching channels for analysis."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Channel 1"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Channel 2"))

    result = await repo.fetch_channels_for_analysis()
    assert len(result) == 2
    assert result[0]["title"] == "Channel 1"
    assert result[1]["title"] == "Channel 2"


async def test_fetch_channels_for_analysis_with_messages(repo, channels_repo):
    """Test that message_count is included."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    # Insert messages
    await repo._db.executemany(
        _INSERT_MSG_NO_TEXT,
        [(1, 100), (1, 101), (1, 102)],
    )
    await repo._db.commit()

    result = await repo.fetch_channels_for_analysis()
    assert len(result) == 1
    assert result[0]["message_count"] == 3


async def test_fetch_channels_for_analysis_by_id(repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Channel 1"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Channel 2"))

    result = await repo.fetch_channels_for_analysis(channel_id=1)
    assert len(result) == 1
    assert result[0]["channel_id"] == 1


async def test_fetch_channels_for_analysis_nonexistent_id(repo, channels_repo):
    """Test filtering by non-existent channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Channel 1"))

    result = await repo.fetch_channels_for_analysis(channel_id=999)
    assert result == []


# fetch_uniqueness_map tests

async def test_fetch_uniqueness_map_empty(repo):
    """Test uniqueness map with no messages."""
    result = await repo.fetch_uniqueness_map()
    assert result == {}


async def test_fetch_uniqueness_map_basic(repo, channels_repo):
    """Test basic uniqueness calculation."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    # Insert messages with different prefixes
    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "First message here"),
            (1, 101, "Second message here"),  # Same 100-char prefix as 100
            (1, 102, "Different content"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_uniqueness_map()
    assert 1 in result
    total, unique = result[1]
    assert total == 3
    # "First message here" and "Second message here" share same 100-char prefix
    # "Different content" is different
    # All are under 100 chars, so uniqueness depends on exact content
    assert unique == 3


async def test_fetch_uniqueness_map_excludes_null_text(repo, channels_repo):
    """Test that NULL text is excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Valid text"),
            (1, 101, None),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_uniqueness_map()
    assert result[1][0] == 1  # Only 1 message counted


async def test_fetch_uniqueness_map_by_channel(repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [(1, 100, "Message from channel 1"), (2, 100, "Message from channel 2")],
    )
    await repo._db.commit()

    result = await repo.fetch_uniqueness_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


# fetch_subscriber_map tests

async def test_fetch_subscriber_map_empty(repo):
    """Test subscriber map with no stats."""
    result = await repo.fetch_subscriber_map()
    assert result == {}


async def test_fetch_subscriber_map_basic(repo, channels_repo):
    """Test basic subscriber map."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.execute(
        _INSERT_STATS,
        (1, 1000),
    )
    await repo._db.commit()

    result = await repo.fetch_subscriber_map()
    assert result[1] == 1000


async def test_fetch_subscriber_map_latest_only(repo, channels_repo):
    """Test that only the latest subscriber count is returned."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    # Insert multiple stats with different timestamps
    await repo._db.executemany(
        _INSERT_STATS_TS,
        [
            (1, 1000, "2026-03-10 12:00:00"),
            (1, 1500, "2026-03-15 12:00:00"),
            (1, 1200, "2026-03-12 12:00:00"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_subscriber_map()
    assert result[1] == 1500  # Latest


async def test_fetch_subscriber_map_excludes_null(repo, channels_repo):
    """Test that NULL subscriber_count is excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.execute(
        _INSERT_STATS_NULL,
        (1,),
    )
    await repo._db.commit()

    result = await repo.fetch_subscriber_map()
    assert 1 not in result


async def test_fetch_subscriber_map_by_channel(repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_STATS,
        [(1, 1000), (2, 2000)],
    )
    await repo._db.commit()

    result = await repo.fetch_subscriber_map(channel_id=1)
    assert result == {1: 1000}


# fetch_short_message_map tests

async def test_fetch_short_message_map_empty(repo):
    """Test short message map with no messages."""
    result = await repo.fetch_short_message_map()
    assert result == {}


async def test_fetch_short_message_map_basic(repo, channels_repo):
    """Test basic short message counting."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Short"),      # 5 chars <= 10
            (1, 101, "Long message that is definitely more than ten characters"),
            (1, 102, "Tiny"),       # 4 chars <= 10
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_short_message_map()
    total, short = result[1]
    assert total == 3
    assert short == 2


async def test_fetch_short_message_map_null_text(repo, channels_repo):
    """Test handling of NULL text."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Short"),
            (1, 101, None),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_short_message_map()
    total, short = result[1]
    assert total == 2
    assert short == 1  # NULL is not counted as short


async def test_fetch_short_message_map_by_channel(repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [(1, 100, "Short"), (2, 100, "Short")],
    )
    await repo._db.commit()

    result = await repo.fetch_short_message_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


# count_matching_prefixes_in_other_channels tests

async def test_count_matching_prefixes_empty_list(repo):
    """Test with empty prefixes list."""
    result = await repo.count_matching_prefixes_in_other_channels(1, [])
    assert result == 0


async def test_count_matching_prefixes_no_matches(repo, channels_repo):
    """Test with no matching prefixes."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [(2, 100, "Completely different text")],
    )
    await repo._db.commit()

    result = await repo.count_matching_prefixes_in_other_channels(
        1, ["This is a specific prefix that won't match"]
    )
    assert result == 0


async def test_count_matching_prefixes_with_matches(repo, channels_repo):
    """Test with matching prefixes."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    prefix = "Same prefix message"
    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, prefix),
            (2, 100, prefix),  # Same prefix in other channel
        ],
    )
    await repo._db.commit()

    result = await repo.count_matching_prefixes_in_other_channels(1, [prefix])
    assert result == 1


async def test_count_matching_prefixes_multiple(repo, channels_repo):
    """Test with multiple matching prefixes."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (2, 100, "Prefix one message"),
            (2, 101, "Prefix two message"),
            (2, 102, "Prefix one message"),  # Duplicate prefix, counted once
        ],
    )
    await repo._db.commit()

    result = await repo.count_matching_prefixes_in_other_channels(
        1, ["Prefix one message", "Prefix two message"]
    )
    assert result == 2


# fetch_cross_dupe_map tests

async def test_fetch_cross_dupe_map_empty(repo):
    """Test cross dupe map with no messages."""
    result = await repo.fetch_cross_dupe_map()
    assert result == {}


async def test_fetch_cross_dupe_map_no_dupes(repo, channels_repo):
    """Test with no cross-channel duplicates."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Unique message one"),
            (1, 101, "Unique message two"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cross_dupe_map()
    total, duped = result[1]
    assert total == 2
    assert duped == 0


async def test_fetch_cross_dupe_map_with_dupes(repo, channels_repo):
    """Test with cross-channel duplicates."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    # Same message in both channels (longer than 10 chars)
    shared_text = "This is a shared message between channels"
    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, shared_text),
            (2, 100, shared_text),
            (1, 101, "Unique to channel 1"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cross_dupe_map()
    total, duped = result[1]
    assert total == 2  # 2 unique prefixes in channel 1
    assert duped == 1  # 1 is duplicated in another channel


async def test_fetch_cross_dupe_map_excludes_short(repo, channels_repo):
    """Test that short messages (<=10 chars) are excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Short"),  # <= 10 chars, excluded
            (2, 100, "Short"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cross_dupe_map()
    assert result == {}  # No messages > 10 chars


async def test_fetch_cross_dupe_map_by_channel(repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Long message from channel 1"),
            (2, 100, "Long message from channel 2"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cross_dupe_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


# fetch_cyrillic_map tests

async def test_fetch_cyrillic_map_empty(repo):
    """Test cyrillic map with no messages."""
    result = await repo.fetch_cyrillic_map()
    assert result == {}


async def test_fetch_cyrillic_map_basic(repo, channels_repo):
    """Test basic cyrillic counting."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "English text"),
            (1, 101, "Русский текст"),
            (1, 102, "Mixed text с русским"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cyrillic_map()
    total, cyr = result[1]
    assert total == 3
    assert cyr == 2  # 2 messages have cyrillic


async def test_fetch_cyrillic_map_excludes_null_empty(repo, channels_repo):
    """Test that NULL and empty text are excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Valid text"),
            (1, 101, None),
            (1, 102, ""),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cyrillic_map()
    total, cyr = result[1]
    assert total == 1  # Only non-null, non-empty counted


async def test_fetch_cyrillic_map_by_channel(repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Текст на русском"),
            (2, 100, "English text"),
        ],
    )
    await repo._db.commit()

    result = await repo.fetch_cyrillic_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


async def test_fetch_cyrillic_map_udf_registered(repo, channels_repo):
    """Test that UDF is registered on first call."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await repo._db.execute(
        _INSERT_MSG,
        (1, 100, "Test"),
    )
    await repo._db.commit()

    # First call should register UDF
    result1 = await repo.fetch_cyrillic_map()
    assert repo._udf_registered is True

    # Second call should use already registered UDF
    result2 = await repo.fetch_cyrillic_map()
    assert result1 == result2
