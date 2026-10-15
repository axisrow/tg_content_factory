"""Tests for FilterRepository."""

from __future__ import annotations

import pytest

from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.filters import _has_cyrillic_udf
from src.models import Channel

_INSERT_MSG = (
    "INSERT INTO messages (channel_id, message_id, text, date)" " VALUES (?, ?, ?, datetime('now'))"
)
_INSERT_MSG_ENTITIES = (
    "INSERT INTO messages (channel_id, message_id, text, entities_json, date)"
    " VALUES (?, ?, ?, ?, datetime('now'))"
)
_INSERT_MSG_NO_TEXT = (
    "INSERT INTO messages (channel_id, message_id, date)" " VALUES (?, ?, datetime('now'))"
)
_INSERT_MSG_VIEWS = (
    "INSERT INTO messages (channel_id, message_id, text, views, date)"
    " VALUES (?, ?, ?, ?, datetime('now'))"
)
_INSERT_STATS = (
    "INSERT INTO channel_stats (channel_id, subscriber_count, collected_at)"
    " VALUES (?, ?, datetime('now'))"
)
_INSERT_STATS_TS = (
    "INSERT INTO channel_stats (channel_id, subscriber_count, collected_at)" " VALUES (?, ?, ?)"
)
_INSERT_STATS_NULL = (
    "INSERT INTO channel_stats (channel_id, subscriber_count, collected_at)"
    " VALUES (?, NULL, datetime('now'))"
)



@pytest.fixture
async def channels_repo(db):
    """Create channels repository instance."""
    return ChannelsRepository(db.db, database=db)


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


async def test_fetch_channels_for_analysis_empty(filters_repo, channels_repo):
    """Test fetching when no channels exist."""
    result = await filters_repo.fetch_channels_for_analysis()
    assert result == []


async def test_fetch_channels_for_analysis_basic(filters_repo, channels_repo):
    """Test fetching channels for analysis."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Channel 1"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Channel 2"))

    result = await filters_repo.fetch_channels_for_analysis()
    assert len(result) == 2
    assert result[0]["title"] == "Channel 1"
    assert result[1]["title"] == "Channel 2"


async def test_fetch_channels_for_analysis_with_messages(filters_repo, channels_repo):
    """Test that message_count is included."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    # Insert messages
    await filters_repo._db.executemany(
        _INSERT_MSG_NO_TEXT,
        [(1, 100), (1, 101), (1, 102)],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_channels_for_analysis()
    assert len(result) == 1
    assert result[0]["message_count"] == 3


async def test_fetch_channels_for_analysis_by_id(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Channel 1"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Channel 2"))

    result = await filters_repo.fetch_channels_for_analysis(channel_id=1)
    assert len(result) == 1
    assert result[0]["channel_id"] == 1


async def test_fetch_channels_for_analysis_nonexistent_id(filters_repo, channels_repo):
    """Test filtering by non-existent channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Channel 1"))

    result = await filters_repo.fetch_channels_for_analysis(channel_id=999)
    assert result == []


# fetch_uniqueness_map tests


async def test_fetch_uniqueness_map_empty(filters_repo):
    """Test uniqueness map with no messages."""
    result = await filters_repo.fetch_uniqueness_map()
    assert result == {}


async def test_fetch_uniqueness_map_basic(filters_repo, channels_repo):
    """Test basic uniqueness calculation."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    # Insert messages with different prefixes
    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "First message here"),
            (1, 101, "Second message here"),  # Same 100-char prefix as 100
            (1, 102, "Different content"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_uniqueness_map()
    assert 1 in result
    total, unique = result[1]
    assert total == 3
    # "First message here" and "Second message here" share same 100-char prefix
    # "Different content" is different
    # All are under 100 chars, so uniqueness depends on exact content
    assert unique == 3


async def test_fetch_uniqueness_map_excludes_null_text(filters_repo, channels_repo):
    """Test that NULL text is excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Valid text"),
            (1, 101, None),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_uniqueness_map()
    assert result[1][0] == 1  # Only 1 message counted


async def test_fetch_uniqueness_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [(1, 100, "Message from channel 1"), (2, 100, "Message from channel 2")],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_uniqueness_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


# fetch_subscriber_map tests


async def test_fetch_subscriber_map_empty(filters_repo):
    """Test subscriber map with no stats."""
    result = await filters_repo.fetch_subscriber_map()
    assert result == {}


async def test_fetch_subscriber_map_basic(filters_repo, channels_repo):
    """Test basic subscriber map."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.execute(
        _INSERT_STATS,
        (1, 1000),
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_subscriber_map()
    assert result[1] == 1000


async def test_fetch_subscriber_map_latest_only(filters_repo, channels_repo):
    """Test that only the latest subscriber count is returned."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    # Insert multiple stats with different timestamps
    await filters_repo._db.executemany(
        _INSERT_STATS_TS,
        [
            (1, 1000, "2026-03-10 12:00:00"),
            (1, 1500, "2026-03-15 12:00:00"),
            (1, 1200, "2026-03-12 12:00:00"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_subscriber_map()
    assert result[1] == 1500  # Latest


async def test_fetch_subscriber_map_excludes_null(filters_repo, channels_repo):
    """Test that NULL subscriber_count is excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.execute(
        _INSERT_STATS_NULL,
        (1,),
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_subscriber_map()
    assert 1 not in result


async def test_fetch_subscriber_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_STATS,
        [(1, 1000), (2, 2000)],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_subscriber_map(channel_id=1)
    assert result == {1: 1000}


# fetch_short_message_map tests


async def test_fetch_short_message_map_empty(filters_repo):
    """Test short message map with no messages."""
    result = await filters_repo.fetch_short_message_map()
    assert result == {}


async def test_fetch_short_message_map_basic(filters_repo, channels_repo):
    """Test basic short message counting."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Short"),  # 5 chars <= 10
            (1, 101, "Long message that is definitely more than ten characters"),
            (1, 102, "Tiny"),  # 4 chars <= 10
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_short_message_map()
    total, short = result[1]
    assert total == 3
    assert short == 2


async def test_fetch_short_message_map_null_text(filters_repo, channels_repo):
    """Test handling of NULL text."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Short"),
            (1, 101, None),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_short_message_map()
    total, short = result[1]
    assert total == 2
    assert short == 1  # NULL is not counted as short


async def test_fetch_short_message_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [(1, 100, "Short"), (2, 100, "Short")],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_short_message_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


# count_matching_prefixes_in_other_channels tests


async def test_count_matching_prefixes_empty_list(filters_repo):
    """Test with empty prefixes list."""
    result = await filters_repo.count_matching_prefixes_in_other_channels(1, [])
    assert result == 0


async def test_count_matching_prefixes_no_matches(filters_repo, channels_repo):
    """Test with no matching prefixes."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [(2, 100, "Completely different text")],
    )
    await filters_repo._db.commit()

    result = await filters_repo.count_matching_prefixes_in_other_channels(
        1, ["This is a specific prefix that won't match"]
    )
    assert result == 0


async def test_count_matching_prefixes_with_matches(filters_repo, channels_repo):
    """Test with matching prefixes."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    prefix = "Same prefix message"
    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, prefix),
            (2, 100, prefix),  # Same prefix in other channel
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.count_matching_prefixes_in_other_channels(1, [prefix])
    assert result == 1


async def test_count_matching_prefixes_multiple(filters_repo, channels_repo):
    """Test with multiple matching prefixes."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (2, 100, "Prefix one message"),
            (2, 101, "Prefix two message"),
            (2, 102, "Prefix one message"),  # Duplicate prefix, counted once
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.count_matching_prefixes_in_other_channels(
        1, ["Prefix one message", "Prefix two message"]
    )
    assert result == 2


async def test_count_matching_prefixes_ignores_forwards(filters_repo, channels_repo):
    """Форвард-копия в чужом канале не считается совпадением (атрибуция); копипаст — да."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Other"))

    prefix = "Same prefix message"
    await filters_repo._db.executemany(
        "INSERT INTO messages (channel_id, message_id, text, forward_from_channel_id, date)"
        " VALUES (?, ?, ?, ?, datetime('now'))",
        [
            (1, 100, prefix, None),
            (2, 100, prefix, 999),  # чужая копия — оформленный форвард
            (1, 101, "Plain prefix copy here", None),
            (2, 101, "Plain prefix copy here", None),  # чужая копия — копипаст
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.count_matching_prefixes_in_other_channels(
        1, [prefix, "Plain prefix copy here"]
    )
    assert result == 1


# fetch_cross_dupe_map tests


async def test_fetch_cross_dupe_map_empty(filters_repo):
    """Test cross dupe map with no messages."""
    result = await filters_repo.fetch_cross_dupe_map()
    assert result == {}


async def test_fetch_cross_dupe_map_no_dupes(filters_repo, channels_repo):
    """Test with no cross-channel duplicates."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Unique message one"),
            (1, 101, "Unique message two"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cross_dupe_map()
    total, duped = result[1]
    assert total == 2
    assert duped == 0


async def test_fetch_cross_dupe_map_with_dupes(filters_repo, channels_repo):
    """Асимметричная семантика: дупликат — публикация ПОЗЖЕ чужой."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    shared_text = "This is a shared message between channels"
    await filters_repo._db.executemany(
        "INSERT INTO messages (channel_id, message_id, text, date) VALUES (?, ?, ?, ?)",
        [
            (1, 100, shared_text, "2026-01-01"),
            (1, 101, "Unique to channel 1", "2026-01-01"),
            (2, 100, shared_text, "2026-02-01"),  # копия — позже источника
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cross_dupe_map()
    total, duped = result[1]
    assert total == 2  # 2 unique prefixes in channel 1
    assert duped == 0  # канал 1 — источник: не считается дупликатом
    total2, duped2 = result[2]
    assert (total2, duped2) == (1, 1)  # канал 2 скопировал позже — дупликат


async def test_fetch_cross_dupe_map_excludes_short(filters_repo, channels_repo):
    """Test that short messages (<=10 chars) are excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Short"),  # <= 10 chars, excluded
            (2, 100, "Short"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cross_dupe_map()
    assert result == {}  # No messages > 10 chars


async def test_fetch_cross_dupe_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Long message from channel 1"),
            (2, 100, "Long message from channel 2"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cross_dupe_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


# fetch_cyrillic_map tests


async def test_fetch_cyrillic_map_empty(filters_repo):
    """Test cyrillic map with no messages."""
    result = await filters_repo.fetch_cyrillic_map()
    assert result == {}


async def test_fetch_cyrillic_map_basic(filters_repo, channels_repo):
    """Test basic cyrillic counting."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "English text"),
            (1, 101, "Русский текст"),
            (1, 102, "Mixed text с русским"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cyrillic_map()
    total, cyr = result[1]
    assert total == 3
    assert cyr == 2  # 2 messages have cyrillic


async def test_fetch_cyrillic_map_excludes_null_empty(filters_repo, channels_repo):
    """Test that NULL and empty text are excluded."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Valid text"),
            (1, 101, None),
            (1, 102, ""),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cyrillic_map()
    total, cyr = result[1]
    assert total == 1  # Only non-null, non-empty counted


async def test_fetch_cyrillic_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Текст на русском"),
            (2, 100, "English text"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_cyrillic_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


async def test_fetch_cyrillic_map_udf_registered(filters_repo, channels_repo):
    """Test that UDF is registered on first call."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.execute(
        _INSERT_MSG,
        (1, 100, "Test"),
    )
    await filters_repo._db.commit()

    # First call should register UDF
    result1 = await filters_repo.fetch_cyrillic_map()
    assert filters_repo._udf_registered is True

    # Second call should use already registered UDF
    result2 = await filters_repo.fetch_cyrillic_map()
    assert result1 == result2


# fetch_auto_feed_map tests


async def test_fetch_auto_feed_map_empty(filters_repo):
    """Test auto-feed map with no messages."""
    result = await filters_repo.fetch_auto_feed_map()
    assert result == {}


async def test_fetch_auto_feed_map_basic(filters_repo, channels_repo):
    """Short (<200 chars) messages with an outbound link are counted."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "Slug one | https://example.com/a"),
            (1, 101, "t.me/somechannel update"),
            (1, 102, "Длинный авторский разбор без ссылок, " + "подробности методологии. " * 20),
            (1, 103, "just words, no link"),
            (1, 104, "https://e.co/" + "x" * 187),  # ровно 200 симв. — не «короткий»
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_auto_feed_map()
    total, short_link = result[1]
    assert total == 5
    assert short_link == 2


async def test_fetch_auto_feed_map_sampled_last_n(filters_repo, channels_repo):
    """sample_size counts only the last N messages (by message_id DESC)."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "slug 1 https://example.com/1"),
            (1, 101, "slug 2 https://example.com/2"),
            (1, 102, "slug 3 https://example.com/3"),
            (1, 103, "slug 4 https://example.com/4"),
            (1, 104, "slug 5 https://example.com/5"),
            (1, 105, "Длинный разбор один, " + "без ссылок и шаблона. " * 12),
            (1, 106, "Длинный разбор два, " + "совсем другой текст. " * 12),
            (1, 107, "Длинный разбор три, " + "третий вариант темы. " * 12),
        ],
    )
    await filters_repo._db.commit()

    sampled = await filters_repo.fetch_auto_feed_map(sample_size=4)
    total, short_link = sampled[1]
    assert total == 4  # ids 107..104
    assert short_link == 1  # только id=104

    full = await filters_repo.fetch_auto_feed_map()
    assert full[1] == (8, 5)


async def test_fetch_auto_feed_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 100, "slug https://example.com/1"),
            (2, 100, "author text without links"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_auto_feed_map(channel_id=1)
    assert 1 in result
    assert 2 not in result


async def test_fetch_auto_feed_map_masked_link_entities(filters_repo, channels_repo):
    """#1492: a masked hyperlink keeps its url only in entities_json — the text
    side is just the visible label («Read more»). Such posts count as short_link
    in both SQL templates; LIKE on NULL entities_json is a no-op for plain posts."""
    await channels_repo.add_channel(Channel(channel_id=1, title="MaskedFeed"))
    await channels_repo.add_channel(Channel(channel_id=2, title="PlainFeed"))

    await filters_repo._db.executemany(
        _INSERT_MSG_ENTITIES,
        [
            (1, 100, "Read more", '[{"length": 9, "offset": 0, "type": "text_url", "url": "https://example.internal/a"}]'),
            (1, 101, "Join our feed", '[{"length": 13, "offset": 0, "type": "text_url", "url": "https://t.me/somechannel"}]'),
        ],
    )
    # Обычные посты: без ссылки в тексте и с entities_json NULL — счёт не меняют.
    await filters_repo._db.executemany(
        _INSERT_MSG,
        [
            (1, 102, "just words, no link"),
            (1, 103, "another plain short post"),
            (2, 100, "Read more"),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_auto_feed_map()
    assert result[1] == (4, 2)
    assert result[2] == (1, 0)

    sampled = await filters_repo.fetch_auto_feed_map(sample_size=1)
    assert sampled[1] == (1, 0)  # срез = последний пост (id=103) — без ссылки

    # Позитивный кейс sampled-шаблона: срез 103..101 содержит entities-ссылку
    # (id=101) — этот ассерт краснеет, если из _SQL_AUTO_FEED_SAMPLED убрать
    # ветку OR entities_json LIKE или колонку из проекции подзапроса.
    sampled = await filters_repo.fetch_auto_feed_map(sample_size=3)
    assert sampled[1] == (3, 1)


# fetch_engagement_map tests


async def test_fetch_engagement_map_empty(filters_repo):
    """Test engagement map with no messages."""
    result = await filters_repo.fetch_engagement_map()
    assert result == {}


async def test_fetch_engagement_map_basic(filters_repo, channels_repo):
    """Views aggregate over the window; NULL views excluded from count and sum."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG_VIEWS,
        [
            (1, 100, "post one", 100),
            (1, 101, "post two", None),
            (1, 102, "post three", 50),
            (1, 103, "post four", 70),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_engagement_map()
    viewed, views_sum = result[1]
    assert viewed == 3
    assert views_sum == 220


async def test_fetch_engagement_map_window_last_n(filters_repo, channels_repo):
    """window=N aggregates only the last N messages by message_id DESC."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))

    await filters_repo._db.executemany(
        _INSERT_MSG_VIEWS,
        [
            (1, 100, "old post one", 10000),
            (1, 101, "old post two", 10000),
            (1, 102, "old post three", 10000),
            (1, 103, "recent post", 1),
        ],
    )
    await filters_repo._db.commit()

    last_one = await filters_repo.fetch_engagement_map(window=1)
    assert last_one[1] == (1, 1)  # только id=103

    full = await filters_repo.fetch_engagement_map()
    assert full[1] == (4, 30001)


async def test_fetch_engagement_map_by_channel(filters_repo, channels_repo):
    """Test filtering by channel_id."""
    await channels_repo.add_channel(Channel(channel_id=1, title="Test"))
    await channels_repo.add_channel(Channel(channel_id=2, title="Test 2"))

    await filters_repo._db.executemany(
        _INSERT_MSG_VIEWS,
        [
            (1, 100, "post one", 50),
            (2, 100, "post two", 999),
        ],
    )
    await filters_repo._db.commit()

    result = await filters_repo.fetch_engagement_map(channel_id=1)
    assert 1 in result
    assert 2 not in result
