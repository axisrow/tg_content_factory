"""Tests for TrendService."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services import trend_service as trend_service_module
from src.services.trend_service import (
    MessageVelocity,
    PeakHour,
    TrendingChannel,
    TrendingEmoji,
    TrendingTopic,
    TrendService,
)


@pytest.fixture
def mock_db():
    """Mock Database."""
    db = MagicMock()
    db.execute_fetchall = AsyncMock(return_value=[])
    return db


@pytest.fixture
def service(mock_db):
    """TrendService instance."""
    return TrendService(mock_db)


# === get_trending_topics tests ===


@pytest.mark.anyio
async def test_get_trending_topics_empty_db(service, mock_db):
    """get_trending_topics returns empty list when no messages."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_trending_topics(days=7, limit=20)

    assert result == []


@pytest.mark.anyio
async def test_get_trending_topics_filters_high_frequency(service, mock_db):
    """get_trending_topics filters out words appearing in >85% of messages via max_df."""
    # "hello" appears in all 10 messages → 100% > max_df 85% → filtered
    # "topic" appears in 3/10 = 30% → kept
    messages = [{"text": f"hello world testing {i} topic"} for i in range(7)]
    messages.extend([{"text": f"hello world testing {i}"} for i in range(7, 10)])
    mock_db.execute_fetchall = AsyncMock(return_value=messages)

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert "hello" not in keywords  # 100% documents → filtered by max_df
    assert "topic" in keywords  # 30% documents → kept


@pytest.mark.anyio
async def test_get_trending_topics_respects_limit(service, mock_db):
    """get_trending_topics respects the limit parameter."""
    keywords = [
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
    ]
    messages = []
    for keyword in keywords:
        messages.append({"text": f"{keyword} commonword"})
        messages.append({"text": f"{keyword} commonword"})
    mock_db.execute_fetchall = AsyncMock(return_value=messages)

    result = await service.get_trending_topics(days=7, limit=5)

    assert len(result) == 5
    assert {topic.keyword for topic in result}.issubset(set(keywords))


@pytest.mark.anyio
async def test_get_trending_topics_short_words_filtered(service, mock_db):
    """get_trending_topics filters words shorter than 4 characters."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "abc verify signal"},
            {"text": "abc verify insight"},
            {"text": "signal topic insight"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=20)

    words = [t.keyword for t in result]
    assert "abc" not in words
    assert "verify" in words


@pytest.mark.anyio
async def test_get_trending_topics_with_data(service, mock_db):
    """get_trending_topics processes and returns topics correctly."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "важная важная тема здесь"},
            {"text": "другая важная тема"},
            {"text": "сегодня тема обзор"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=10)

    assert all(isinstance(t, TrendingTopic) for t in result)
    keywords = [t.keyword for t in result]
    assert "важная" in keywords
    important = next((t for t in result if t.keyword == "важная"), None)
    assert important is not None
    assert important.count == 3


@pytest.mark.anyio
async def test_get_trending_topics_non_alpha_only(service, mock_db):
    """get_trending_topics only includes alphabetic words (4+ chars, RU/EN)."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "keepword 123numeric"},
            {"text": "keepword test123"},
            {"text": "другоеслово 123numeric"},
            {"text": "иноеслово test123"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=10)

    words = [t.keyword for t in result]
    assert "123numeric" not in words
    assert "test123" not in words
    assert "keepword" in words


def test_analyze_topic_text_skips_jieba_for_ru_en_text(monkeypatch):
    """RU/EN-only analysis should not call the Chinese segmenter."""

    def fail_cut(_text):
        raise AssertionError("jieba.cut should not run without Han characters")

    monkeypatch.setattr(trend_service_module.jieba, "cut", fail_cut)

    assert TrendService._analyze_topic_text("quantum рынок keepword") == ["quantum", "рынок", "keepword"]


@pytest.mark.anyio
async def test_get_trending_topics_segments_chinese_keywords(service, mock_db):
    """get_trending_topics returns segmented Chinese topic keywords."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "人工智能 推动 芯片 市场 增长"},
            {"text": "人工智能 芯片 公司 发布 新产品"},
            {"text": "芯片 市场 需求 上升"},
            {"text": "新能源汽车 出口 增长"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert result
    assert "人工智能" in keywords
    assert "芯片" in keywords
    chip = next(t for t in result if t.keyword == "芯片")
    assert chip.count == 3


@pytest.mark.anyio
async def test_get_trending_topics_filters_chinese_stop_words(service, mock_db):
    """get_trending_topics filters common Chinese stop words."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "这个 我们 可以 因为 人工智能 芯片"},
            {"text": "这个 我们 可以 因为 人工智能 市场"},
            {"text": "芯片 市场 需求 增长"},
            {"text": "新能源汽车 出口 增长"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert "人工智能" in keywords
    assert "芯片" in keywords
    assert "这个" not in keywords
    assert "我们" not in keywords
    assert "可以" not in keywords
    assert "因为" not in keywords


@pytest.mark.anyio
async def test_get_trending_topics_mixed_ru_en_chinese_corpus(service, mock_db):
    """get_trending_topics keeps meaningful RU, EN, and Chinese keywords together."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "quantum рынок 人工智能 芯片"},
            {"text": "quantum рынок 人工智能 市场"},
            {"text": "robotics криптовалюта 芯片"},
            {"text": "analytics финансы 出口"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert "quantum" in keywords
    assert "рынок" in keywords
    assert "人工智能" in keywords
    assert "芯片" in keywords


@pytest.mark.anyio
async def test_get_trending_topics_respects_days(service, mock_db):
    """get_trending_topics respects days parameter."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[{"text": "test content"}]
    )

    await service.get_trending_topics(days=30, limit=10)

    mock_db.execute_fetchall.assert_called_once()
    # Check that days parameter was passed correctly in the query
    args, _ = mock_db.execute_fetchall.call_args
    # First arg is the SQL query, second is params tuple
    assert "-30 days" in str(args[0]) or any("-30 days" in str(a) for a in args[1])


# === get_trending_channels tests ===


@pytest.mark.anyio
async def test_get_trending_channels_empty_db(service, mock_db):
    """get_trending_channels returns empty list when no data."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_trending_channels(days=7, limit=10)

    assert result == []


@pytest.mark.anyio
async def test_get_trending_channels_with_min_messages(service, mock_db):
    """get_trending_channels only includes channels with >= 3 messages."""
    # SQL HAVING COUNT >= 3 filters, so mock only returns qualifying rows
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {
                "channel_id": 1,
                "title": "Channel 1",
                "username": "channel1",
                "avg_views": 100.0,
                "message_count": 5,
            },
        ]
    )

    result = await service.get_trending_channels(days=7, limit=10)

    # Only channels with >= 3 messages are returned by SQL
    assert len(result) == 1
    assert result[0].channel_id == 1
    assert result[0].message_count == 5


@pytest.mark.anyio
async def test_get_trending_channels_respects_limit(service, mock_db):
    """get_trending_channels respects the limit parameter."""
    # Mock returns 5 items (as SQL LIMIT would do)
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {
                "channel_id": i,
                "title": f"Channel {i}",
                "username": f"channel{i}",
                "avg_views": float(100 - i),
                "message_count": 5,
            }
            for i in range(5)
        ]
    )

    result = await service.get_trending_channels(days=7, limit=5)

    assert len(result) == 5


# === get_trending_emojis tests ===


@pytest.mark.anyio
async def test_get_trending_emojis_empty_db(service, mock_db):
    """get_trending_emojis returns empty list when no reactions."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_trending_emojis(days=7, limit=15)

    assert result == []


@pytest.mark.anyio
async def test_get_trending_emojis_from_reactions(service, mock_db):
    """get_trending_emojis aggregates reactions correctly."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
                {"emoji": "👍", "total": 10},
                {"emoji": "❤️", "total": 5},
            ]
    )

    result = await service.get_trending_emojis(days=7, limit=15)

    assert len(result) == 2
    assert result[0].emoji == "👍"
    assert result[0].count == 10
    assert result[1].emoji == "❤️"
    assert result[1].count == 5


@pytest.mark.anyio
async def test_get_trending_emojis_respects_limit(service, mock_db):
    """get_trending_emojis respects the limit parameter."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[{"emoji": f"emoji{i}", "total": 10 - i} for i in range(5)]
    )

    result = await service.get_trending_emojis(days=7, limit=5)

    assert len(result) == 5


# === get_message_velocity tests ===


@pytest.mark.anyio
async def test_get_message_velocity_empty(service, mock_db):
    """get_message_velocity returns empty list when no messages."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_message_velocity(channel_id=123, days=30)

    assert result == []


@pytest.mark.anyio
async def test_get_message_velocity_by_channel(service, mock_db):
    """get_message_velocity returns daily counts for a channel."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"day": "2024-01-01", "cnt": 5},
            {"day": "2024-01-02", "cnt": 10},
        ]
    )

    result = await service.get_message_velocity(channel_id=123, days=30)

    assert len(result) == 2
    assert result[0].date == "2024-01-01"
    assert result[0].count == 5
    assert result[1].date == "2024-01-02"
    assert result[1].count == 10
    assert mock_db.execute_fetchall.await_args.args[1] == ("-30 days", 123)


@pytest.mark.anyio
async def test_get_message_velocity_all_channels_when_channel_id_omitted(service, mock_db):
    """get_message_velocity aggregates all channels when channel_id is omitted."""
    mock_db.execute_fetchall = AsyncMock(return_value=[{"day": "2024-01-01", "cnt": 15}])

    result = await service.get_message_velocity(days=7)

    sql = mock_db.execute_fetchall.await_args.args[0]
    assert "m.channel_id = ?" not in sql
    assert mock_db.execute_fetchall.await_args.args[1] == ("-7 days",)
    assert result[0].count == 15


# === get_peak_hours tests ===


@pytest.mark.anyio
async def test_get_peak_hours_empty(service, mock_db):
    """get_peak_hours returns empty list when no messages."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_peak_hours(channel_id=123, days=30)

    assert result == []


@pytest.mark.anyio
async def test_get_peak_hours_distribution(service, mock_db):
    """get_peak_hours returns hourly distribution correctly."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"hour": 12, "cnt": 5},
            {"hour": 18, "cnt": 10},
        ]
    )

    result = await service.get_peak_hours(channel_id=123, days=30)

    assert len(result) == 2
    assert result[0].hour == 12
    assert result[0].count == 5
    assert result[1].hour == 18
    assert result[1].count == 10
    assert mock_db.execute_fetchall.await_args.args[1] == ("-30 days", 123)


@pytest.mark.anyio
async def test_get_peak_hours_all_channels_when_channel_id_omitted(service, mock_db):
    """get_peak_hours aggregates all channels when channel_id is omitted."""
    mock_db.execute_fetchall = AsyncMock(return_value=[{"hour": 9, "cnt": 7}])

    result = await service.get_peak_hours(days=7)

    sql = mock_db.execute_fetchall.await_args.args[0]
    assert "m.channel_id = ?" not in sql
    assert mock_db.execute_fetchall.await_args.args[1] == ("-7 days",)
    assert result[0].hour == 9
    assert result[0].count == 7


# === Dataclass tests ===


def test_trending_topic_dataclass():
    """TrendingTopic dataclass stores fields correctly."""
    topic = TrendingTopic(keyword="test", count=10)

    assert topic.keyword == "test"
    assert topic.count == 10


def test_trending_channel_dataclass():
    """TrendingChannel dataclass stores fields correctly."""
    channel = TrendingChannel(
        channel_id=1,
        title="Test",
        username="test",
        avg_views=100.0,
        message_count=10,
    )

    assert channel.channel_id == 1
    assert channel.title == "Test"
    assert channel.username == "test"
    assert channel.avg_views == 100.0
    assert channel.message_count == 10


def test_trending_emoji_dataclass():
    """TrendingEmoji dataclass stores fields correctly."""
    emoji = TrendingEmoji(emoji="👍", count=5)

    assert emoji.emoji == "👍"
    assert emoji.count == 5


def test_message_velocity_dataclass():
    """MessageVelocity dataclass stores fields correctly."""
    velocity = MessageVelocity(date="2024-01-01", count=10)

    assert velocity.date == "2024-01-01"
    assert velocity.count == 10


def test_peak_hour_dataclass():
    """PeakHour dataclass stores fields correctly."""
    peak = PeakHour(hour=12, count=5)

    assert peak.hour == 12
    assert peak.count == 5


# === Regression tests ===


@pytest.mark.anyio
async def test_get_trending_topics_tfidf_suppresses_noise(service, mock_db):
    """Regression #329: words in every message should not appear in trends."""
    # "hello" appears in all 10 messages → max_df filters it out
    # "криптовалюта" appears in 2/10 → high IDF → should be in top
    messages = [{"text": f"hello world testing {i}"} for i in range(10)]
    messages[0]["text"] += " криптовалюта"
    messages[1]["text"] += " криптовалюта"
    mock_db.execute_fetchall = AsyncMock(return_value=messages)

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert "криптовалюта" in keywords
    assert "hello" not in keywords  # in 100% documents → filtered by max_df


@pytest.mark.anyio
async def test_get_trending_topics_cleans_urls_html_and_stop_words(service, mock_db):
    """Regression #539: URL/HTML noise should not outrank meaningful keywords."""
    messages = [
        {"text": "<a href='https://t.me/news'>Read this</a> quantum рынок &amp;nbsp;"},
        {"text": "www.example.com/?utm_source=x quantum рынок after before"},
        {"text": "<b>AI</b> quantum рынок https://example.com/page"},
        {"text": "daily update product launch"},
    ]
    mock_db.execute_fetchall = AsyncMock(return_value=messages)

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert "quantum" in keywords
    assert "рынок" in keywords
    assert "href" not in keywords
    assert "https" not in keywords
    assert "example" not in keywords
    assert "after" not in keywords
    assert "before" not in keywords


@pytest.mark.anyio
async def test_get_trending_topics_filters_english_and_generic_topic_noise(service, mock_db):
    """Regression #541 follow-up: generic English words should not become trends."""
    messages = [
        {"text": "your will need content data robotics"},
        {"text": "your will need page site robotics"},
        {"text": "machinelearning neural insights"},
        {"text": "machinelearning neural trends"},
    ]
    mock_db.execute_fetchall = AsyncMock(return_value=messages)

    result = await service.get_trending_topics(days=7, limit=20)
    keywords = [t.keyword for t in result]

    assert "robotics" in keywords
    assert "machinelearning" in keywords
    assert "neural" in keywords
    assert "your" not in keywords
    assert "will" not in keywords
    assert "need" not in keywords
    assert "content" not in keywords
    assert "data" not in keywords
    assert "page" not in keywords
    assert "site" not in keywords
