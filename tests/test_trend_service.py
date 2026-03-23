"""Tests for TrendService."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

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


@pytest.mark.asyncio
async def test_get_trending_topics_empty_db(service, mock_db):
    """get_trending_topics returns empty list when no messages."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_trending_topics(days=7, limit=20)

    assert result == []


@pytest.mark.asyncio
async def test_get_trending_topics_filters_stop_words(service, mock_db):
    """get_trending_topics filters out stop words."""
    # Return messages with stop words and valid words
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "Это тест и проверка на стоп слова"},
            {"text": "Важное сообщение для теста"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=20)

    # Stop words should be filtered out
    words = [t.keyword for t in result]
    assert "и" not in words
    assert "на" not in words
    assert "для" not in words


@pytest.mark.asyncio
async def test_get_trending_topics_respects_limit(service, mock_db):
    """get_trending_topics respects the limit parameter."""
    # Create many messages with unique words
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": f"word{i} test content"} for i in range(100)
        ]
    )

    result = await service.get_trending_topics(days=7, limit=10)

    assert len(result) <= 10


@pytest.mark.asyncio
async def test_get_trending_topics_short_words_filtered(service, mock_db):
    """get_trending_topics filters words shorter than 4 characters."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "abc test important verify"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=20)

    # "abc" is only 3 chars and should be filtered
    words = [t.keyword for t in result]
    assert "abc" not in words


@pytest.mark.asyncio
async def test_get_trending_topics_with_data(service, mock_db):
    """get_trending_topics processes and returns topics correctly."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "Important topic here", "views": 100},
            {"text": "Another important topic here", "views": 50},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=10)

    assert all(isinstance(t, TrendingTopic) for t in result)
    # Check that "important" appears with count=2 (in both messages)
    important_topic = next((t for t in result if t.keyword == "important"), None)
    assert important_topic is not None
    assert important_topic.count == 2  # "important" appears twice


@pytest.mark.asyncio
async def test_get_trending_topics_non_alpha_only(service, mock_db):
    """get_trending_topics only includes alphabetic words."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[
            {"text": "validword 123numeric"},
            {"text": "test123"},
        ]
    )

    result = await service.get_trending_topics(days=7, limit=10)

    # Non-alpha words like "123numeric" should be filtered
    words = [t.keyword for t in result]
    assert "123numeric" not in words
    assert "test123" not in words
    assert "validword" in words


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_trending_channels_empty_db(service, mock_db):
    """get_trending_channels returns empty list when no data."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_trending_channels(days=7, limit=10)

    assert result == []


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_trending_emojis_empty_db(service, mock_db):
    """get_trending_emojis returns empty list when no reactions."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_trending_emojis(days=7, limit=15)

    assert result == []


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_trending_emojis_respects_limit(service, mock_db):
    """get_trending_emojis respects the limit parameter."""
    mock_db.execute_fetchall = AsyncMock(
        return_value=[{"emoji": f"emoji{i}", "total": 10 - i} for i in range(5)]
    )

    result = await service.get_trending_emojis(days=7, limit=5)

    assert len(result) == 5


# === get_message_velocity tests ===


@pytest.mark.asyncio
async def test_get_message_velocity_empty(service, mock_db):
    """get_message_velocity returns empty list when no messages."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_message_velocity(channel_id=123, days=30)

    assert result == []


@pytest.mark.asyncio
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


# === get_peak_hours tests ===


@pytest.mark.asyncio
async def test_get_peak_hours_empty(service, mock_db):
    """get_peak_hours returns empty list when no messages."""
    mock_db.execute_fetchall = AsyncMock(return_value=[])

    result = await service.get_peak_hours(channel_id=123, days=30)

    assert result == []


@pytest.mark.asyncio
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
