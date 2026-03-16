"""Tests for ChannelStatsRepository."""
from __future__ import annotations

from datetime import datetime

import pytest

from src.database.repositories.channel_stats import ChannelStatsRepository
from src.models import Channel, ChannelStats


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return ChannelStatsRepository(db.db)


async def _create_channel(db, channel_id):
    """Helper to create a channel for FK constraint."""
    channel = Channel(
        channel_id=channel_id,
        title=f"Test Channel {channel_id}",
        username=f"test_{abs(channel_id)}",
    )
    await db.add_channel(channel)


async def test_save_channel_stats(db, repo):
    """Test saving channel stats."""
    await _create_channel(db, -1001234567890)
    stats = ChannelStats(
        channel_id=-1001234567890,
        subscriber_count=10000,
        avg_views=500.5,
        avg_reactions=25.3,
        avg_forwards=10.1,
    )
    result_id = await repo.save_channel_stats(stats)
    assert result_id > 0


async def test_get_channel_stats_empty(repo):
    """Test getting stats for non-existent channel."""
    result = await repo.get_channel_stats(-1009999999999)
    assert result == []


async def test_get_channel_stats(db, repo):
    """Test getting stats for a channel."""
    channel_id = -1001234567890
    await _create_channel(db, channel_id)

    stats = ChannelStats(
        channel_id=channel_id,
        subscriber_count=10000,
        avg_views=500.5,
        avg_reactions=25.3,
        avg_forwards=10.1,
    )
    await repo.save_channel_stats(stats)

    result = await repo.get_channel_stats(channel_id)
    assert len(result) == 1

    retrieved = result[0]
    assert retrieved.channel_id == channel_id
    assert retrieved.subscriber_count == 10000
    assert retrieved.avg_views == 500.5
    assert retrieved.avg_reactions == 25.3
    assert retrieved.avg_forwards == 10.1
    assert retrieved.collected_at is not None


async def test_get_channel_stats_multiple(db, repo):
    """Test getting multiple stats records for a channel."""
    channel_id = -1001234567890
    await _create_channel(db, channel_id)

    # Save multiple stats
    for i in range(3):
        stats = ChannelStats(
            channel_id=channel_id,
            subscriber_count=10000 + i * 100,
            avg_views=500.0 + i,
            avg_reactions=25.0,
            avg_forwards=10.0,
        )
        await repo.save_channel_stats(stats)

    result = await repo.get_channel_stats(channel_id, limit=10)
    assert len(result) == 3

    # Should be ordered by collected_at DESC
    assert result[0].subscriber_count == 10200
    assert result[2].subscriber_count == 10000


async def test_get_channel_stats_limit(db, repo):
    """Test limit parameter in get_channel_stats."""
    channel_id = -1001234567890
    await _create_channel(db, channel_id)

    for i in range(5):
        stats = ChannelStats(
            channel_id=channel_id,
            subscriber_count=10000 + i * 100,
            avg_views=500.0,
            avg_reactions=25.0,
            avg_forwards=10.0,
        )
        await repo.save_channel_stats(stats)

    result = await repo.get_channel_stats(channel_id, limit=2)
    assert len(result) == 2


async def test_get_latest_stats_for_all_empty(repo):
    """Test getting all latest stats when none exist."""
    result = await repo.get_latest_stats_for_all()
    assert result == {}


async def test_get_latest_stats_for_all(db, repo):
    """Test getting latest stats for all channels."""
    # Save stats for multiple channels
    channel_ids = [-1001111111111, -1002222222222, -1003333333333]

    for ch_id in channel_ids:
        await _create_channel(db, ch_id)
        # Save two records per channel
        for i in range(2):
            stats = ChannelStats(
                channel_id=ch_id,
                subscriber_count=10000 + i * 100,
                avg_views=500.0,
                avg_reactions=25.0,
                avg_forwards=10.0,
            )
            await repo.save_channel_stats(stats)

    result = await repo.get_latest_stats_for_all()

    # Should return one entry per channel (the latest)
    assert len(result) == 3
    for ch_id in channel_ids:
        assert ch_id in result
        # Should be the latest (higher subscriber_count)
        assert result[ch_id].subscriber_count == 10100


async def test_save_stats_with_none_values(db, repo):
    """Test saving stats with None optional values."""
    channel_id = -1001234567890
    await _create_channel(db, channel_id)

    stats = ChannelStats(
        channel_id=channel_id,
        subscriber_count=1000,
        avg_views=None,
        avg_reactions=None,
        avg_forwards=None,
    )

    result_id = await repo.save_channel_stats(stats)
    assert result_id > 0

    retrieved = await repo.get_channel_stats(channel_id)
    assert len(retrieved) == 1
    assert retrieved[0].avg_views is None
    assert retrieved[0].avg_reactions is None
    assert retrieved[0].avg_forwards is None


async def test_get_latest_stats_isolates_channels(db, repo):
    """Test that get_latest_stats_for_all correctly isolates per-channel max dates."""
    ch1 = -1001111111111
    ch2 = -1002222222222

    await _create_channel(db, ch1)
    await _create_channel(db, ch2)

    # Channel 1: save older
    stats1_old = ChannelStats(
        channel_id=ch1,
        subscriber_count=1000,
        avg_views=100.0,
        avg_reactions=10.0,
        avg_forwards=1.0,
    )
    await repo.save_channel_stats(stats1_old)

    # Channel 2: save once
    stats2 = ChannelStats(
        channel_id=ch2,
        subscriber_count=2000,
        avg_views=200.0,
        avg_reactions=20.0,
        avg_forwards=2.0,
    )
    await repo.save_channel_stats(stats2)

    # Channel 1: save newer
    stats1_new = ChannelStats(
        channel_id=ch1,
        subscriber_count=1100,
        avg_views=110.0,
        avg_reactions=11.0,
        avg_forwards=1.1,
    )
    await repo.save_channel_stats(stats1_new)

    result = await repo.get_latest_stats_for_all()
    assert len(result) == 2
    # Channel 1 should have newer values
    assert result[ch1].subscriber_count == 1100
    assert result[ch2].subscriber_count == 2000


async def test_stats_collected_at_is_datetime(db, repo):
    """Test that collected_at is properly converted to datetime."""
    channel_id = -1001234567890
    await _create_channel(db, channel_id)

    stats = ChannelStats(
        channel_id=channel_id,
        subscriber_count=10000,
        avg_views=500.0,
        avg_reactions=25.0,
        avg_forwards=10.0,
    )
    await repo.save_channel_stats(stats)

    result = await repo.get_channel_stats(channel_id)
    assert len(result) == 1
    assert isinstance(result[0].collected_at, datetime)


async def test_save_stats_returns_id(db, repo):
    """Test that save returns a valid row ID."""
    ch1 = -1001234567890
    ch2 = -1009999999999
    await _create_channel(db, ch1)
    await _create_channel(db, ch2)

    stats1 = ChannelStats(
        channel_id=ch1,
        subscriber_count=10000,
        avg_views=500.0,
        avg_reactions=25.0,
        avg_forwards=10.0,
    )
    id1 = await repo.save_channel_stats(stats1)

    stats2 = ChannelStats(
        channel_id=ch2,
        subscriber_count=5000,
        avg_views=250.0,
        avg_reactions=12.0,
        avg_forwards=5.0,
    )
    id2 = await repo.save_channel_stats(stats2)

    assert id1 > 0
    assert id2 > 0
    assert id1 != id2


async def test_negative_channel_id(db, repo):
    """Test handling negative channel IDs (Telegram format)."""
    channel_id = -1001234567890123
    await _create_channel(db, channel_id)

    stats = ChannelStats(
        channel_id=channel_id,
        subscriber_count=5000,
        avg_views=250.0,
        avg_reactions=12.5,
        avg_forwards=5.0,
    )
    await repo.save_channel_stats(stats)

    result = await repo.get_channel_stats(channel_id)
    assert len(result) == 1
    assert result[0].channel_id == channel_id
