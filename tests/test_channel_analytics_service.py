from __future__ import annotations

import argparse
import asyncio
import dataclasses
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.config import AppConfig
from src.models import Channel, ChannelStats, Message
from src.services.channel_analytics_service import (
    ChannelAnalyticsService,
    ChannelComparison,
    ChannelListItem,
    ChannelOverview,
    ChannelRanking,
    CitationStats,
)

# ─── helpers ───────────────────────────────────────────────────────


async def _seed_channel(db, channel_id=100123, title="Test Channel", username="test_chan"):
    await db.add_channel(Channel(channel_id=channel_id, title=title, username=username))
    return await db.get_channel_by_channel_id(channel_id)


async def _seed_stats(db, channel_id, subscriber_count=1000, avg_views=500.0,
                      avg_reactions=25.0, avg_forwards=10.0):
    stats = ChannelStats(
        channel_id=channel_id,
        subscriber_count=subscriber_count,
        avg_views=avg_views,
        avg_reactions=avg_reactions,
        avg_forwards=avg_forwards,
    )
    await db.save_channel_stats(stats)
    return stats


async def _seed_message(db, channel_id, msg_id=1, text="hello", views=100,
                        forwards=5, reply_count=2,
                        date_str="2025-01-15T12:00:00+00:00"):
    msg = Message(
        channel_id=channel_id,
        message_id=msg_id,
        text=text,
        views=views,
        forwards=forwards,
        reply_count=reply_count,
        date=datetime.fromisoformat(date_str),
    )
    await db.insert_message(msg)
    return msg


async def _seed_messages_batch(db, channel_id, messages):
    return await db.insert_messages_batch(messages) or len(messages) or 0


# ─── tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_active_channels(db):
    await _seed_channel(db, channel_id=100111, title="Active 1", username="active1")
    await _seed_channel(db, channel_id=100222, title="Active 2", username="active2")
    ch3 = await _seed_channel(db, channel_id=100333, title="Filtered", username="filtered_chan")
    await db.set_channel_filtered(ch3.id, True)

    svc = ChannelAnalyticsService(db)
    channels = await svc.get_active_channels()
    assert len(channels) == 2
    ids = {c.channel_id for c in channels}
    assert 100111 in ids
    assert 100222 in ids
    assert 100333 not in ids
    assert all(isinstance(c, ChannelListItem) for c in channels)


@pytest.mark.asyncio
async def test_get_active_channels_empty(db):
    svc = ChannelAnalyticsService(db)
    channels = await svc.get_active_channels()
    assert channels == []


@pytest.mark.asyncio
async def test_get_channel_overview_basic(db):
    await _seed_channel(db, channel_id=100123, title="Overview Chan", username="ov_chan")
    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(100123)
    assert isinstance(overview, ChannelOverview)
    assert overview.channel_id == 100123
    assert overview.title == "Overview Chan"
    assert overview.username == "ov_chan"
    assert overview.subscriber_count is None
    assert overview.total_posts == 0
    assert overview.posts_today == 0
    assert overview.posts_month == 0
    assert overview.err is None
    assert overview.err24 is None


@pytest.mark.asyncio
async def test_get_channel_overview_with_subscribers(db):
    await _seed_channel(db, channel_id=100123, title="Sub Chan")
    # Two stats entries for delta calculation
    await _seed_stats(db, 100123, subscriber_count=1000, avg_views=500.0)
    await _seed_stats(db, 100123, subscriber_count=1200, avg_views=600.0)

    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(100123)
    assert overview.subscriber_count == 1200
    assert overview.subscriber_delta == 200  # 1200 - 1000
    assert overview.avg_views == 600.0
    assert overview.avg_forwards == 10.0
    assert overview.avg_reactions == 25.0


@pytest.mark.asyncio
async def test_get_channel_overview_with_posts(db):
    await _seed_channel(db, channel_id=100123, title="Post Chan")
    await _seed_stats(db, 100123, subscriber_count=500, avg_views=200.0)
    now = datetime.now(timezone.utc)

    messages = []
    # 5 today
    for i in range(5):
        messages.append(Message(
            channel_id=100123,
            message_id=100 + i,
            text=f"msg {i}",
            views=50 + i * 10,
            forwards=i,
            reply_count=1,
            date=now - timedelta(hours=i),
        ))
    # 3 within the last week but more than 1 day ago
    for i in range(3):
        messages.append(Message(
            channel_id=100123,
            message_id=200 + i,
            text=f"week msg {i}",
            views=30 + i,
            forwards=i,
            reply_count=0,
            date=now - timedelta(days=5),
        ))
    # 1 old message from 35 days ago
    messages.append(Message(
        channel_id=100123,
        message_id=300,
        text="old msg",
        views=10,
        forwards=0,
        reply_count=0,
        date=now - timedelta(days=35),
    ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(100123, days=30)
    assert overview.total_posts == 9
    assert overview.posts_today == 5
    assert overview.posts_week == 8
    assert overview.posts_month == 8  # 5 today + 3 this week (within 30d)

    overview7 = await svc.get_channel_overview(100123, days=7)
    assert overview7.posts_month == 8  # 5 today + 3 this week

    overview1 = await svc.get_channel_overview(100123, days=1)
    assert overview1.posts_month == 5  # 5 within last day


@pytest.mark.asyncio
async def test_get_channel_overview_missing_channel(db):
    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(999999)
    assert overview.channel_id == 999999
    assert overview.title is None
    assert overview.username is None


@pytest.mark.asyncio
async def test_get_subscriber_history(db):
    await _seed_channel(db, channel_id=100123, title="Hist Chan")
    for i in range(5):
        await db.save_channel_stats(ChannelStats(
            channel_id=100123,
            subscriber_count=100 + i * 50,
        ))

    svc = ChannelAnalyticsService(db)
    history = await svc.get_subscriber_history(100123, days=5)
    assert len(history) == 5
    # Verify chronological order
    counts = [entry["subscriber_count"] for entry in history]
    assert counts == sorted(counts)
    assert counts[-1] == 300


@pytest.mark.asyncio
async def test_get_views_timeseries(db):
    await _seed_channel(db, channel_id=100123, title="Views Chan")
    now = datetime.now(timezone.utc)
    messages = []
    for i in range(3):
        ts = now - timedelta(days=i)
        messages.append(Message(
            channel_id=100123,
            message_id=10 + i,
            text=f"msg {i}",
            views=100 * (i + 1),
            date=ts,
        ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    ts = await svc.get_views_timeseries(100123, days=5)
    assert len(ts) >= 1
    for entry in ts:
        assert "day" in entry
        assert "message_count" in entry
        assert "avg_views" in entry


@pytest.mark.asyncio
async def test_get_post_frequency(db):
    await _seed_channel(db, channel_id=100123, title="Freq Chan")
    now = datetime.now(timezone.utc)
    today_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
    messages = []
    # 3 messages today (anchored to noon so they never cross midnight)
    for i in range(3):
        messages.append(Message(
            channel_id=100123,
            message_id=100 + i,
            text=f"today msg {i}",
            views=10,
            date=today_noon - timedelta(hours=i),
        ))
    # 2 yesterday
    for i in range(2):
        messages.append(Message(
            channel_id=100123,
            message_id=200 + i,
            text=f"yesterday msg {i}",
            date=today_noon - timedelta(days=1, hours=i),
        ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    freq = await svc.get_post_frequency(100123, days=7)
    assert len(freq) == 2
    today_count = sum(e["count"] for e in freq if e["day"] == today_noon.strftime("%Y-%m-%d"))
    assert today_count == 3
    yesterday_count = sum(
        e["count"] for e in freq
        if e["day"] == (today_noon - timedelta(days=1)).strftime("%Y-%m-%d")
    )
    assert yesterday_count == 2


@pytest.mark.asyncio
async def test_get_citation_stats(db):
    await _seed_channel(db, channel_id=100123, title="Cite Chan")
    messages = []
    for i in range(5):
        messages.append(Message(
            channel_id=100123,
            message_id=100 + i,
            text=f"msg {i}",
            views=100,
            forwards=i * 10,
            date=datetime(2025, 1, 15, 12, 0, 0),
        ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    stats = await svc.get_citation_stats(100123)
    assert isinstance(stats, CitationStats)
    assert stats.post_count == 5
    assert stats.total_forwards == sum(range(5)) * 10  # 0+10+20+30+40 = 100
    assert stats.avg_forwards == 20.0  # 100 / 5


@pytest.mark.asyncio
async def test_get_err(db):
    await _seed_channel(db, channel_id=100123, title="ERR Chan")
    await _seed_stats(db, 100123, subscriber_count=1000, avg_views=200.0)
    messages = []
    for i in range(5):
        messages.append(Message(
            channel_id=100123,
            message_id=100 + i,
            text=f"msg {i}",
            views=100,
            forwards=5,
            reply_count=2,
            date=datetime(2025, 1, 15, 12, 0, 0),
        ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    err = await svc.get_err(100123)
    assert isinstance(err, float)
    assert err > 0
    # ERR = total_engagement / (num_posts * subscribers) * 100
    # total_engagement = 5 * (100 + 5 + 2) = 535
    # err = 535 / (5 * 1000) * 100 = 10.7
    assert abs(err - 10.7) < 0.1


@pytest.mark.asyncio
async def test_get_err_no_subscribers(db):
    await _seed_channel(db, channel_id=100123, title="No Sub Chan")
    svc = ChannelAnalyticsService(db)
    err = await svc.get_err(100123)
    assert err is None


@pytest.mark.asyncio
async def test_get_err24(db):
    await _seed_channel(db, channel_id=100123, title="ERR24 Chan")
    await _seed_stats(db, 100123, subscriber_count=500)
    now = datetime.now(timezone.utc)
    msg = Message(
        channel_id=100123,
        message_id=1,
        text="recent msg",
        views=200,
        forwards=10,
        reply_count=5,
        date=now - timedelta(hours=1),
    )
    await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    err24 = await svc.get_err24(100123)
    assert isinstance(err24, float)
    assert err24 > 0
    # engagement = 200 + 10 + 5 = 215; err24 = 215 / (1 * 500) * 100 = 43.0
    assert abs(err24 - 43.0) < 0.1


@pytest.mark.asyncio
async def test_get_err24_no_recent_posts(db):
    await _seed_channel(db, channel_id=100123, title="Empty ERR24")
    await _seed_stats(db, 100123, subscriber_count=500)
    msg = Message(
        channel_id=100123,
        message_id=1,
        text="old msg",
        views=50,
        date=datetime.now(timezone.utc) - timedelta(days=2),
    )
    await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    err24 = await svc.get_err24(100123)
    assert err24 is None


@pytest.mark.asyncio
async def test_get_hourly_activity(db):
    await _seed_channel(db, channel_id=100123, title="Hourly Chan")
    now = datetime.now(timezone.utc)
    messages = []
    for hour in [8, 8, 8, 14, 14, 20]:
        messages.append(Message(
            channel_id=100123,
            message_id=len(messages) + 1,
            text=f"msg at {hour}",
            date=now.replace(hour=hour, minute=0, second=0, microsecond=0),
        ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    activity = await svc.get_hourly_activity(100123, days=7)
    assert len(activity) > 0
    hour_counts = {e["hour"]: e["count"] for e in activity}
    assert hour_counts.get(8) == 3
    assert hour_counts.get(14) == 2
    assert hour_counts.get(20) == 1


@pytest.mark.asyncio
async def test_get_ranked_channels_by_err(db):
    await _seed_channel(db, channel_id=100111, title="Low ERR", username="low_err")
    await _seed_channel(db, channel_id=100222, title="High ERR", username="high_err")
    await _seed_stats(db, 100111, subscriber_count=1000, avg_views=100.0)
    await _seed_stats(db, 100222, subscriber_count=1000, avg_views=500.0)

    for i in range(3):
        msg = Message(
            channel_id=100111,
            message_id=100 + i,
            text=f"low msg {i}",
            views=50,
            forwards=1,
            reply_count=0,
            date=datetime(2025, 1, 15, 12, 0, 0),
        )
        await db.insert_message(msg)

    for i in range(3):
        msg = Message(
            channel_id=100222,
            message_id=200 + i,
            text=f"high msg {i}",
            views=500,
            forwards=20,
            reply_count=10,
            date=datetime(2025, 1, 15, 12, 0, 0),
        )
        await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    rankings = await svc.get_ranked_channels(metric="err")
    assert len(rankings) == 2
    assert all(isinstance(r, ChannelRanking) for r in rankings)
    assert rankings[0].channel_id == 100222
    assert rankings[0].rank == 1
    assert rankings[0].score > rankings[1].score
    assert rankings[1].channel_id == 100111
    assert rankings[1].rank == 2


@pytest.mark.asyncio
async def test_get_ranked_channels_by_subscriber_count(db):
    await _seed_channel(db, channel_id=100111, title="Small", username="small")
    await _seed_channel(db, channel_id=100222, title="Big", username="big")
    await _seed_stats(db, 100111, subscriber_count=100)
    await _seed_stats(db, 100222, subscriber_count=10000)

    svc = ChannelAnalyticsService(db)
    rankings = await svc.get_ranked_channels(metric="subscriber_count")
    assert len(rankings) == 2
    assert rankings[0].channel_id == 100222
    assert rankings[0].score == 10000.0
    assert rankings[1].channel_id == 100111
    assert rankings[1].score == 100.0


@pytest.mark.asyncio
async def test_get_ranked_channels_limit(db):
    for i in range(5):
        await _seed_channel(db, channel_id=-100100 - i, title=f"Ch {i}")
        await _seed_stats(db, -100100 - i, subscriber_count=(i + 1) * 100)

    svc = ChannelAnalyticsService(db)
    rankings = await svc.get_ranked_channels(metric="subscriber_count", limit=3)
    assert len(rankings) == 3
    assert all(r.rank <= 3 for r in rankings)


@pytest.mark.asyncio
async def test_get_channel_comparison(db):
    await _seed_channel(db, channel_id=100111, title="Chan A", username="chan_a")
    await _seed_channel(db, channel_id=100222, title="Chan B", username="chan_b")
    await _seed_stats(db, 100111, subscriber_count=1000)
    await _seed_stats(db, 100222, subscriber_count=2000)

    svc = ChannelAnalyticsService(db)
    comparison = await svc.get_channel_comparison([100111, 100222])
    assert isinstance(comparison, ChannelComparison)
    assert len(comparison.channels) == 2
    assert comparison.channels[0].channel_id == 100111
    assert comparison.channels[1].channel_id == 100222
    assert comparison.channels[0].subscriber_count == 1000
    assert comparison.channels[1].subscriber_count == 2000
    assert "subscriber_count" in comparison.metrics
    assert "err" in comparison.metrics


@pytest.mark.asyncio
async def test_get_channel_comparison_empty(db):
    svc = ChannelAnalyticsService(db)
    comparison = await svc.get_channel_comparison([])
    assert isinstance(comparison, ChannelComparison)
    assert comparison.channels == []


@pytest.mark.asyncio
async def test_get_ranked_channels_unknown_metric(db):
    await _seed_channel(db, channel_id=100111, title="Unknown Metric Chan")
    await _seed_stats(db, 100111, subscriber_count=500)

    svc = ChannelAnalyticsService(db)
    rankings = await svc.get_ranked_channels(metric="nonexistent_metric")
    assert len(rankings) == 1
    assert rankings[0].score == 0.0


@pytest.mark.asyncio
async def test_overview_to_dict(db):
    """Verify dataclass serialization works for template rendering."""
    await _seed_channel(db, channel_id=100123, title="Dict Chan")
    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(100123)
    d = dataclasses.asdict(overview)
    assert isinstance(d, dict)
    assert d["channel_id"] == 100123
    assert d["title"] == "Dict Chan"
    assert "subscriber_count" in d
    assert "err" in d
    assert "posts_today" in d


# ── heatmap tests ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_heatmap_empty(db):
    await _seed_channel(db, channel_id=100123, title="Heatmap Empty")
    svc = ChannelAnalyticsService(db)
    data = await svc.get_heatmap(100123, days=30)
    assert data == []


@pytest.mark.asyncio
async def test_get_heatmap_basic(db):
    await _seed_channel(db, channel_id=100123, title="Heatmap Chan")
    now = datetime.now(timezone.utc)
    messages = []
    # Monday (weekday 1 via %w = 1)
    mon = now - timedelta(days=now.weekday())
    for h in [8, 9, 10]:
        messages.append(Message(
            channel_id=100123,
            message_id=len(messages) + 1,
            text=f"mon {h}",
            date=mon.replace(hour=h, minute=0, second=0, microsecond=0),
        ))
    # Wednesday (%w = 3)
    wed = mon + timedelta(days=2)
    for h in [14, 14]:
        messages.append(Message(
            channel_id=100123,
            message_id=len(messages) + 1,
            text=f"wed {h}",
            date=wed.replace(hour=14, minute=0, second=0, microsecond=0),
        ))
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_heatmap(100123, days=30)
    assert len(data) > 0
    # All rows have expected keys
    for row in data:
        assert "hour" in row
        assert "weekday" in row
        assert "count" in row
        assert 0 <= row["hour"] <= 23
        assert 0 <= row["weekday"] <= 6

    # Check actual counts: Monday(%w=1) should have 3 messages at hours 8,9,10
    # The weekday values depend on the actual date of `mon`, but the data
    # should aggregate correctly. Just check total messages match.
    total_count = sum(r["count"] for r in data)
    assert total_count == 5  # 3 Monday + 2 Wednesday


@pytest.mark.asyncio
async def test_get_heatmap_days_filter(db):
    await _seed_channel(db, channel_id=100123, title="Heatmap Days")
    now = datetime.now(timezone.utc)
    # Recent message
    await db.insert_message(Message(
        channel_id=100123, message_id=1, text="recent",
        date=now - timedelta(hours=1),
    ))
    # Old message outside 7-day window
    await db.insert_message(Message(
        channel_id=100123, message_id=2, text="old",
        date=now - timedelta(days=14),
    ))

    svc = ChannelAnalyticsService(db)
    data7 = await svc.get_heatmap(100123, days=7)
    data30 = await svc.get_heatmap(100123, days=30)
    total_7 = sum(r["count"] for r in data7)
    total_30 = sum(r["count"] for r in data30)
    assert total_7 == 1
    assert total_30 == 2


# ── cross-channel citation tests ─────────────────────────────────────


@pytest.mark.asyncio
async def test_get_cross_channel_citations_empty(db):
    await _seed_channel(db, channel_id=100123, title="Cite Empty")
    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123)
    assert data == []


@pytest.mark.asyncio
async def test_get_cross_channel_citations_basic(db):
    # Source channel
    await _seed_channel(db, channel_id=100500, title="Source Chan", username="src_chan")
    # Target channel
    await _seed_channel(db, channel_id=100123, title="Target Chan", username="tgt_chan")

    now = datetime.now(timezone.utc)
    # Messages forwarded from 100500 into 100123
    for i in range(3):
        msg = Message(
            channel_id=100123,
            message_id=100 + i,
            text=f"fwd msg {i}",
            date=now - timedelta(hours=i),
            forwards=0,
        )
        msg.forward_from_channel_id = 100500
        await db.insert_message(msg)
    # One regular message (no forward)
    await db.insert_message(Message(
        channel_id=100123,
        message_id=200,
        text="regular msg",
        date=now,
    ))

    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30)
    assert len(data) == 1
    assert data[0]["source_channel_id"] == 100500
    assert data[0]["source_title"] == "Source Chan"
    assert data[0]["citation_count"] == 3
    assert data[0]["latest_date"] is not None


@pytest.mark.asyncio
async def test_get_cross_channel_citations_multiple_sources(db):
    await _seed_channel(db, channel_id=100111, title="Source A", username="src_a")
    await _seed_channel(db, channel_id=100222, title="Source B", username="src_b")
    await _seed_channel(db, channel_id=100123, title="Target", username="target")

    now = datetime.now(timezone.utc)
    for i in range(5):
        msg = Message(
            channel_id=100123, message_id=100 + i, text=f"fwd a {i}",
            date=now,
        )
        msg.forward_from_channel_id = 100111
        await db.insert_message(msg)
    for i in range(2):
        msg = Message(
            channel_id=100123, message_id=200 + i, text=f"fwd b {i}",
            date=now,
        )
        msg.forward_from_channel_id = 100222
        await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30)
    assert len(data) == 2
    # Sorted by citation_count DESC
    assert data[0]["source_channel_id"] == 100111
    assert data[0]["citation_count"] == 5
    assert data[1]["source_channel_id"] == 100222
    assert data[1]["citation_count"] == 2


@pytest.mark.asyncio
async def test_get_cross_channel_citations_limit(db):
    await _seed_channel(db, channel_id=100123, title="Target Limit")
    now = datetime.now(timezone.utc)
    for i in range(10):
        msg = Message(
            channel_id=100123, message_id=100 + i, text=f"fwd {i}",
            date=now,
        )
        msg.forward_from_channel_id = 200 + i
        await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30, limit=3)
    assert len(data) <= 3


# ── repo-level tests for new methods ─────────────────────────────────


@pytest.mark.asyncio
async def test_repo_hour_weekday_heatmap(db):
    await _seed_channel(db, channel_id=100123, title="Repo Heatmap")
    now = datetime.now(timezone.utc)
    # Create messages at known hour/weekday
    messages = [
        Message(channel_id=100123, message_id=1, text="a",
                date=now.replace(hour=10, minute=0, second=0, microsecond=0)),
        Message(channel_id=100123, message_id=2, text="b",
                date=now.replace(hour=10, minute=0, second=0, microsecond=0)),
        Message(channel_id=100123, message_id=3, text="c",
                date=now.replace(hour=22, minute=0, second=0, microsecond=0)),
    ]
    await _seed_messages_batch(db, 100123, messages)

    rows = await db.repos.messages.get_hour_weekday_heatmap(100123, days=7)
    assert len(rows) >= 2  # At least hour 10 and hour 22 entries
    for r in rows:
        assert 0 <= r["hour"] <= 23
        assert 0 <= r["weekday"] <= 6
        assert r["count"] > 0


@pytest.mark.asyncio
async def test_repo_cross_channel_citations_no_forward(db):
    await _seed_channel(db, channel_id=100123, title="No Fwd")
    await db.insert_message(Message(
        channel_id=100123, message_id=1, text="hello",
        date=datetime.now(timezone.utc),
    ))

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert rows == []


# ── web API route tests ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_api_heatmap_route(db):
    """Verify heatmap service returns list (route requires full app state)."""
    await _seed_channel(db, channel_id=100123, title="API Heat")
    svc = ChannelAnalyticsService(db)
    data = await svc.get_heatmap(100123, days=30)
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_cross_citations_route(db):
    """Verify cross-citations service returns list (route requires full app state)."""
    await _seed_channel(db, channel_id=100123, title="API Cross")
    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30)
    assert isinstance(data, list)


# ── heatmap: weekday / hour precision tests ────────────────────────────


@pytest.mark.asyncio
async def test_heatmap_specific_weekday_hour_values(db):
    """Verify heatmap returns correct weekday (%w) and hour values."""
    await _seed_channel(db, channel_id=100123, title="Heatmap Precise")
    # Use dates within the last 30 days to ensure they fall in the query window.
    # Find a recent Monday and Wednesday.
    now = datetime.now(timezone.utc)
    # Monday of the current week
    mon = now - timedelta(days=now.weekday())
    # Wednesday of the current week
    wed = mon + timedelta(days=2)
    mon_10 = mon.replace(hour=10, minute=30, second=0, microsecond=0)
    wed_22 = wed.replace(hour=22, minute=0, second=0, microsecond=0)
    messages = [
        Message(channel_id=100123, message_id=1, text="mon 10:30", date=mon_10),
        Message(channel_id=100123, message_id=2, text="wed 22", date=wed_22),
    ]
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_heatmap(100123, days=30)

    # Build a lookup (weekday, hour) -> count
    lookup = {(r["weekday"], r["hour"]): r["count"] for r in data}

    # Monday is %w=1, hour=10 (date was Monday so strftime %w = 1)
    assert lookup.get((1, 10)) == 1
    # Wednesday is %w=3, hour=22
    assert lookup.get((3, 22)) == 1
    # Empty cells should be absent
    assert (0, 0) not in lookup or lookup.get((0, 0)) is None  # Sunday midnight unlikely


@pytest.mark.asyncio
async def test_heatmap_aggregation_multiple_same_slot(db):
    """Multiple messages in the same (weekday, hour) slot aggregate."""
    await _seed_channel(db, channel_id=100123, title="Heatmap Agg")
    # All messages on same hour of the current day
    now = datetime.now(timezone.utc)
    base = now.replace(hour=14, minute=0, second=0, microsecond=0)
    messages = [
        Message(channel_id=100123, message_id=i, text=f"msg {i}",
                date=base + timedelta(minutes=i * 5))
        for i in range(10)
    ]
    await _seed_messages_batch(db, 100123, messages)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_heatmap(100123, days=7)
    lookup = {(r["weekday"], r["hour"]): r["count"] for r in data}
    # All 10 land in hour 14; weekday depends on today
    today_wd = int(now.strftime("%w"))
    assert lookup.get((today_wd, 14)) == 10
    # Total messages should be exactly 10
    assert sum(r["count"] for r in data) == 10


# ── cross-channel citation edge cases ─────────────────────────────────


@pytest.mark.asyncio
async def test_cross_citations_date_filtering(db):
    """Messages outside the days window are excluded from cross-citations."""
    await _seed_channel(db, channel_id=100500, title="Source")
    await _seed_channel(db, channel_id=100123, title="Target")
    now = datetime.now(timezone.utc)

    # Recent forwarded message
    msg_recent = Message(
        channel_id=100123, message_id=1, text="recent fwd",
        date=now - timedelta(hours=1),
    )
    msg_recent.forward_from_channel_id = 100500
    await db.insert_message(msg_recent)

    # Old forwarded message (outside 7-day window)
    msg_old = Message(
        channel_id=100123, message_id=2, text="old fwd",
        date=now - timedelta(days=30),
    )
    msg_old.forward_from_channel_id = 100500
    await db.insert_message(msg_old)

    svc = ChannelAnalyticsService(db)
    data_7d = await svc.get_cross_channel_citations(100123, days=7)
    data_90d = await svc.get_cross_channel_citations(100123, days=90)

    assert len(data_7d) == 1
    assert data_7d[0]["citation_count"] == 1  # only recent

    assert len(data_90d) == 1
    assert data_90d[0]["citation_count"] == 2  # both


@pytest.mark.asyncio
async def test_cross_citations_unknown_source(db):
    """Cross-citations work even when source channel is not in channels table."""
    await _seed_channel(db, channel_id=100123, title="Target Only")
    now = datetime.now(timezone.utc)

    # Forward from a channel that doesn't exist in DB
    msg = Message(
        channel_id=100123, message_id=1, text="fwd from unknown",
        date=now,
    )
    msg.forward_from_channel_id = 999999
    await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30)
    assert len(data) == 1
    assert data[0]["source_channel_id"] == 999999
    assert data[0]["source_title"] is None  # no matching channel row
    assert data[0]["source_username"] is None
    assert data[0]["citation_count"] == 1


@pytest.mark.asyncio
async def test_cross_citations_respects_limit(db):
    """Limit parameter caps the number of returned source channels."""
    await _seed_channel(db, channel_id=100123, title="Target")
    now = datetime.now(timezone.utc)
    for i in range(15):
        msg = Message(
            channel_id=100123, message_id=100 + i,
            text=f"fwd {i}", date=now,
        )
        msg.forward_from_channel_id = 200 + i
        await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30, limit=5)
    assert len(data) == 5


@pytest.mark.asyncio
async def test_cross_citations_only_includes_forwards(db):
    """Regular messages (no forward_from_channel_id) are not counted."""
    await _seed_channel(db, channel_id=100123, title="No Fwd Target")
    now = datetime.now(timezone.utc)
    # 5 regular messages
    for i in range(5):
        await db.insert_message(Message(
            channel_id=100123, message_id=100 + i,
            text=f"regular {i}", date=now,
        ))

    svc = ChannelAnalyticsService(db)
    data = await svc.get_cross_channel_citations(100123, days=30)
    assert data == []


# ── service-level heatmap / citation integration ──────────────────────


@pytest.mark.asyncio
async def test_heatmap_service_delegates_to_repo(db):
    """Service get_heatmap delegates correctly and returns repo output."""
    await _seed_channel(db, channel_id=100123, title="Delegator")
    now = datetime.now(timezone.utc)
    await db.insert_message(Message(
        channel_id=100123, message_id=1, text="x",
        date=now.replace(hour=15, minute=0, second=0, microsecond=0),
    ))

    svc = ChannelAnalyticsService(db)
    svc_data = await svc.get_heatmap(100123, days=7)
    repo_data = await db.repos.messages.get_hour_weekday_heatmap(100123, days=7)
    assert svc_data == repo_data


@pytest.mark.asyncio
async def test_cross_citations_service_delegates_to_repo(db):
    """Service get_cross_channel_citations delegates correctly."""
    await _seed_channel(db, channel_id=100123, title="Delegator")
    svc = ChannelAnalyticsService(db)
    svc_data = await svc.get_cross_channel_citations(100123, days=7)
    repo_data = await db.repos.messages.get_cross_channel_citations(100123, days=7)
    assert svc_data == repo_data


# ── repo-level heatmap edge cases ─────────────────────────────────────


@pytest.mark.asyncio
async def test_repo_heatmap_all_hours_covered(db):
    """Messages spanning many hours produce correct hour range."""
    await _seed_channel(db, channel_id=100123, title="All Hours")
    now = datetime.now(timezone.utc)
    messages = []
    for h in range(0, 24, 3):  # hours 0, 3, 6, 9, 12, 15, 18, 21
        messages.append(Message(
            channel_id=100123, message_id=h + 1, text=f"h{h}",
            date=now.replace(hour=h, minute=0, second=0, microsecond=0),
        ))
    await _seed_messages_batch(db, 100123, messages)

    rows = await db.repos.messages.get_hour_weekday_heatmap(100123, days=7)
    hours_seen = {r["hour"] for r in rows}
    for h in range(0, 24, 3):
        assert h in hours_seen


@pytest.mark.asyncio
async def test_repo_heatmap_no_messages_returns_empty(db):
    """Heatmap for a channel with no messages returns empty list."""
    await _seed_channel(db, channel_id=100123, title="Empty Heat")
    rows = await db.repos.messages.get_hour_weekday_heatmap(100123, days=30)
    assert rows == []


@pytest.mark.asyncio
async def test_repo_cross_citations_multiple_from_same_source(db):
    """Multiple forwarded messages from the same source are aggregated."""
    await _seed_channel(db, channel_id=100500, title="Agg Source")
    await _seed_channel(db, channel_id=100123, title="Agg Target")
    now = datetime.now(timezone.utc)
    for i in range(7):
        msg = Message(
            channel_id=100123, message_id=100 + i,
            text=f"agg fwd {i}", date=now - timedelta(hours=i),
        )
        msg.forward_from_channel_id = 100500
        await db.insert_message(msg)

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert len(rows) == 1
    assert rows[0]["source_channel_id"] == 100500
    assert rows[0]["citation_count"] == 7


@pytest.mark.asyncio
async def test_repo_cross_citations_latest_date(db):
    """latest_date is the most recent forwarded message date."""
    await _seed_channel(db, channel_id=100500, title="Src Latest")
    await _seed_channel(db, channel_id=100123, title="Tgt Latest")
    now = datetime.now(timezone.utc)

    # Older forward
    msg1 = Message(
        channel_id=100123, message_id=1, text="old",
        date=now - timedelta(days=5),
    )
    msg1.forward_from_channel_id = 100500
    await db.insert_message(msg1)

    # Newer forward
    msg2 = Message(
        channel_id=100123, message_id=2, text="new",
        date=now - timedelta(hours=2),
    )
    msg2.forward_from_channel_id = 100500
    await db.insert_message(msg2)

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert len(rows) == 1
    latest = rows[0]["latest_date"]
    assert latest is not None
    # The latest_date should be closer to now than 5 days ago
    latest_dt = datetime.fromisoformat(latest)
    assert (now - latest_dt).total_seconds() < 86400  # within 24h


# ── CLI analytics channel command test ────────────────────────────────


@pytest.mark.aiosqlite_serial
def test_cli_analytics_channel(cli_db, capsys):
    """End-to-end test for `analytics channel <channel_id>` CLI command."""
    from src.cli.commands.analytics import run as analytics_run

    # Seed data synchronously via the db
    async def _seed():
        await cli_db.add_channel(Channel(
            channel_id=100123, title="CLI Channel", username="cli_chan",
        ))
        await cli_db.save_channel_stats(ChannelStats(
            channel_id=100123, subscriber_count=5000, avg_views=300.0,
            avg_reactions=15.0, avg_forwards=8.0,
        ))
        now = datetime.now(timezone.utc)
        for i in range(3):
            msg = Message(
                channel_id=100123, message_id=100 + i,
                text=f"cli msg {i}",
                views=200, forwards=5, reply_count=2,
                date=now - timedelta(hours=i),
            )
            await cli_db.insert_message(msg)
        # Forward from another channel
        await cli_db.add_channel(Channel(
            channel_id=100500, title="CLI Source", username="cli_src",
        ))
        fwd_msg = Message(
            channel_id=100123, message_id=999,
            text="forwarded msg", views=100,
            date=now - timedelta(hours=1),
        )
        fwd_msg.forward_from_channel_id = 100500
        await cli_db.insert_message(fwd_msg)

    asyncio.run(_seed())

    config = AppConfig()

    async def fake_init_db(_config_path):
        return config, cli_db

    with patch("src.cli.commands.analytics.runtime.init_db", side_effect=fake_init_db):
        analytics_run(argparse.Namespace(
            config="config.yaml",
            analytics_action="channel",
            channel_id=100123,
            days=30,
        ))

    out = capsys.readouterr().out
    assert "CLI Channel" in out
    assert "5000" in out
    assert "ERR:" in out
    assert "Citations (forwards)" in out
    assert "CLI Source" in out
    assert "Heatmap" in out


@pytest.mark.aiosqlite_serial
def test_cli_analytics_channel_not_found(cli_db, capsys):
    """CLI analytics channel for nonexistent channel prints not found."""
    from src.cli.commands.analytics import run as analytics_run

    config = AppConfig()

    async def fake_init_db(_config_path):
        return config, cli_db

    with patch("src.cli.commands.analytics.runtime.init_db", side_effect=fake_init_db):
        analytics_run(argparse.Namespace(
            config="config.yaml",
            analytics_action="channel",
            channel_id=999999,
            days=30,
        ))

    out = capsys.readouterr().out
    assert "not found" in out.lower()
