from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

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


async def _seed_channel(db, channel_id=-100123, title="Test Channel", username="test_chan"):
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
    await _seed_channel(db, channel_id=-100111, title="Active 1", username="active1")
    await _seed_channel(db, channel_id=-100222, title="Active 2", username="active2")
    ch3 = await _seed_channel(db, channel_id=-100333, title="Filtered", username="filtered_chan")
    await db.set_channel_filtered(ch3.id, True)

    svc = ChannelAnalyticsService(db)
    channels = await svc.get_active_channels()
    assert len(channels) == 2
    ids = {c.channel_id for c in channels}
    assert -100111 in ids
    assert -100222 in ids
    assert -100333 not in ids
    assert all(isinstance(c, ChannelListItem) for c in channels)


@pytest.mark.asyncio
async def test_get_active_channels_empty(db):
    svc = ChannelAnalyticsService(db)
    channels = await svc.get_active_channels()
    assert channels == []


@pytest.mark.asyncio
async def test_get_channel_overview_basic(db):
    await _seed_channel(db, channel_id=-100123, title="Overview Chan", username="ov_chan")
    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(-100123)
    assert isinstance(overview, ChannelOverview)
    assert overview.channel_id == -100123
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
    await _seed_channel(db, channel_id=-100123, title="Sub Chan")
    # Two stats entries for delta calculation
    await _seed_stats(db, -100123, subscriber_count=1000, avg_views=500.0)
    await _seed_stats(db, -100123, subscriber_count=1200, avg_views=600.0)

    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(-100123)
    assert overview.subscriber_count == 1200
    assert overview.subscriber_delta == 200  # 1200 - 1000
    assert overview.avg_views == 600.0
    assert overview.avg_forwards == 10.0
    assert overview.avg_reactions == 25.0


@pytest.mark.asyncio
async def test_get_channel_overview_with_posts(db):
    await _seed_channel(db, channel_id=-100123, title="Post Chan")
    await _seed_stats(db, -100123, subscriber_count=500, avg_views=200.0)
    now = datetime.now(timezone.utc)

    messages = []
    # 5 today
    for i in range(5):
        messages.append(Message(
            channel_id=-100123,
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
            channel_id=-100123,
            message_id=200 + i,
            text=f"week msg {i}",
            views=30 + i,
            forwards=i,
            reply_count=0,
            date=now - timedelta(days=5),
        ))
    # 1 old message from 35 days ago
    messages.append(Message(
        channel_id=-100123,
        message_id=300,
        text="old msg",
        views=10,
        forwards=0,
        reply_count=0,
        date=now - timedelta(days=35),
    ))
    await _seed_messages_batch(db, -100123, messages)

    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(-100123, days=30)
    assert overview.total_posts == 9
    assert overview.posts_today == 5
    assert overview.posts_week == 8
    assert overview.posts_month == 8  # 5 today + 3 this week (within 30d)

    overview7 = await svc.get_channel_overview(-100123, days=7)
    assert overview7.posts_month == 8  # 5 today + 3 this week

    overview1 = await svc.get_channel_overview(-100123, days=1)
    assert overview1.posts_month == 5  # 5 within last day


@pytest.mark.asyncio
async def test_get_channel_overview_missing_channel(db):
    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(-999999)
    assert overview.channel_id == -999999
    assert overview.title is None
    assert overview.username is None


@pytest.mark.asyncio
async def test_get_subscriber_history(db):
    await _seed_channel(db, channel_id=-100123, title="Hist Chan")
    for i in range(5):
        await db.save_channel_stats(ChannelStats(
            channel_id=-100123,
            subscriber_count=100 + i * 50,
        ))

    svc = ChannelAnalyticsService(db)
    history = await svc.get_subscriber_history(-100123, days=5)
    assert len(history) == 5
    # Verify chronological order
    counts = [entry["subscriber_count"] for entry in history]
    assert counts == sorted(counts)
    assert counts[-1] == 300


@pytest.mark.asyncio
async def test_get_views_timeseries(db):
    await _seed_channel(db, channel_id=-100123, title="Views Chan")
    now = datetime.now(timezone.utc)
    messages = []
    for i in range(3):
        ts = now - timedelta(days=i)
        messages.append(Message(
            channel_id=-100123,
            message_id=10 + i,
            text=f"msg {i}",
            views=100 * (i + 1),
            date=ts,
        ))
    await _seed_messages_batch(db, -100123, messages)

    svc = ChannelAnalyticsService(db)
    ts = await svc.get_views_timeseries(-100123, days=5)
    assert len(ts) >= 1
    for entry in ts:
        assert "day" in entry
        assert "message_count" in entry
        assert "avg_views" in entry


@pytest.mark.asyncio
async def test_get_post_frequency(db):
    await _seed_channel(db, channel_id=-100123, title="Freq Chan")
    now = datetime.now(timezone.utc)
    messages = []
    # 3 messages today
    for i in range(3):
        messages.append(Message(
            channel_id=-100123,
            message_id=100 + i,
            text=f"today msg {i}",
            views=10,
            date=now - timedelta(hours=i),
        ))
    # 2 yesterday
    for i in range(2):
        messages.append(Message(
            channel_id=-100123,
            message_id=200 + i,
            text=f"yesterday msg {i}",
            date=now - timedelta(days=1),
        ))
    await _seed_messages_batch(db, -100123, messages)

    svc = ChannelAnalyticsService(db)
    freq = await svc.get_post_frequency(-100123, days=7)
    assert len(freq) == 2
    today_count = sum(e["count"] for e in freq if e["day"] == now.strftime("%Y-%m-%d"))
    assert today_count == 3
    yesterday_count = sum(
        e["count"] for e in freq
        if e["day"] == (now - timedelta(days=1)).strftime("%Y-%m-%d")
    )
    assert yesterday_count == 2


@pytest.mark.asyncio
async def test_get_citation_stats(db):
    await _seed_channel(db, channel_id=-100123, title="Cite Chan")
    messages = []
    for i in range(5):
        messages.append(Message(
            channel_id=-100123,
            message_id=100 + i,
            text=f"msg {i}",
            views=100,
            forwards=i * 10,
            date=datetime(2025, 1, 15, 12, 0, 0),
        ))
    await _seed_messages_batch(db, -100123, messages)

    svc = ChannelAnalyticsService(db)
    stats = await svc.get_citation_stats(-100123)
    assert isinstance(stats, CitationStats)
    assert stats.post_count == 5
    assert stats.total_forwards == sum(range(5)) * 10  # 0+10+20+30+40 = 100
    assert stats.avg_forwards == 20.0  # 100 / 5


@pytest.mark.asyncio
async def test_get_err(db):
    await _seed_channel(db, channel_id=-100123, title="ERR Chan")
    await _seed_stats(db, -100123, subscriber_count=1000, avg_views=200.0)
    messages = []
    for i in range(5):
        messages.append(Message(
            channel_id=-100123,
            message_id=100 + i,
            text=f"msg {i}",
            views=100,
            forwards=5,
            reply_count=2,
            date=datetime(2025, 1, 15, 12, 0, 0),
        ))
    await _seed_messages_batch(db, -100123, messages)

    svc = ChannelAnalyticsService(db)
    err = await svc.get_err(-100123)
    assert isinstance(err, float)
    assert err > 0
    # ERR = total_engagement / (num_posts * subscribers) * 100
    # total_engagement = 5 * (100 + 5 + 2) = 535
    # err = 535 / (5 * 1000) * 100 = 10.7
    assert abs(err - 10.7) < 0.1


@pytest.mark.asyncio
async def test_get_err_no_subscribers(db):
    await _seed_channel(db, channel_id=-100123, title="No Sub Chan")
    svc = ChannelAnalyticsService(db)
    err = await svc.get_err(-100123)
    assert err is None


@pytest.mark.asyncio
async def test_get_err24(db):
    await _seed_channel(db, channel_id=-100123, title="ERR24 Chan")
    await _seed_stats(db, -100123, subscriber_count=500)
    now = datetime.now(timezone.utc)
    msg = Message(
        channel_id=-100123,
        message_id=1,
        text="recent msg",
        views=200,
        forwards=10,
        reply_count=5,
        date=now - timedelta(hours=1),
    )
    await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    err24 = await svc.get_err24(-100123)
    assert isinstance(err24, float)
    assert err24 > 0
    # engagement = 200 + 10 + 5 = 215; err24 = 215 / (1 * 500) * 100 = 43.0
    assert abs(err24 - 43.0) < 0.1


@pytest.mark.asyncio
async def test_get_err24_no_recent_posts(db):
    await _seed_channel(db, channel_id=-100123, title="Empty ERR24")
    await _seed_stats(db, -100123, subscriber_count=500)
    msg = Message(
        channel_id=-100123,
        message_id=1,
        text="old msg",
        views=50,
        date=datetime.now(timezone.utc) - timedelta(days=2),
    )
    await db.insert_message(msg)

    svc = ChannelAnalyticsService(db)
    err24 = await svc.get_err24(-100123)
    assert err24 is None


@pytest.mark.asyncio
async def test_get_hourly_activity(db):
    await _seed_channel(db, channel_id=-100123, title="Hourly Chan")
    now = datetime.now(timezone.utc)
    messages = []
    for hour in [8, 8, 8, 14, 14, 20]:
        messages.append(Message(
            channel_id=-100123,
            message_id=len(messages) + 1,
            text=f"msg at {hour}",
            date=now.replace(hour=hour, minute=0, second=0, microsecond=0),
        ))
    await _seed_messages_batch(db, -100123, messages)

    svc = ChannelAnalyticsService(db)
    activity = await svc.get_hourly_activity(-100123, days=7)
    assert len(activity) > 0
    hour_counts = {e["hour"]: e["count"] for e in activity}
    assert hour_counts.get(8) == 3
    assert hour_counts.get(14) == 2
    assert hour_counts.get(20) == 1


@pytest.mark.asyncio
async def test_get_ranked_channels_by_err(db):
    await _seed_channel(db, channel_id=-100111, title="Low ERR", username="low_err")
    await _seed_channel(db, channel_id=-100222, title="High ERR", username="high_err")
    await _seed_stats(db, -100111, subscriber_count=1000, avg_views=100.0)
    await _seed_stats(db, -100222, subscriber_count=1000, avg_views=500.0)

    for i in range(3):
        msg = Message(
            channel_id=-100111,
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
            channel_id=-100222,
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
    assert rankings[0].channel_id == -100222
    assert rankings[0].rank == 1
    assert rankings[0].score > rankings[1].score
    assert rankings[1].channel_id == -100111
    assert rankings[1].rank == 2


@pytest.mark.asyncio
async def test_get_ranked_channels_by_subscriber_count(db):
    await _seed_channel(db, channel_id=-100111, title="Small", username="small")
    await _seed_channel(db, channel_id=-100222, title="Big", username="big")
    await _seed_stats(db, -100111, subscriber_count=100)
    await _seed_stats(db, -100222, subscriber_count=10000)

    svc = ChannelAnalyticsService(db)
    rankings = await svc.get_ranked_channels(metric="subscriber_count")
    assert len(rankings) == 2
    assert rankings[0].channel_id == -100222
    assert rankings[0].score == 10000.0
    assert rankings[1].channel_id == -100111
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
    await _seed_channel(db, channel_id=-100111, title="Chan A", username="chan_a")
    await _seed_channel(db, channel_id=-100222, title="Chan B", username="chan_b")
    await _seed_stats(db, -100111, subscriber_count=1000)
    await _seed_stats(db, -100222, subscriber_count=2000)

    svc = ChannelAnalyticsService(db)
    comparison = await svc.get_channel_comparison([-100111, -100222])
    assert isinstance(comparison, ChannelComparison)
    assert len(comparison.channels) == 2
    assert comparison.channels[0].channel_id == -100111
    assert comparison.channels[1].channel_id == -100222
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
    await _seed_channel(db, channel_id=-100111, title="Unknown Metric Chan")
    await _seed_stats(db, -100111, subscriber_count=500)

    svc = ChannelAnalyticsService(db)
    rankings = await svc.get_ranked_channels(metric="nonexistent_metric")
    assert len(rankings) == 1
    assert rankings[0].score == 0.0


@pytest.mark.asyncio
async def test_overview_to_dict(db):
    """Verify dataclass serialization works for template rendering."""
    await _seed_channel(db, channel_id=-100123, title="Dict Chan")
    svc = ChannelAnalyticsService(db)
    overview = await svc.get_channel_overview(-100123)
    d = dataclasses.asdict(overview)
    assert isinstance(d, dict)
    assert d["channel_id"] == -100123
    assert d["title"] == "Dict Chan"
    assert "subscriber_count" in d
    assert "err" in d
    assert "posts_today" in d
