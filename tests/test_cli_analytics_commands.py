"""Tests for src/cli/commands/analytics.py — CLI analytics subcommands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.analytics import run


def _fake_asyncio_run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_db(**overrides):
    db = MagicMock()
    db.close = AsyncMock()
    db.get_top_messages = AsyncMock(return_value=[])
    db.get_engagement_by_media_type = AsyncMock(return_value=[])
    db.get_hourly_activity = AsyncMock(return_value=[])
    db.search_messages = AsyncMock(return_value=([], 0))
    for k, v in overrides.items():
        setattr(db, k, v)
    return db


def _make_config():
    return MagicMock()


def _init_patches(db, config=_make_config()):
    return (
        patch("src.cli.commands.analytics.runtime.init_db", AsyncMock(return_value=(config, db))),
        patch("asyncio.run", _fake_asyncio_run),
    )


# ---------------------------------------------------------------------------
# top
# ---------------------------------------------------------------------------


def test_top_empty(capsys):
    db = _make_db()
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="top", limit=10, date_from=None, date_to=None))
    assert "No messages" in capsys.readouterr().out


def test_top_with_data(capsys):
    rows = [{"channel_title": "Test", "channel_username": None, "channel_id": 100,
             "text": "Hello world", "date": "2024-01-01 12:00", "total_reactions": 5}]
    db = _make_db(get_top_messages=AsyncMock(return_value=rows))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="top", limit=10, date_from=None, date_to=None))
    out = capsys.readouterr().out
    assert "Test" in out
    assert "5" in out


# ---------------------------------------------------------------------------
# content-types
# ---------------------------------------------------------------------------


def test_content_types_empty(capsys):
    db = _make_db()
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="content-types", date_from=None, date_to=None))
    assert "No data" in capsys.readouterr().out


def test_content_types_with_data(capsys):
    rows = [{"content_type": "text", "message_count": 50, "avg_reactions": 2.5}]
    db = _make_db(get_engagement_by_media_type=AsyncMock(return_value=rows))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="content-types", date_from=None, date_to=None))
    assert "text" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# hourly
# ---------------------------------------------------------------------------


def test_hourly_empty(capsys):
    db = _make_db()
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="hourly", date_from=None, date_to=None))
    assert "No data" in capsys.readouterr().out


def test_hourly_with_data(capsys):
    rows = [{"hour": 14, "message_count": 100, "avg_reactions": 3.0}]
    db = _make_db(get_hourly_activity=AsyncMock(return_value=rows))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="hourly", date_from=None, date_to=None))
    assert "14:00" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def test_summary(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_summary = AsyncMock(return_value={
        "total_generations": 100, "total_published": 80,
        "total_pending": 10, "total_rejected": 10, "pipelines_count": 5,
    })
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="summary"))
    out = capsys.readouterr().out
    assert "100" in out
    assert "pipelines" in out.lower()


# ---------------------------------------------------------------------------
# pipeline-stats
# ---------------------------------------------------------------------------


def test_pipeline_stats_empty(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_pipeline_stats = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="pipeline-stats", pipeline_id=None))
    assert "No pipeline stats" in capsys.readouterr().out


def test_pipeline_stats_with_data(capsys):
    db = _make_db()
    s = MagicMock(pipeline_name="TestPipe", total_generations=10, total_published=8,
                  total_rejected=1, pending_moderation=1, success_rate=0.8)
    mock_svc = MagicMock()
    mock_svc.get_pipeline_stats = AsyncMock(return_value=[s])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="pipeline-stats", pipeline_id=None))
    assert "TestPipe" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# daily
# ---------------------------------------------------------------------------


def test_daily_empty(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_daily_stats = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="daily", days=30, pipeline_id=None))
    assert "No data" in capsys.readouterr().out


def test_daily_with_data(capsys):
    db = _make_db()
    rows = [{"date": "2024-01-01", "count": 5, "published": 3}]
    mock_svc = MagicMock()
    mock_svc.get_daily_stats = AsyncMock(return_value=rows)
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="daily", days=30, pipeline_id=None))
    assert "2024-01-01" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# trending-topics
# ---------------------------------------------------------------------------


def test_trending_topics_empty(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_trending_topics = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="trending-topics", days=7, limit=20))
    assert "No trending" in capsys.readouterr().out


def test_trending_topics_with_data(capsys):
    db = _make_db()
    t = MagicMock(keyword="python", count=42)
    mock_svc = MagicMock()
    mock_svc.get_trending_topics = AsyncMock(return_value=[t])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="trending-topics", days=7, limit=20))
    assert "python" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# trending-channels
# ---------------------------------------------------------------------------


def test_trending_channels_with_data(capsys):
    db = _make_db()
    ch = MagicMock(title="NewsCh", count=100)
    mock_svc = MagicMock()
    mock_svc.get_trending_channels = AsyncMock(return_value=[ch])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="trending-channels", days=7, limit=20))
    assert "NewsCh" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# velocity
# ---------------------------------------------------------------------------


def test_velocity_with_data(capsys):
    db = _make_db()
    v = MagicMock(date="2024-01-01", count=50)
    mock_svc = MagicMock()
    mock_svc.get_message_velocity = AsyncMock(return_value=[v])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="velocity", days=30))
    assert "2024-01-01" in capsys.readouterr().out


def test_velocity_empty(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_message_velocity = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="velocity", days=30))
    assert "No velocity" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# peak-hours
# ---------------------------------------------------------------------------


def test_peak_hours_with_data(capsys):
    db = _make_db()
    h = MagicMock(hour=14, count=200)
    mock_svc = MagicMock()
    mock_svc.get_peak_hours = AsyncMock(return_value=[h])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="peak-hours"))
    out = capsys.readouterr().out
    assert "14:00" in out


def test_peak_hours_empty(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_peak_hours = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="peak-hours"))
    assert "No peak" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# calendar
# ---------------------------------------------------------------------------


def test_calendar_empty(capsys):
    db = _make_db()
    mock_svc = MagicMock()
    mock_svc.get_upcoming = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_calendar_service.ContentCalendarService", return_value=mock_svc):
        run(_args(analytics_action="calendar", limit=20, pipeline_id=None))
    assert "No upcoming" in capsys.readouterr().out


def test_calendar_with_data(capsys):
    db = _make_db()
    e = MagicMock(run_id=1, pipeline_name="Pipe", moderation_status="pending",
                  scheduled_time="2024-01-01 12:00", created_at="2024-01-01", preview="Hello")
    mock_svc = MagicMock()
    mock_svc.get_upcoming = AsyncMock(return_value=[e])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.content_calendar_service.ContentCalendarService", return_value=mock_svc):
        run(_args(analytics_action="calendar", limit=20, pipeline_id=None))
    out = capsys.readouterr().out
    assert "Pipe" in out


# ---------------------------------------------------------------------------
# trending-emojis
# ---------------------------------------------------------------------------


def test_trending_emojis_no_messages(capsys):
    db = _make_db(search_messages=AsyncMock(return_value=([], 0)))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="trending-emojis", days=7, limit=20))
    assert "No emojis" in capsys.readouterr().out


def test_trending_emojis_with_emojis(capsys):
    msg = MagicMock(text="Hello 🎉 world 🌍 test 🎉")
    db = _make_db(search_messages=AsyncMock(return_value=([msg], 1)))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="trending-emojis", days=7, limit=20))
    out = capsys.readouterr().out
    assert "🎉" in out


# ---------------------------------------------------------------------------
# channel
# ---------------------------------------------------------------------------


def test_channel_not_found(capsys):
    db = _make_db()
    ov = MagicMock(title=None, username=None)
    mock_svc = MagicMock()
    mock_svc.get_channel_overview = AsyncMock(return_value=ov)
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.channel_analytics_service.ChannelAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="channel", channel_id=999, days=30))
    assert "not found" in capsys.readouterr().out


def test_channel_found(capsys):
    db = _make_db()
    ov = MagicMock(
        title="TestCh", username="testch", subscriber_count=1000,
        subscriber_delta_week=50, subscriber_delta_month=200,
        err=5.5, err24=3.2, total_posts=500,
        posts_today=5, posts_week=30, posts_month=100,
        avg_views=500, avg_forwards=10, avg_reactions=25,
    )
    cit = MagicMock(total_forwards=100, post_count=50, avg_forwards=2.0)
    mock_svc = MagicMock()
    mock_svc.get_channel_overview = AsyncMock(return_value=ov)
    mock_svc.get_citation_stats = AsyncMock(return_value=cit)
    mock_svc.get_cross_channel_citations = AsyncMock(return_value=[])
    mock_svc.get_heatmap = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("src.services.channel_analytics_service.ChannelAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="channel", channel_id=100, days=30))
    out = capsys.readouterr().out
    assert "TestCh" in out
    assert "1000" in out
