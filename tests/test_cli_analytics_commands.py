"""Tests for src/cli/commands/analytics.py — CLI analytics subcommands."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tests.helpers import cli_ns, fake_asyncio_run, make_cli_config, make_cli_db
from tg_agent.cli.commands.analytics import run


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return cli_ns(**defaults)


def _init_patches(db, config=None):
    config = config or make_cli_config()
    return (
        patch("tg_agent.cli.commands.analytics.runtime.init_db", AsyncMock(return_value=(config, db))),
        patch("asyncio.run", fake_asyncio_run),
    )


# ---------------------------------------------------------------------------
# top
# ---------------------------------------------------------------------------


def test_top_empty(capsys):
    db = make_cli_db()
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="top", limit=10, date_from=None, date_to=None))
    assert "No messages" in capsys.readouterr().out


def test_top_with_data(capsys):
    rows = [{"channel_title": "Test", "channel_username": None, "channel_id": 100,
             "text": "Hello world", "date": "2024-01-01 12:00", "total_reactions": 5}]
    db = make_cli_db(get_top_messages=AsyncMock(return_value=rows))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="top", limit=10, date_from=None, date_to=None))
    out = capsys.readouterr().out
    assert "Test" in out
    assert "5" in out


# ---------------------------------------------------------------------------
# content-types
# ---------------------------------------------------------------------------


def test_content_types_empty(capsys):
    db = make_cli_db()
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="content-types", date_from=None, date_to=None))
    assert "No data" in capsys.readouterr().out


def test_content_types_with_data(capsys):
    rows = [{"content_type": "text", "message_count": 50, "avg_reactions": 2.5}]
    db = make_cli_db(get_engagement_by_media_type=AsyncMock(return_value=rows))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="content-types", date_from=None, date_to=None))
    assert "text" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# hourly
# ---------------------------------------------------------------------------


def test_hourly_empty(capsys):
    db = make_cli_db()
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="hourly", date_from=None, date_to=None))
    assert "No data" in capsys.readouterr().out


def test_hourly_with_data(capsys):
    rows = [{"hour": 14, "message_count": 100, "avg_reactions": 3.0}]
    db = make_cli_db(get_hourly_activity=AsyncMock(return_value=rows))
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_args(analytics_action="hourly", date_from=None, date_to=None))
    assert "14:00" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def test_summary(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_summary = AsyncMock(return_value={
        "total_generations": 100, "total_published": 80,
        "total_pending": 10, "total_rejected": 10, "pipelines_count": 5,
    })
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="summary"))
    out = capsys.readouterr().out
    assert "100" in out
    assert "pipelines" in out.lower()


# ---------------------------------------------------------------------------
# pipeline-stats
# ---------------------------------------------------------------------------


def test_pipeline_stats_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_pipeline_stats = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="pipeline-stats", pipeline_id=None))
    assert "No pipeline stats" in capsys.readouterr().out


def test_pipeline_stats_with_data(capsys):
    db = make_cli_db()
    s = MagicMock(pipeline_name="TestPipe", total_generations=10, total_published=8,
                  total_rejected=1, pending_moderation=1, success_rate=80.0)
    mock_svc = MagicMock()
    mock_svc.get_pipeline_stats = AsyncMock(return_value=[s])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="pipeline-stats", pipeline_id=None))
    assert "TestPipe" in capsys.readouterr().out


def test_pipeline_stats_rate_is_percent_not_remultiplied(capsys):
    # Issue #1616: the service returns success_rate already in percent units
    # (50.0 == 1 published out of 2, pinned by test_content_analytics_service).
    # The CLI must render it verbatim, not re-multiply it via format spec `:.0%`.
    cases = [(1, 2, 50.0, "50%", "5000%"), (0, 2, 0.0, "0%", None), (2, 2, 100.0, "100%", "10000%")]
    for published, total, rate, expected, wrong in cases:
        db = make_cli_db()
        s = MagicMock(pipeline_name="P", total_generations=total, total_published=published,
                      total_rejected=0, pending_moderation=0, success_rate=rate)
        mock_svc = MagicMock()
        mock_svc.get_pipeline_stats = AsyncMock(return_value=[s])
        with _init_patches(db)[0], _init_patches(db)[1], \
             patch("tg_agent.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
            run(_args(analytics_action="pipeline-stats", pipeline_id=None))
        out = capsys.readouterr().out
        assert expected in out, f"expected {expected!r} in output: {out!r}"
        if wrong is not None:
            assert wrong not in out, f"inflated {wrong!r} leaked into output: {out!r}"


# ---------------------------------------------------------------------------
# daily
# ---------------------------------------------------------------------------


def test_daily_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_daily_stats = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="daily", days=30, pipeline_id=None))
    assert "No data" in capsys.readouterr().out


def test_daily_with_data(capsys):
    from tg_agent.services.content_analytics_service import DailyStats

    db = make_cli_db()
    rows = [DailyStats(date="2024-01-01", generations=5, publications=3, rejections=0)]
    mock_svc = MagicMock()
    mock_svc.get_daily_stats = AsyncMock(return_value=rows)
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_analytics_service.ContentAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="daily", days=30, pipeline_id=None))
    assert "2024-01-01" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# trending-topics
# ---------------------------------------------------------------------------


def test_trending_topics_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_trending_topics = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="trending-topics", days=7, limit=20))
    assert "No trending" in capsys.readouterr().out


def test_trending_topics_with_data(capsys):
    db = make_cli_db()
    t = MagicMock(keyword="python", count=42)
    mock_svc = MagicMock()
    mock_svc.get_trending_topics = AsyncMock(return_value=[t])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="trending-topics", days=7, limit=20))
    assert "python" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# trending-channels
# ---------------------------------------------------------------------------


def test_trending_channels_with_data(capsys):
    db = make_cli_db()
    ch = MagicMock(title="NewsCh", message_count=100)
    mock_svc = MagicMock()
    mock_svc.get_trending_channels = AsyncMock(return_value=[ch])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="trending-channels", days=7, limit=20))
    assert "NewsCh" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# velocity
# ---------------------------------------------------------------------------


def test_velocity_with_data(capsys):
    db = make_cli_db()
    v = MagicMock(date="2024-01-01", count=50)
    mock_svc = MagicMock()
    mock_svc.get_message_velocity = AsyncMock(return_value=[v])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="velocity", days=30))
    assert "2024-01-01" in capsys.readouterr().out


def test_velocity_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_message_velocity = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="velocity", days=30))
    assert "No velocity" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# peak-hours
# ---------------------------------------------------------------------------


def test_peak_hours_with_data(capsys):
    db = make_cli_db()
    h = MagicMock(hour=14, count=200)
    mock_svc = MagicMock()
    mock_svc.get_peak_hours = AsyncMock(return_value=[h])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="peak-hours"))
    out = capsys.readouterr().out
    assert "14:00" in out


def test_peak_hours_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_peak_hours = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.trend_service.TrendService", return_value=mock_svc):
        run(_args(analytics_action="peak-hours"))
    assert "No peak" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# calendar
# ---------------------------------------------------------------------------


def test_calendar_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.get_upcoming = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_calendar_service.ContentCalendarService", return_value=mock_svc):
        run(_args(analytics_action="calendar", limit=20, pipeline_id=None))
    assert "No upcoming" in capsys.readouterr().out


def test_calendar_with_data(capsys):
    db = make_cli_db()
    e = MagicMock(run_id=1, pipeline_name="Pipe", moderation_status="pending",
                  scheduled_time="2024-01-01 12:00", created_at="2024-01-01", preview="Hello")
    mock_svc = MagicMock()
    mock_svc.get_upcoming = AsyncMock(return_value=[e])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.content_calendar_service.ContentCalendarService", return_value=mock_svc):
        run(_args(analytics_action="calendar", limit=20, pipeline_id=None))
    out = capsys.readouterr().out
    assert "Pipe" in out


# ---------------------------------------------------------------------------
# trending-emojis
# ---------------------------------------------------------------------------


def test_trending_emojis_no_reactions(capsys):
    db = make_cli_db()
    init_db_patch, run_patch = _init_patches(db)
    with init_db_patch, run_patch, patch("tg_agent.services.trend_service.TrendService") as mock_svc:
        mock_svc.return_value.get_trending_emojis = AsyncMock(return_value=[])
        run(_args(analytics_action="trending-emojis", days=7, limit=20))
    assert "No emoji reactions" in capsys.readouterr().out
    mock_svc.return_value.get_trending_emojis.assert_awaited_once_with(days=7, limit=20)


def test_trending_emojis_with_reaction_emojis(capsys):
    db = make_cli_db()
    rows = [SimpleNamespace(emoji="🎉", count=2), SimpleNamespace(emoji="🌍", count=1)]
    init_db_patch, run_patch = _init_patches(db)
    with init_db_patch, run_patch, patch("tg_agent.services.trend_service.TrendService") as mock_svc:
        mock_svc.return_value.get_trending_emojis = AsyncMock(return_value=rows)
        run(_args(analytics_action="trending-emojis", days=7, limit=20))
    out = capsys.readouterr().out
    assert "🎉" in out
    assert "2" in out


# ---------------------------------------------------------------------------
# channel
# ---------------------------------------------------------------------------


def test_channel_not_found(capsys):
    db = make_cli_db()
    ov = MagicMock(title=None, username=None)
    mock_svc = MagicMock()
    mock_svc.get_channel_overview = AsyncMock(return_value=ov)
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.channel_analytics_service.ChannelAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="channel", channel_id=999, days=30))
    assert "not found" in capsys.readouterr().out


def test_channel_found(capsys):
    db = make_cli_db()
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
         patch("tg_agent.services.channel_analytics_service.ChannelAnalyticsService", return_value=mock_svc):
        run(_args(analytics_action="channel", channel_id=100, days=30))
    out = capsys.readouterr().out
    assert "TestCh" in out
    assert "1000" in out


# ---------------------------------------------------------------------------
# channel-rating (read-only list of stored verdicts)
# ---------------------------------------------------------------------------


def test_channel_rating_empty(capsys):
    db = make_cli_db()
    mock_svc = MagicMock()
    mock_svc.list_ratings = AsyncMock(return_value=[])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.channel_analysis_service.ChannelAnalysisService", return_value=mock_svc):
        run(_args(analytics_action="channel-rating", useful=None, genre=None, limit=50))
    assert "No channel ratings" in capsys.readouterr().out


def test_channel_rating_with_data(capsys):
    from tg_agent.models import ChannelRating

    db = make_cli_db()
    rating = ChannelRating(
        channel_id=100, title="NewsCh", username="newsch",
        useful="useful", genre="original", confidence=0.91,
    )
    mock_svc = MagicMock()
    mock_svc.list_ratings = AsyncMock(return_value=[rating])
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.channel_analysis_service.ChannelAnalysisService", return_value=mock_svc):
        run(_args(analytics_action="channel-rating", useful="useful", genre=None, limit=50))
    out = capsys.readouterr().out
    assert "NewsCh" in out
    assert "useful" in out
    assert "original" in out
    mock_svc.list_ratings.assert_awaited_once_with(useful="useful", genre=None, limit=50)


# ---------------------------------------------------------------------------
# usefulness (контент-рейтинг «полезное vs вода», детерминированный, без LLM)
# ---------------------------------------------------------------------------


def _usefulness_args(**overrides):
    defaults = {"analytics_action": "usefulness", "limit": 100, "json": False}
    defaults.update(overrides)
    return cli_ns(**defaults)


def _usefulness_db(samples, channels=()):
    db = make_cli_db()
    db.filter_repo.fetch_recent_post_samples = AsyncMock(return_value=samples)
    db.get_channels = AsyncMock(return_value=list(channels))
    return db


def test_usefulness_empty(capsys):
    db = _usefulness_db({})
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_usefulness_args())
    out = capsys.readouterr().out
    assert "No channels with collected posts" in out
    db.filter_repo.fetch_recent_post_samples.assert_awaited_once()


def test_usefulness_table_sorted_by_tier(capsys):
    # Канал 1: вода (все посты со штампами), канал 2: не хватает выборки → no_data.
    # Формат сэмпла — сырой ответ репозитория: [(text, is_forward), …].
    water = [("В современном мире ИИ. " * 10, False)] * 40
    samples = {(-100): water, (-200): [("мало", False)]}
    channels = [
        SimpleNamespace(channel_id=-100, title="WaterCh", username="water", filter_flags=""),
        SimpleNamespace(channel_id=-200, title="TinyCh", username="tiny", filter_flags="auto_feed"),
    ]
    db = _usefulness_db(samples, channels)
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_usefulness_args())
    out = capsys.readouterr().out
    assert "WaterCh" in out and "water" in out
    assert "TinyCh" in out and "no_data" in out
    # Тир-сортировка: вода выше «нет данных».
    assert out.index("WaterCh") < out.index("TinyCh")


def test_usefulness_json(capsys):
    water = [("В современном мире ИИ. " * 10, False)] * 40
    channels = [SimpleNamespace(channel_id=-100, title="WaterCh", username="water", filter_flags="auto_feed")]
    db = _usefulness_db({(-100): water}, channels)
    with _init_patches(db)[0], _init_patches(db)[1]:
        run(_usefulness_args(json=True))
    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 1
    row = rows[0]
    assert row["channel_id"] == -100
    assert row["tier"] == "water"
    assert row["flags"] == "auto_feed"
    assert row["title"] == "WaterCh"


# ---------------------------------------------------------------------------
# channel-rate (#994: write path — run the LLM judge, upsert the verdict)
# ---------------------------------------------------------------------------


def _rate_args(**overrides):
    defaults = {
        "analytics_action": "channel-rate",
        "channel_id": 100,
        "model": None,
        "sample_size": 40,
    }
    defaults.update(overrides)
    return _args(**defaults)


def _make_provider_svc(*, has_providers=True, resolve=None, resolve_error=None):
    provider_svc = MagicMock()
    provider_svc.load_db_providers = AsyncMock(return_value=1 if has_providers else 0)
    provider_svc.has_providers = MagicMock(return_value=has_providers)
    if resolve_error is not None:
        provider_svc.resolve_provider_callable = MagicMock(side_effect=resolve_error)
    else:
        provider_svc.resolve_provider_callable = MagicMock(return_value=resolve or AsyncMock())
    return provider_svc


def _make_analysis_svc(*, rating=None, posts=("post",), classify_error=None):
    analysis_svc = MagicMock()
    analysis_svc.sample_posts = AsyncMock(return_value=list(posts))
    if classify_error is not None:
        analysis_svc.classify_channel = AsyncMock(side_effect=classify_error)
    else:
        analysis_svc.classify_channel = AsyncMock(return_value=rating)
    return analysis_svc


def _run_rate(db, provider_svc, analysis_svc, **arg_overrides):
    with _init_patches(db)[0], _init_patches(db)[1], \
         patch("tg_agent.services.provider_service.RuntimeProviderRegistry", return_value=provider_svc), \
         patch("tg_agent.services.channel_analysis_service.ChannelAnalysisService", return_value=analysis_svc):
        run(_rate_args(**arg_overrides))


def test_channel_rate_no_provider(capsys):
    """Without a configured provider the judge must not run (no spend, no write)."""
    db = make_cli_db()
    provider_svc = _make_provider_svc(has_providers=False)
    analysis_svc = _make_analysis_svc(rating=None)
    _run_rate(db, provider_svc, analysis_svc)
    out = capsys.readouterr().out
    assert "LLM provider is not configured" in out
    analysis_svc.classify_channel.assert_not_awaited()


def test_channel_rate_unknown_model_aborts(capsys):
    """A mistyped --model must abort loudly, not silently persist a stub verdict."""
    db = make_cli_db()
    provider_svc = _make_provider_svc(
        resolve_error=ValueError("Model/provider 'gpt-nope' is not registered. Available providers: cohere.")
    )
    analysis_svc = _make_analysis_svc(rating=None)
    _run_rate(db, provider_svc, analysis_svc, model="gpt-nope")
    out = capsys.readouterr().out
    assert "not registered" in out
    analysis_svc.classify_channel.assert_not_awaited()
    analysis_svc.sample_posts.assert_not_awaited()


def test_channel_rate_empty_channel_skips(capsys):
    """A channel with no posts must skip the provider call and the upsert."""
    db = make_cli_db()
    provider_svc = _make_provider_svc()
    analysis_svc = _make_analysis_svc(rating=None, posts=())
    _run_rate(db, provider_svc, analysis_svc)
    out = capsys.readouterr().out
    assert "no text posts to judge" in out
    analysis_svc.classify_channel.assert_not_awaited()


def test_channel_rate_provider_failure_exits_nonzero(capsys):
    """A provider/network failure surfaces a readable error and exits non-zero."""
    import pytest

    db = make_cli_db()
    provider_svc = _make_provider_svc()
    analysis_svc = _make_analysis_svc(classify_error=RuntimeError("boom: 503 from provider"))
    with pytest.raises(SystemExit) as excinfo:
        _run_rate(db, provider_svc, analysis_svc)
    assert excinfo.value.code == 1
    out = capsys.readouterr().out
    assert "Judge failed" in out
    assert "boom: 503" in out


def test_channel_rate_runs_judge(capsys):
    from tg_agent.models import ChannelRating

    db = make_cli_db()
    provider_callable = AsyncMock(return_value="{}")
    provider_svc = _make_provider_svc(resolve=provider_callable)
    rating = ChannelRating(
        channel_id=100, title="JudgedCh", username="judged",
        useful="useless", genre="ad", confidence=0.77,
        reason="реклама без сути", n_total=12,
    )
    analysis_svc = _make_analysis_svc(rating=rating, posts=("a", "b"))
    _run_rate(db, provider_svc, analysis_svc, model="gpt-4o-mini", sample_size=12)

    out = capsys.readouterr().out
    assert "JudgedCh" in out
    assert "useless" in out
    assert "ad" in out
    assert "0.77" in out
    assert "реклама без сути" in out
    provider_svc.resolve_provider_callable.assert_called_once_with("gpt-4o-mini")
    analysis_svc.classify_channel.assert_awaited_once_with(
        100, provider_callable=provider_callable, sample_size=12
    )


def test_channel_rate_sample_size_clamped():
    """--sample-size is clamped to [1, 200]."""
    from tg_agent.models import ChannelRating

    rating = ChannelRating(channel_id=100, useful="useful", genre="original")

    # Floor: 0 -> 1
    db = make_cli_db()
    analysis_svc = _make_analysis_svc(rating=rating)
    _run_rate(db, _make_provider_svc(), analysis_svc, sample_size=0)
    _, kwargs = analysis_svc.classify_channel.await_args
    assert kwargs["sample_size"] == 1

    # Ceiling: 10000 -> 200
    db = make_cli_db()
    analysis_svc = _make_analysis_svc(rating=rating)
    _run_rate(db, _make_provider_svc(), analysis_svc, sample_size=10000)
    _, kwargs = analysis_svc.classify_channel.await_args
    assert kwargs["sample_size"] == 200


# ---------------------------------------------------------------------------
# authority (#1748)
# ---------------------------------------------------------------------------


def _seed_authority_rows():
    from datetime import datetime, timedelta, timezone

    # Полдень UTC: обе метки в одних сутках (суточный бакет не рвёт кластер).
    now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    story = "Anthropic выпустила Claude 5.2 с режимом длинного контекста: модель держит весь репозиторий."
    return [
        {
            "channel_id": 100,
            "message_id": 1,
            "date": (now - timedelta(hours=3)).isoformat(),
            "text": story,
            "is_forward": False,
        },
        {
            "channel_id": 200,
            "message_id": 2,
            "date": (now - timedelta(hours=1)).isoformat(),
            "text": f"⚡ Коротко: {story} Подробности у первоисточника.",
            "is_forward": False,
        },
    ]


def _run_authority(db, **ns):
    db.get_channels = AsyncMock(return_value=[])
    db.repos.messages.fetch_authority_posts = AsyncMock(return_value=_seed_authority_rows())
    patches = _init_patches(db)
    with patches[0], patches[1]:
        run(_args(analytics_action="authority", days=30, apply=False, json=False, limit=100, **ns))


def test_authority_empty_window(capsys):
    db = make_cli_db()
    db.repos.messages.fetch_authority_posts = AsyncMock(return_value=[])
    patches = _init_patches(db)
    with patches[0], patches[1]:
        run(_args(analytics_action="authority", days=30, apply=False, json=False, limit=100))
    assert "No posts found in the last 30 days." in capsys.readouterr().out


def test_authority_report_scoop_first(capsys):
    db = make_cli_db()
    _run_authority(db)
    out = capsys.readouterr().out
    assert "authority window=30d posts=2" in out
    assert "tags_written=0 (dry-run)" in out
    # канал 100 напечатал первым — его строка выше (score 1.0 против 0.0)
    assert out.index("100") < out.index("200")
    assert "1.00" in out and "0.00" in out


def test_authority_json_envelope(capsys):
    db = make_cli_db()
    patches = _init_patches(db)
    with patches[0], patches[1]:
        db.get_channels = AsyncMock(return_value=[])
        db.repos.messages.fetch_authority_posts = AsyncMock(return_value=_seed_authority_rows())
        run(_args(analytics_action="authority", days=7, apply=False, json=True, limit=100))
    payload = json.loads(capsys.readouterr().out)
    assert payload["window_days"] == 7 and payload["posts"] == 2 and payload["channels"] == 2
    assert payload["applied"] == 0 and payload["tiers"] == {"A": 0, "B": 0, "C": 0}
    assert payload["rows"][0]["channel_id"] == 100 and payload["rows"][0]["scoop_rate"] == 1.0
    assert payload["rows"][0]["tier"] is None  # volume < 10 — без тира


def test_authority_apply_writes_tags(capsys):
    db = make_cli_db()
    db.get_channels = AsyncMock(return_value=[])
    db.repos.messages.fetch_authority_posts = AsyncMock(return_value=_seed_authority_rows())
    db.repos.channels.get_channel_by_channel_id = AsyncMock(return_value=SimpleNamespace(id=5))
    db.repos.channels.get_channel_tags = AsyncMock(return_value=["keep"])
    db.repos.channels.set_channel_tags = AsyncMock()
    patches = _init_patches(db)
    with patch("tg_agent.cli.commands.analytics.assign_tiers", return_value={100: "B", 200: "C"}):
        with patches[0], patches[1]:
            run(_args(analytics_action="authority", days=30, apply=True, json=False, limit=100))
    db.repos.channels.set_channel_tags.assert_any_await(5, ["keep", "auth:B"])
    assert "tags_written=2 (applied)" in capsys.readouterr().out
