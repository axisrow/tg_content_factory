"""Tests for agent tools: analytics.py MCP tools."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


class TestAnalyticsToolGetAnalyticsSummary:
    @pytest.mark.anyio
    async def test_returns_summary(self, mock_db):
        summary = {
            "total_generations": 100,
            "total_published": 60,
            "total_pending": 20,
            "total_rejected": 10,
            "pipelines_count": 5,
        }
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_summary = AsyncMock(return_value=summary)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_analytics_summary"]({})
        text = _text(result)
        assert "100" in text
        assert "60" in text
        assert "Аналитика контента" in text
        assert "Пайплайнов: 5" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_summary = AsyncMock(side_effect=Exception("analytics down"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_analytics_summary"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetPipelineStats:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({})
        assert "не найдена" in _text(result)

    @pytest.mark.anyio
    async def test_with_stats(self, mock_db):
        stat = SimpleNamespace(
            pipeline_id=1,
            pipeline_name="Pipeline X",
            total_generations=50,
            total_published=40,
            total_rejected=5,
            pending_moderation=5,
            success_rate=0.80,
        )
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(return_value=[stat])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({"pipeline_id": 1})
        text = _text(result)
        assert "Pipeline X" in text
        assert "генераций=50" in text
        assert "80%" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetTrendingTopics:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({"days": 7, "limit": 10})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_topics(self, mock_db):
        topics = [
            SimpleNamespace(keyword="Python", count=500),
            SimpleNamespace(keyword="AI", count=300),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(return_value=topics)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({"days": 14, "limit": 5})
        text = _text(result)
        assert "Python" in text
        assert "500 упоминаний" in text
        assert "AI" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(side_effect=Exception("trend err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetTrendingChannels:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_channels = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_channels"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_channels(self, mock_db):
        channels = [
            SimpleNamespace(title="TechChannel", channel_id=100, message_count=200),
            SimpleNamespace(title="NewsChannel", channel_id=200, message_count=150),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_channels = AsyncMock(return_value=channels)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_channels"]({"days": 7, "limit": 10})
        text = _text(result)
        assert "TechChannel" in text
        assert "id=100" in text
        assert "200 сообщений" in text


class TestAnalyticsToolGetMessageVelocity:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_message_velocity = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_message_velocity"]({"days": 30})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_velocity(self, mock_db):
        velocity = [
            SimpleNamespace(date="2026-01-01", count=1000),
            SimpleNamespace(date="2026-01-02", count=1200),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_message_velocity = AsyncMock(return_value=velocity)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_message_velocity"]({"days": 7})
        text = _text(result)
        assert "2026-01-01" in text
        assert "1000 сообщений" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_message_velocity = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_message_velocity"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetPeakHours:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_peak_hours = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_peak_hours"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_hours(self, mock_db):
        hours = [
            SimpleNamespace(hour=9, count=500),
            SimpleNamespace(hour=18, count=800),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_peak_hours = AsyncMock(return_value=hours)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_peak_hours"]({})
        text = _text(result)
        assert "09:00" in text
        assert "500 сообщений" in text
        assert "18:00" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_peak_hours = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_peak_hours"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetCalendar:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({"limit": 10})
        assert "Нет запланированных" in _text(result)

    @pytest.mark.anyio
    async def test_with_events(self, mock_db):
        event = SimpleNamespace(
            run_id=7,
            pipeline_id=2,
            pipeline_name="Content Pipe",
            moderation_status="approved",
            scheduled_time="2026-05-01T09:00:00",
            created_at="2026-04-30",
            preview="Preview of upcoming post",
        )
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(return_value=[event])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({})
        text = _text(result)
        assert "run_id=7" in text
        assert "Content Pipe" in text
        assert "Preview of upcoming post" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(side_effect=Exception("cal err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetDailyStats:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_daily_stats"]({"days": 7})
        assert "Нет данных" in _text(result)

    @pytest.mark.anyio
    async def test_with_data(self, mock_db):
        from src.services.content_analytics_service import DailyStats

        rows = [
            DailyStats(date="2026-01-01", generations=10, publications=8, rejections=1),
            DailyStats(date="2026-01-02", generations=15, publications=12, rejections=0),
        ]
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(return_value=rows)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_daily_stats"]({"days": 7, "pipeline_id": 1})
        text = _text(result)
        assert "2026-01-01" in text
        assert "генераций=10" in text
        assert "опубл.=8" in text

    @pytest.mark.anyio
    async def test_default_days(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            await handlers["get_daily_stats"]({})
            # Should call with days=30 by default
            mock_svc.return_value.get_daily_stats.assert_awaited_once_with(days=30, pipeline_id=None)

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(side_effect=Exception("stats err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_daily_stats"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetTopMessages:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.get_top_messages = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_top_messages"]({})
        assert "не найдены" in _text(result) or "Нет" in _text(result)

    @pytest.mark.anyio
    async def test_with_messages(self, mock_db):
        msgs = [
            {
                "channel_id": 100, "channel_title": "TestChannel",
                "message_id": 42, "text": "Hello world", "total_reactions": 50,
            },
        ]
        mock_db.get_top_messages = AsyncMock(return_value=msgs)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_top_messages"]({"limit": 20})
        text = _text(result)
        assert "TestChannel" in text
        assert "50" in text

    @pytest.mark.anyio
    async def test_with_date_range(self, mock_db):
        mock_db.get_top_messages = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        await handlers["get_top_messages"]({
            "date_from": "2026-01-01", "date_to": "2026-01-31", "limit": 50,
        })
        mock_db.get_top_messages.assert_awaited_once()

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        mock_db.get_top_messages = AsyncMock(side_effect=Exception("db err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_top_messages"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetContentTypeStats:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.get_engagement_by_media_type = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_content_type_stats"]({
            "date_from": "2026-01-01", "date_to": "2026-01-31",
        })
        assert "не найдены" in _text(result) or "Нет данных" in _text(result)

    @pytest.mark.anyio
    async def test_with_data(self, mock_db):
        rows = [
            {"content_type": "text", "message_count": 100, "avg_reactions": 5.0},
            {"content_type": "photo", "message_count": 50, "avg_reactions": 10.0},
        ]
        mock_db.get_engagement_by_media_type = AsyncMock(return_value=rows)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_content_type_stats"]({
            "date_from": "2026-01-01", "date_to": "2026-01-31",
        })
        text = _text(result)
        assert "text" in text
        assert "100" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        mock_db.get_engagement_by_media_type = AsyncMock(side_effect=Exception("err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_content_type_stats"]({
            "date_from": "2026-01-01", "date_to": "2026-01-31",
        })
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetHourlyActivity:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.get_hourly_activity = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_hourly_activity"]({})
        assert "не найдены" in _text(result) or "Нет" in _text(result)

    @pytest.mark.anyio
    async def test_with_data(self, mock_db):
        hours = [
            {"hour": 9, "message_count": 500, "avg_reactions": 5.0},
            {"hour": 18, "message_count": 800, "avg_reactions": 10.0},
        ]
        mock_db.get_hourly_activity = AsyncMock(return_value=hours)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_hourly_activity"]({"days": 7})
        text = _text(result)
        assert "09:00" in text
        assert "500" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        mock_db.get_hourly_activity = AsyncMock(side_effect=Exception("err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_hourly_activity"]({})
        assert "Ошибка" in _text(result)
