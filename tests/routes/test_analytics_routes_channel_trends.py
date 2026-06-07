"""Tests for analytics routes — channel analytics and trends endpoints."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.services.trend_service import TrendingChannel, TrendingEmoji, TrendingTopic

# ── Trends page ─────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_trends_page_renders(route_client):
    """Test trends page renders."""
    resp = await route_client.get("/analytics/trends")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_trends_page_with_days(route_client):
    """Test trends page with custom days param."""
    resp = await route_client.get("/analytics/trends?days=14")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_trends_page_invalid_days(route_client):
    """Test trends page with invalid days falls back to 7."""
    resp = await route_client.get("/analytics/trends?days=99")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_api_trending_topics(route_client):
    """GET /analytics/trends/topics returns topic JSON."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_trending_topics = AsyncMock(return_value=[TrendingTopic(keyword="ai", count=3)])
        resp = await route_client.get("/analytics/trends/topics?days=7&limit=5")

    assert resp.status_code == 200
    assert resp.json() == [{"keyword": "ai", "count": 3}]
    instance.get_trending_topics.assert_awaited_once_with(days=7, limit=5)


@pytest.mark.anyio
async def test_api_trending_topics_clamps_days_and_limit(route_client):
    """GET /analytics/trends/topics clamps expensive query bounds."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_trending_topics = AsyncMock(return_value=[])
        resp = await route_client.get("/analytics/trends/topics?days=999999&limit=999999")

    assert resp.status_code == 200
    instance.get_trending_topics.assert_awaited_once_with(days=365, limit=100)


@pytest.mark.anyio
async def test_api_trending_channels(route_client):
    """GET /analytics/trends/channels returns channel JSON."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_trending_channels = AsyncMock(
            return_value=[
                TrendingChannel(
                    channel_id=100,
                    title="Test",
                    username="test",
                    avg_views=10.5,
                    message_count=4,
                )
            ]
        )
        resp = await route_client.get("/analytics/trends/channels?days=7&limit=5")

    assert resp.status_code == 200
    assert resp.json()[0]["channel_id"] == 100
    instance.get_trending_channels.assert_awaited_once_with(days=7, limit=5)


@pytest.mark.anyio
async def test_api_trending_channels_clamps_floor(route_client):
    """GET /analytics/trends/channels clamps lower bounds to one."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_trending_channels = AsyncMock(return_value=[])
        resp = await route_client.get("/analytics/trends/channels?days=-5&limit=0")

    assert resp.status_code == 200
    instance.get_trending_channels.assert_awaited_once_with(days=1, limit=1)


@pytest.mark.anyio
async def test_api_trending_emojis(route_client):
    """GET /analytics/trends/emojis returns reaction emoji JSON."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_trending_emojis = AsyncMock(return_value=[TrendingEmoji(emoji="🔥", count=9)])
        resp = await route_client.get("/analytics/trends/emojis?days=14&limit=3")

    assert resp.status_code == 200
    assert resp.json() == [{"emoji": "🔥", "count": 9}]
    instance.get_trending_emojis.assert_awaited_once_with(days=14, limit=3)


@pytest.mark.anyio
async def test_api_trending_emojis_clamps_days_and_limit(route_client):
    """GET /analytics/trends/emojis clamps expensive query bounds."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_trending_emojis = AsyncMock(return_value=[])
        resp = await route_client.get("/analytics/trends/emojis?days=999999&limit=999999")

    assert resp.status_code == 200
    instance.get_trending_emojis.assert_awaited_once_with(days=365, limit=100)


# ── Channel analytics page ─────────────────────────────────────────


@pytest.mark.anyio
async def test_channel_analytics_page_renders(route_client):
    """Test channel analytics page renders."""
    resp = await route_client.get("/analytics/channels")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_channel_analytics_page_with_channel(route_client, base_app):
    """Test channel analytics page with channel_id param."""
    resp = await route_client.get("/analytics/channels?channel_id=100&days=30")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_channel_analytics_page_invalid_days(route_client):
    """Test channel analytics page with invalid days falls back to 30."""
    resp = await route_client.get("/analytics/channels?days=99")
    assert resp.status_code == 200


# ── Channel API endpoints ──────────────────────────────────────────


@pytest.mark.anyio
async def test_api_channel_overview(route_client):
    """Test channel overview API returns JSON."""
    from src.services.channel_analytics_service import ChannelOverview

    overview = ChannelOverview(
        channel_id=100,
        title="Test",
        username="test",
        subscriber_count=500,
        err=2.5,
        err24=3.1,
    )
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_channel_overview = AsyncMock(return_value=overview)
        resp = await route_client.get("/analytics/channels/api/overview?channel_id=100&days=14")
        assert resp.status_code == 200
        data = resp.json()
        assert data["channel_id"] == 100
        assert data["title"] == "Test"
        instance.get_channel_overview.assert_awaited_once_with(100, days=14)


@pytest.mark.anyio
async def test_api_subscriber_history(route_client):
    """Test subscriber history API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_subscriber_history = AsyncMock(
            return_value=[{"date": "2025-01-01", "count": 100}]
        )
        resp = await route_client.get("/analytics/channels/api/subscribers?channel_id=100&days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_views_timeseries(route_client):
    """Test views timeseries API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_views_timeseries = AsyncMock(
            return_value=[{"date": "2025-01-01", "avg_views": 50.0, "msg_count": 10}]
        )
        resp = await route_client.get("/analytics/channels/api/views?channel_id=100&days=7")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_post_frequency(route_client):
    """Test post frequency API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_post_frequency = AsyncMock(
            return_value=[{"date": "2025-01-01", "count": 5}]
        )
        resp = await route_client.get("/analytics/channels/api/frequency?channel_id=100&days=14")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_err(route_client):
    """Test ERR API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_err = AsyncMock(return_value=2.5)
        instance.get_err24 = AsyncMock(return_value=3.1)
        resp = await route_client.get("/analytics/channels/api/err?channel_id=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["err"] == 2.5
        assert data["err24"] == 3.1


@pytest.mark.anyio
async def test_api_hourly_activity(route_client):
    """Test hourly activity API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_hourly_activity = AsyncMock(
            return_value=[{"hour": 12, "count": 42}]
        )
        resp = await route_client.get("/analytics/channels/api/hourly?channel_id=100&days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_citation_stats(route_client):
    """Test citation stats API returns JSON."""
    from src.services.channel_analytics_service import CitationStats

    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_citation_stats = AsyncMock(
            return_value=CitationStats(total_forwards=100, post_count=50, avg_forwards=2.0)
        )
        resp = await route_client.get("/analytics/channels/api/citation?channel_id=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_forwards"] == 100


@pytest.mark.anyio
async def test_api_heatmap(route_client):
    """Test heatmap API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_heatmap = AsyncMock(
            return_value=[{"hour": 12, "weekday": 1, "count": 10}]
        )
        resp = await route_client.get("/analytics/channels/api/heatmap?channel_id=100&days=7")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_cross_citations(route_client):
    """Test cross-citations API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_cross_channel_citations = AsyncMock(
            return_value=[{"source_channel_id": 200, "count": 5}]
        )
        resp = await route_client.get(
            "/analytics/channels/api/cross-citations?channel_id=100&days=30&limit=10"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
