"""Tests for analytics routes — channel analytics and trends endpoints."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


# ── Trends page ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trends_page_renders(client):
    """Test trends page renders."""
    resp = await client.get("/analytics/trends")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_trends_page_with_days(client):
    """Test trends page with custom days param."""
    resp = await client.get("/analytics/trends?days=14")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_trends_page_invalid_days(client):
    """Test trends page with invalid days falls back to 7."""
    resp = await client.get("/analytics/trends?days=99")
    assert resp.status_code == 200


# ── Channel analytics page ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_channel_analytics_page_renders(client):
    """Test channel analytics page renders."""
    resp = await client.get("/analytics/channels")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_channel_analytics_page_with_channel(client, base_app):
    """Test channel analytics page with channel_id param."""
    resp = await client.get("/analytics/channels?channel_id=100&days=30")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_channel_analytics_page_invalid_days(client):
    """Test channel analytics page with invalid days falls back to 30."""
    resp = await client.get("/analytics/channels?days=99")
    assert resp.status_code == 200


# ── Channel API endpoints ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_api_channel_overview(client):
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
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_channel_overview = AsyncMock(return_value=overview)
        resp = await client.get("/analytics/channels/api/overview?channel_id=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["channel_id"] == 100
        assert data["title"] == "Test"


@pytest.mark.asyncio
async def test_api_subscriber_history(client):
    """Test subscriber history API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_subscriber_history = AsyncMock(
            return_value=[{"date": "2025-01-01", "count": 100}]
        )
        resp = await client.get("/analytics/channels/api/subscribers?channel_id=100&days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_views_timeseries(client):
    """Test views timeseries API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_views_timeseries = AsyncMock(
            return_value=[{"date": "2025-01-01", "avg_views": 50.0, "msg_count": 10}]
        )
        resp = await client.get("/analytics/channels/api/views?channel_id=100&days=7")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_post_frequency(client):
    """Test post frequency API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_post_frequency = AsyncMock(
            return_value=[{"date": "2025-01-01", "count": 5}]
        )
        resp = await client.get("/analytics/channels/api/frequency?channel_id=100&days=14")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_err(client):
    """Test ERR API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_err = AsyncMock(return_value=2.5)
        instance.get_err24 = AsyncMock(return_value=3.1)
        resp = await client.get("/analytics/channels/api/err?channel_id=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["err"] == 2.5
        assert data["err24"] == 3.1


@pytest.mark.asyncio
async def test_api_hourly_activity(client):
    """Test hourly activity API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_hourly_activity = AsyncMock(
            return_value=[{"hour": 12, "count": 42}]
        )
        resp = await client.get("/analytics/channels/api/hourly?channel_id=100&days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_citation_stats(client):
    """Test citation stats API returns JSON."""
    from src.services.channel_analytics_service import CitationStats

    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_citation_stats = AsyncMock(
            return_value=CitationStats(total_forwards=100, post_count=50, avg_forwards=2.0)
        )
        resp = await client.get("/analytics/channels/api/citation?channel_id=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_forwards"] == 100


@pytest.mark.asyncio
async def test_api_heatmap(client):
    """Test heatmap API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_heatmap = AsyncMock(
            return_value=[{"hour": 12, "weekday": 1, "count": 10}]
        )
        resp = await client.get("/analytics/channels/api/heatmap?channel_id=100&days=7")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_cross_citations(client):
    """Test cross-citations API returns JSON."""
    with patch("src.web.routes.analytics.ChannelAnalyticsService") as MockSvc:
        instance = MockSvc.return_value
        instance.get_cross_channel_citations = AsyncMock(
            return_value=[{"source_channel_id": 200, "count": 5}]
        )
        resp = await client.get(
            "/analytics/channels/api/cross-citations?channel_id=100&days=30&limit=10"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
