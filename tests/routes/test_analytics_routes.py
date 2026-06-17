"""Tests for analytics routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.services.content_analytics_service import DailyStats


@pytest.mark.anyio
async def test_analytics_page_renders(route_client):
    """Test analytics page renders without errors."""
    resp = await route_client.get("/analytics")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_analytics_page_with_dates(route_client):
    """Test analytics page with date filters."""
    resp = await route_client.get(
        "/analytics?date_from=2024-01-01&date_to=2024-12-31"
    )
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_analytics_page_limit_param(route_client):
    """Test analytics page with limit parameter."""
    resp = await route_client.get("/analytics?limit=20")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_analytics_page_invalid_limit(route_client):
    """Test analytics page with invalid limit returns 422."""
    resp = await route_client.get("/analytics?limit=abc")
    # FastAPI returns 422 for validation error
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_analytics_page_empty_db(route_client):
    """Test analytics page with empty database."""
    resp = await route_client.get("/analytics")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_content_analytics_page_renders(route_client):
    """Test content analytics page renders."""
    resp = await route_client.get("/analytics/content")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_content_analytics_pipeline_links_use_edit_route(route_client):
    """Pipeline statistics links point to an existing pipeline page."""
    db = route_client._transport_app.state.db
    from src.models import ContentPipeline

    pipeline_id = await db.repos.content_pipelines.add(
        ContentPipeline(name="Linked Pipeline", prompt_template="Write"),
        source_channel_ids=[],
        targets=[],
    )

    # Pipeline stats now render in the lazy-loaded fragment (#756).
    resp = await route_client.get("/analytics/content/fragments/pipelines")

    assert resp.status_code == 200
    assert f'href="/pipelines/{pipeline_id}/edit"' in resp.text
    assert f'href="/pipelines/{pipeline_id}"' not in resp.text


@pytest.mark.anyio
async def test_analytics_top_message_link_uses_bare_channel_id(route_client):
    """Regression #633-9: t.me/c link for a no-username channel keeps the bare id.

    The id starts with ``100`` (1005551782); the old template truncated it via
    ``(channel_id | abs) - 1000000000000`` producing a broken/negative link.
    """
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = route_client._transport_app.state.db
    channel_id = 1005551782
    await db.add_channel(
        Channel(channel_id=channel_id, title="No Username Channel", username=None)
    )
    await db.insert_messages_batch(
        [
            Message(
                channel_id=channel_id,
                message_id=789,
                text="top reacted message",
                reactions_json='[{"emoji": "👍", "count": 42}]',
                date=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
            )
        ]
    )

    # Top messages now render in the lazy-loaded fragment (#756).
    resp = await route_client.get("/analytics/fragments/top-messages")

    assert resp.status_code == 200
    assert "https://t.me/c/1005551782/789" in resp.text
    # The broken legacy forms must be gone.
    assert "t.me/c/5551782/" not in resp.text
    assert "-998994448218" not in resp.text


@pytest.mark.anyio
async def test_api_content_summary_returns_json(route_client):
    """Test content summary API returns JSON."""
    resp = await route_client.get("/analytics/content/api/summary")
    assert resp.status_code == 200
    import json
    data = json.loads(resp.text)
    assert isinstance(data, dict)


@pytest.mark.anyio
async def test_api_content_type_stats_returns_json(route_client):
    """GET /analytics/content/api/types returns a JSON list."""
    resp = await route_client.get("/analytics/content/api/types")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_api_pipelines_returns_json(route_client):
    """Test pipeline stats API returns JSON."""
    resp = await route_client.get("/analytics/content/api/pipelines")
    assert resp.status_code == 200
    import json
    data = json.loads(resp.text)
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_pipelines_with_data(route_client):
    """Test pipeline stats API with created pipeline."""
    db = route_client._transport_app.state.db
    from src.models import (
        ContentPipeline,
        PipelineGenerationBackend,
        PipelinePublishMode,
        PipelineTarget,
    )

    pipeline = ContentPipeline(
        name="Test Pipeline",
        prompt_template="Write",
        publish_mode=PipelinePublishMode.MODERATED,
        generation_backend=PipelineGenerationBackend.CHAIN,
    )
    await db.repos.content_pipelines.add(
        pipeline,
        source_channel_ids=[100],
        targets=[
            PipelineTarget(
                pipeline_id=0,
                phone="+1234567890",
                dialog_id=200,
                title="Target",
                dialog_type="channel",
            )
        ],
    )

    resp = await route_client.get("/analytics/content/api/pipelines")
    assert resp.status_code == 200
    import json
    data = json.loads(resp.text)
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["pipeline_name"] == "Test Pipeline"


@pytest.mark.anyio
async def test_api_pipelines_filter_by_id(route_client):
    """Test pipeline stats API filtered by pipeline_id."""
    db = route_client._transport_app.state.db
    from src.models import (
        ContentPipeline,
        PipelineGenerationBackend,
        PipelinePublishMode,
        PipelineTarget,
    )

    pipeline = ContentPipeline(
        name="Filter Test",
        prompt_template="Write",
        publish_mode=PipelinePublishMode.MODERATED,
        generation_backend=PipelineGenerationBackend.CHAIN,
    )
    pipeline_id = await db.repos.content_pipelines.add(
        pipeline,
        source_channel_ids=[100],
        targets=[
            PipelineTarget(
                pipeline_id=0,
                phone="+1234567890",
                dialog_id=200,
                title="Target",
                dialog_type="channel",
            )
        ],
    )

    resp = await route_client.get(f"/analytics/content/api/pipelines?pipeline_id={pipeline_id}")
    assert resp.status_code == 200
    import json
    data = json.loads(resp.text)
    assert len(data) == 1
    assert data[0]["pipeline_id"] == pipeline_id


@pytest.mark.anyio
async def test_api_daily_stats(route_client):
    """GET /analytics/content/api/daily returns daily content stats."""
    with patch("src.web.routes.analytics.ContentAnalyticsService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_daily_stats = AsyncMock(
            return_value=[
                DailyStats(date="2026-06-06", generations=2, publications=1, rejections=0)
            ]
        )
        resp = await route_client.get("/analytics/content/api/daily?days=7&pipeline_id=5")

    assert resp.status_code == 200
    assert resp.json() == [
        {"date": "2026-06-06", "generations": 2, "publications": 1, "rejections": 0}
    ]
    instance.get_daily_stats.assert_awaited_once_with(days=7, pipeline_id=5)


@pytest.mark.anyio
async def test_api_messages_top_returns_json(route_client):
    """GET /analytics/messages/top returns a JSON list (parity: analytics top)."""
    resp = await route_client.get("/analytics/messages/top?limit=5")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_api_hourly_activity_returns_json(route_client):
    """GET /analytics/messages/hourly returns a JSON list."""
    resp = await route_client.get("/analytics/messages/hourly")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_api_messages_top_with_data(route_client):
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = route_client._transport_app.state.db
    await db.add_channel(Channel(channel_id=500, title="Top Chan", username="top"))
    await db.insert_messages_batch([
        Message(
            channel_id=500, message_id=1, text="reacted",
            reactions_json='[{"emoji": "👍", "count": 99}]',
            date=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
    ])
    resp = await route_client.get("/analytics/messages/top?limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert any(row.get("total_reactions") for row in data)


@pytest.mark.anyio
async def test_api_pipeline_stats_alias_returns_json(route_client):
    """GET /analytics/pipelines/stats returns a JSON list (parity: analytics pipeline-stats)."""
    resp = await route_client.get("/analytics/pipelines/stats")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_api_message_velocity_returns_json(route_client):
    """GET /analytics/messages/velocity returns a JSON list (parity: analytics velocity)."""
    resp = await route_client.get("/analytics/messages/velocity?days=30")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_api_message_velocity_clamps_days(route_client):
    """GET /analytics/messages/velocity clamps expensive day windows."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_message_velocity = AsyncMock(return_value=[])
        resp = await route_client.get("/analytics/messages/velocity?days=999999")

    assert resp.status_code == 200
    instance.get_message_velocity.assert_awaited_once_with(days=365)


@pytest.mark.anyio
async def test_api_peak_hours_returns_json(route_client):
    """GET /analytics/peak-hours returns a JSON list (parity: analytics peak-hours)."""
    resp = await route_client.get("/analytics/peak-hours?days=30")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_api_peak_hours_clamps_days(route_client):
    """GET /analytics/peak-hours clamps expensive day windows."""
    with patch("src.web.routes.analytics.TrendService") as mock_svc:
        instance = mock_svc.return_value
        instance.get_peak_hours = AsyncMock(return_value=[])
        resp = await route_client.get("/analytics/peak-hours?days=0")

    assert resp.status_code == 200
    instance.get_peak_hours.assert_awaited_once_with(days=1)


# ── #756: lazyload skeletons for content & trends ────────────────────


@pytest.mark.anyio
async def test_content_page_lazy_loads_fragments(route_client):
    """#756: the content page paints a skeleton wired to HTMX fragment endpoints."""
    resp = await route_client.get("/analytics/content")
    assert resp.status_code == 200
    assert 'hx-get="/analytics/content/fragments/summary"' in resp.text
    assert 'hx-get="/analytics/content/fragments/pipelines"' in resp.text
    assert 'hx-trigger="load"' in resp.text


@pytest.mark.anyio
async def test_trends_page_lazy_loads_fragments(route_client):
    """#756: the trends page paints a skeleton; NLP aggregations load as fragments."""
    resp = await route_client.get("/analytics/trends?days=14")
    assert resp.status_code == 200
    assert 'hx-get="/analytics/trends/fragments/topics?days=14"' in resp.text
    assert 'hx-get="/analytics/trends/fragments/channels?days=14"' in resp.text
    assert 'hx-get="/analytics/trends/fragments/emojis?days=14"' in resp.text
    assert 'hx-trigger="load"' in resp.text


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path",
    [
        "/analytics/content/fragments/summary",
        "/analytics/content/fragments/pipelines",
        "/analytics/trends/fragments/topics?days=7",
        "/analytics/trends/fragments/channels?days=7",
        "/analytics/trends/fragments/emojis?days=7",
    ],
)
async def test_analytics_lazy_fragments_return_partial_html(route_client, path):
    """Content/trends fragment endpoints return bare partials, not a full page."""
    resp = await route_client.get(path)
    assert resp.status_code == 200
    assert "<html" not in resp.text.lower()
