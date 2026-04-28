"""Tests for analytics routes."""

from __future__ import annotations

import pytest


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
async def test_api_content_summary_returns_json(route_client):
    """Test content summary API returns JSON."""
    resp = await route_client.get("/analytics/content/api/summary")
    assert resp.status_code == 200
    import json
    data = json.loads(resp.text)
    assert isinstance(data, dict)


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
