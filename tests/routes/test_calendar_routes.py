"""Tests for calendar routes."""
from __future__ import annotations

import pytest

from src.models import ContentPipeline


async def _create_calendar_run(route_client) -> int:
    app = route_client._transport_app
    db = app.state.db
    pipeline_id = await db.repos.content_pipelines.add(
        ContentPipeline(name="Calendar Pipeline", prompt_template="prompt"),
        source_channel_ids=[],
        targets=[],
    )
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt")
    await db.repos.generation_runs.save_result(run_id, "Generated calendar text", {})
    return run_id


@pytest.mark.anyio
async def test_calendar_page_renders(route_client):
    """Calendar page renders successfully and returns the calendar template."""
    resp = await route_client.get("/calendar/")
    assert resp.status_code == 200
    assert "Календарь" in resp.text


@pytest.mark.anyio
async def test_calendar_page_with_days_param(route_client):
    """Calendar page echoes the requested days window back into the template."""
    resp = await route_client.get("/calendar/?days=14")
    assert resp.status_code == 200
    assert "Календарь" in resp.text
    # Active-window selector marks the requested range as selected.
    assert 'value="14"' in resp.text


@pytest.mark.anyio
async def test_calendar_page_with_pipeline_filter(route_client):
    """Calendar page accepts pipeline_id filter and renders the form with that value."""
    resp = await route_client.get("/calendar/?pipeline_id=1")
    assert resp.status_code == 200
    assert "Календарь" in resp.text


@pytest.mark.anyio
async def test_calendar_page_accepts_empty_pipeline_filter(route_client):
    """Calendar page treats the empty 'All pipelines' value as no filter."""
    resp = await route_client.get("/calendar/?days=14&pipeline_id=")
    assert resp.status_code == 200
    assert "Календарь" in resp.text


@pytest.mark.anyio
async def test_calendar_open_links_use_moderation_view(route_client):
    """Calendar cards (now in the lazy grid fragment) link to the moderation run view."""
    run_id = await _create_calendar_run(route_client)

    resp = await route_client.get("/calendar/fragments/grid")

    assert resp.status_code == 200
    assert f"/moderation/{run_id}/view" in resp.text
    assert f"/pipelines/runs/{run_id}" not in resp.text


@pytest.mark.anyio
async def test_calendar_page_lazy_loads_fragments(route_client):
    """#756: the calendar page paints a skeleton wired to HTMX fragment endpoints."""
    resp = await route_client.get("/calendar/")
    assert resp.status_code == 200
    assert 'hx-get="/calendar/fragments/stats"' in resp.text
    assert 'hx-get="/calendar/fragments/grid' in resp.text
    assert 'hx-get="/calendar/fragments/upcoming' in resp.text
    assert 'hx-trigger="load"' in resp.text


@pytest.mark.anyio
async def test_calendar_fragments_return_partial_html(route_client):
    """Calendar fragment endpoints return bare partials, not a full page."""
    for path in (
        "/calendar/fragments/stats",
        "/calendar/fragments/grid?days=7",
        "/calendar/fragments/upcoming",
    ):
        resp = await route_client.get(path)
        assert resp.status_code == 200, path
        assert "<html" not in resp.text.lower(), path


@pytest.mark.anyio
async def test_api_calendar_empty(route_client):
    """Calendar JSON API returns empty list when no data."""
    resp = await route_client.get("/calendar/api/calendar")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_calendar_with_days(route_client):
    """Calendar JSON API accepts days parameter."""
    resp = await route_client.get("/calendar/api/calendar?days=7")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_calendar_accepts_empty_pipeline_filter(route_client):
    """Calendar JSON API treats empty pipeline_id as no filter."""
    resp = await route_client.get("/calendar/api/calendar?days=14&pipeline_id=")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_upcoming_empty(route_client):
    """Upcoming JSON API returns empty list."""
    resp = await route_client.get("/calendar/api/upcoming")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_upcoming_with_limit(route_client):
    """Upcoming JSON API accepts limit parameter."""
    resp = await route_client.get("/calendar/api/upcoming?limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_upcoming_accepts_empty_pipeline_filter(route_client):
    """Upcoming JSON API treats empty pipeline_id as no filter."""
    resp = await route_client.get("/calendar/api/upcoming?limit=5&pipeline_id=")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_api_stats(route_client):
    """Calendar stats JSON API returns stats dict."""
    resp = await route_client.get("/calendar/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
