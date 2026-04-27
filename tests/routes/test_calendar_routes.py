"""Tests for calendar routes."""
from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_calendar_page_renders(route_client):
    """Calendar page renders successfully and returns the calendar template."""
    resp = await route_client.get("/calendar/")
    assert resp.status_code == 200
    assert "Календарь" in resp.text


@pytest.mark.asyncio
async def test_calendar_page_with_days_param(route_client):
    """Calendar page echoes the requested days window back into the template."""
    resp = await route_client.get("/calendar/?days=14")
    assert resp.status_code == 200
    assert "Календарь" in resp.text
    # Active-window selector marks the requested range as selected.
    assert 'value="14"' in resp.text


@pytest.mark.asyncio
async def test_calendar_page_with_pipeline_filter(route_client):
    """Calendar page accepts pipeline_id filter and renders the form with that value."""
    resp = await route_client.get("/calendar/?pipeline_id=1")
    assert resp.status_code == 200
    assert "Календарь" in resp.text


@pytest.mark.asyncio
async def test_api_calendar_empty(route_client):
    """Calendar JSON API returns empty list when no data."""
    resp = await route_client.get("/calendar/api/calendar")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_calendar_with_days(route_client):
    """Calendar JSON API accepts days parameter."""
    resp = await route_client.get("/calendar/api/calendar?days=7")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_upcoming_empty(route_client):
    """Upcoming JSON API returns empty list."""
    resp = await route_client.get("/calendar/api/upcoming")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_upcoming_with_limit(route_client):
    """Upcoming JSON API accepts limit parameter."""
    resp = await route_client.get("/calendar/api/upcoming?limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_stats(route_client):
    """Calendar stats JSON API returns stats dict."""
    resp = await route_client.get("/calendar/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
