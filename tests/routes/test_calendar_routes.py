"""Tests for calendar routes."""
from __future__ import annotations

import pytest


@pytest.fixture
async def client(route_client):
    return route_client


@pytest.mark.asyncio
async def test_calendar_page_renders(client):
    """Calendar page renders successfully."""
    resp = await client.get("/calendar/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_calendar_page_with_days_param(client):
    """Calendar page accepts days parameter."""
    resp = await client.get("/calendar/?days=14")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_calendar_page_with_pipeline_filter(client):
    """Calendar page accepts pipeline_id filter."""
    resp = await client.get("/calendar/?pipeline_id=1")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_api_calendar_empty(client):
    """Calendar JSON API returns empty list when no data."""
    resp = await client.get("/calendar/api/calendar")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_calendar_with_days(client):
    """Calendar JSON API accepts days parameter."""
    resp = await client.get("/calendar/api/calendar?days=7")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_upcoming_empty(client):
    """Upcoming JSON API returns empty list."""
    resp = await client.get("/calendar/api/upcoming")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_upcoming_with_limit(client):
    """Upcoming JSON API accepts limit parameter."""
    resp = await client.get("/calendar/api/upcoming?limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_stats(client):
    """Calendar stats JSON API returns stats dict."""
    resp = await client.get("/calendar/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
