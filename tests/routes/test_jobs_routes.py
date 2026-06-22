"""Tests for the unified jobs read API + fragment (#964)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.anyio


async def _seed_jobs(db):
    # One collection task (PENDING) + one telegram command (succeeded).
    await db.repos.tasks.create_collection_task(700900, "Jobs Chan")
    from src.models import TelegramCommand, TelegramCommandStatus

    await db.repos.telegram_commands.create_command(
        TelegramCommand(command_type="get_profile", status=TelegramCommandStatus.SUCCEEDED)
    )


async def test_jobs_api_list_returns_all_sources(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)
    resp = await route_client.get("/jobs/api/list")
    assert resp.status_code == 200
    data = resp.json()
    sources = {j["source"] for j in data}
    assert "collection_task" in sources
    assert "telegram_command" in sources


async def test_jobs_api_filters_by_source(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)
    resp = await route_client.get("/jobs/api/list?source=telegram_command")
    assert resp.status_code == 200
    data = resp.json()
    assert data and all(j["source"] == "telegram_command" for j in data)


async def test_jobs_api_filters_by_status(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)
    resp = await route_client.get("/jobs/api/list?status=pending")
    assert resp.status_code == 200
    assert all(j["runtime_state"] == "pending" for j in resp.json())


async def test_jobs_api_ignores_unknown_filter_tokens(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)
    # Unknown source token → treated as no filter, returns 200 (not 422/500).
    resp = await route_client.get("/jobs/api/list?source=bogus")
    assert resp.status_code == 200


async def test_jobs_fragment_renders(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)
    resp = await route_client.get("/jobs/fragments/list")
    assert resp.status_code == 200
    assert "Jobs Chan" in resp.text
    assert 'hx-target="#jobs-table"' in resp.text
