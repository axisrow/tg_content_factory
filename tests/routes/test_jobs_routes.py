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


async def test_jobs_page_renders_lazyload_shell(route_client):
    # The dashboard page (#965) must paint instantly without querying the DB and
    # defer the table to the fragment via hx-trigger="load" (the #756 pattern).
    resp = await route_client.get("/jobs")
    assert resp.status_code == 200
    body = resp.text
    assert 'id="jobs-table"' in body
    assert 'hx-get="/jobs/fragments/list"' in body
    assert 'hx-trigger="load"' in body


async def test_jobs_page_omits_table_data(route_client):
    # The shell must not contain the fragment's table — it's loaded lazily, so a
    # seeded job must NOT appear in the page response itself (only in the fragment).
    db = route_client._transport_app.state.db
    await db.repos.tasks.create_collection_task(700903, "Lazy Only In Fragment")
    resp = await route_client.get("/jobs")
    assert "Lazy Only In Fragment" not in resp.text


async def test_jobs_fragment_shows_pause_gate_state(route_client):
    # A PENDING collection task while the queue is paused must surface as
    # pause_gate in the fragment with the warning badge (#770 LiveRuntimePauseGate).
    from datetime import datetime, timezone

    from src.models import RuntimeSnapshot

    db = route_client._transport_app.state.db
    await db.repos.tasks.create_collection_task(700904, "Held By Pause Gate")
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="collection_queue_status",
            payload={"paused": True, "active_task_ids": []},
            updated_at=datetime.now(timezone.utc),
        )
    )
    resp = await route_client.get("/jobs/fragments/list")
    assert resp.status_code == 200
    body = resp.text
    assert "Held By Pause Gate" in body
    assert "pause_gate" in body
    assert "bg-warning" in body


async def test_jobs_api_sorts_mixed_null_and_naive_timestamps(route_client):
    # Regression: the sort key mixed a tz-aware None-sentinel with the naive
    # ``created_at`` values SQLite stores, so a job with ``created_at IS NULL``
    # next to one with a real timestamp raised ``TypeError: can't compare
    # offset-naive and offset-aware datetimes`` → HTTP 500. One NULL + one naive
    # row must now sort cleanly.
    db = route_client._transport_app.state.db
    await db.repos.tasks.create_collection_task(700901, "Has Timestamp")
    await db.repos.tasks.create_collection_task(700902, "Null Timestamp")
    await db.execute_write("UPDATE collection_tasks SET created_at = NULL WHERE channel_id = ?", (700902,))
    resp = await route_client.get("/jobs/api/list")
    assert resp.status_code == 200
    # channel_title surfaces via the JobView ``summary`` field.
    summaries = {j["summary"] for j in resp.json()}
    assert {"Has Timestamp", "Null Timestamp"} <= summaries
