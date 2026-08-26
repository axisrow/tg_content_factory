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


async def test_jobs_api_list_supports_optional_pagination(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)

    resp = await route_client.get("/jobs/api/list?page=1&limit=1")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["jobs"]) == 1
    assert data["total_count"] == 2
    assert data["page"] == 1
    assert data["limit"] == 1


async def test_jobs_api_list_page_beyond_total_is_empty(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)

    resp = await route_client.get("/jobs/api/list?page=99&limit=1")

    assert resp.status_code == 200
    assert resp.json()["jobs"] == []
    assert resp.json()["total_count"] == 2


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


async def test_jobs_fragment_accepts_page(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)

    resp = await route_client.get("/jobs/fragments/list?page=2&limit=1")

    assert resp.status_code == 200
    assert resp.text.count("<tr>") == 2  # header plus one paginated job


async def test_jobs_page_renders_lazyload_shell(route_client):
    # The dashboard page (#965) must paint instantly without querying the DB and
    # defer the table to the fragment via hx-trigger="load" (the #756 pattern).
    resp = await route_client.get("/jobs")
    assert resp.status_code == 200
    body = resp.text
    assert 'id="jobs-table"' in body
    assert 'hx-get="/jobs/fragments/list"' in body
    assert 'hx-trigger="load"' in body
    assert 'hx-swap="innerHTML"' in body


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


async def test_jobs_fragment_shows_status_tabs_counts_and_pagination(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)

    resp = await route_client.get("/jobs/fragments/list?page=1&limit=1")

    assert resp.status_code == 200
    assert "Все (2)" in resp.text
    assert "Активные (1)" in resp.text
    assert "Завершённые (1)" in resp.text
    assert "Страница 1 из 2" in resp.text
    assert 'class="d-lg-none mobile-cards p-2"' in resp.text


async def test_jobs_fragment_collection_actions_are_source_scoped(route_client):
    db = route_client._transport_app.state.db
    await _seed_jobs(db)

    collection = await route_client.get(
        "/jobs/fragments/list?source=collection_task&page=1&limit=100"
    )
    telegram = await route_client.get(
        "/jobs/fragments/list?source=telegram_command&page=1&limit=100"
    )

    assert 'action="/jobs/tasks/clear-pending-collect' in collection.text
    assert "/cancel" in collection.text
    assert 'action="/jobs/tasks/clear-pending-collect' not in telegram.text
    assert "/cancel" not in telegram.text


async def test_jobs_cancel_route_delegates_and_preserves_filters(route_client):
    from src.models import CollectionTaskStatus

    db = route_client._transport_app.state.db
    task_id = await db.repos.tasks.create_collection_task(700910, "Cancel Me")

    resp = await route_client.post(
        f"/jobs/tasks/{task_id}/cancel?source=collection_task&status=active&page=2&limit=25",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert resp.headers["location"].startswith("/jobs?")
    assert "source=collection_task" in resp.headers["location"]
    assert "status=active" in resp.headers["location"]
    assert "page=2" in resp.headers["location"]
    assert "limit=25" in resp.headers["location"]
    assert "msg=task_cancelled" in resp.headers["location"]
    assert (await db.repos.tasks.get_collection_task(task_id)).status == CollectionTaskStatus.CANCELLED


async def test_jobs_clear_pending_route_deletes_only_pending_collect(route_client):
    from src.models import CollectionTaskStatus, StatsAllTaskPayload

    db = route_client._transport_app.state.db
    pending_id = await db.repos.tasks.create_collection_task(700920, "Pending")
    running_id = await db.repos.tasks.create_collection_task(700921, "Running")
    await db.repos.tasks.update_collection_task(running_id, CollectionTaskStatus.RUNNING)
    stats_id = await db.repos.tasks.create_stats_task(StatsAllTaskPayload(channel_ids=[700920]))

    resp = await route_client.post(
        "/jobs/tasks/clear-pending-collect?status=active&page=1&limit=100",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=pending_collect_tasks_deleted" in resp.headers["location"]
    assert await db.repos.tasks.get_collection_task(pending_id) is None
    assert (await db.repos.tasks.get_collection_task(running_id)).status == CollectionTaskStatus.RUNNING
    assert await db.repos.tasks.get_collection_task(stats_id) is not None


async def test_jobs_fragment_renders_collection_result_and_type_label(route_client):
    from src.models import CollectionTaskStatus

    db = route_client._transport_app.state.db
    task_id = await db.repos.tasks.create_collection_task(
        700930,
        "Progress Channel",
        payload={"messages_total": 5000},
    )
    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.RUNNING,
        messages_collected=1000,
    )

    resp = await route_client.get(
        "/jobs/fragments/list?source=collection_task&page=1&limit=100"
    )

    assert resp.status_code == 200
    assert "Сбор канала" in resp.text
    assert "1000/5000" in resp.text
    assert "Выполняется" in resp.text


async def test_jobs_fragment_renders_pipeline_result_metadata(route_client):
    from src.models import CollectionTaskStatus, ContentPipeline

    db = route_client._transport_app.state.db
    pipeline_id = await db.repos.content_pipelines.add(
        pipeline=ContentPipeline(
            name="Reaction Pipeline",
            prompt_template=".",
            publish_mode="moderated",
        ),
        source_channel_ids=[],
        targets=[],
    )
    run_id = await db.repos.generation_runs.create_run(pipeline_id, ".")
    await db.repos.generation_runs.save_result(
        run_id,
        "",
        {
            "result_kind": "processed_messages",
            "result_count": 3,
            "action_counts": {"react": 3},
        },
    )
    task_id = await db.repos.tasks.create_generic_task(
        task_type="pipeline_run",
        title="Reaction Pipeline",
        payload={
            "task_kind": "pipeline_run",
            "pipeline_id": pipeline_id,
            "dry_run": False,
            "since_hours": 24.0,
        },
    )
    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=3,
        note=f"Pipeline run id={run_id}",
    )

    resp = await route_client.get(
        "/jobs/fragments/list?source=collection_task&status=completed&page=1&limit=100"
    )

    assert resp.status_code == 200
    assert "Обработано" in resp.text
    assert "Обработано: 3" in resp.text


async def test_jobs_fragment_renders_stats_progress_counter(route_client):
    from src.models import CollectionTaskStatus, StatsAllTaskPayload

    db = route_client._transport_app.state.db
    task_id = await db.repos.tasks.create_stats_task(
        StatsAllTaskPayload(channel_ids=[101, 102, 103])
    )
    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.RUNNING,
        messages_collected=2,
    )

    resp = await route_client.get(
        "/jobs/fragments/list?source=collection_task&status=active&page=1&limit=100"
    )

    assert resp.status_code == 200
    assert "Статистика" in resp.text
    assert "2/3" in resp.text


async def test_jobs_page_preserves_filters_in_lazy_fragment_url(route_client):
    resp = await route_client.get("/jobs?source=collection_task&status=active&page=2&limit=25")

    assert resp.status_code == 200
    assert (
        'hx-get="/jobs/fragments/list?source=collection_task&amp;status=active&amp;page=2&amp;limit=25"'
        in resp.text
    )


async def test_jobs_autoreload_is_capped(route_client):
    """Active jobs refresh only the fragment and stop after a bounded count."""
    db = route_client._transport_app.state.db
    await db.repos.tasks.create_collection_task(700930, "Active")

    resp = await route_client.get(
        "/jobs/fragments/list?source=collection_task&status=active&page=1&limit=100"
    )

    assert resp.status_code == 200
    assert "window.location.reload" not in resp.text
    assert 'hx-target="#jobs-table"' in resp.text
    assert "jobsAutoReload" in resp.text
    assert "jobs-autoreload-count" in resp.text
    assert "count >= maxReloads" in resp.text
    assert 'id="jobs-autoreload-paused"' in resp.text
