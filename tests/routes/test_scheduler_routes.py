"""Tests for scheduler routes."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import Channel, CollectionTaskStatus, ContentPipeline, SearchQuery, StatsAllTaskPayload


@pytest.fixture
async def client(base_app):
    """Create test client with scheduler."""
    app, _, pool_mock = base_app

    async def _resolve_channel(identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    pool_mock.clients = {}
    pool_mock.resolve_channel = _resolve_channel

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c



@pytest.mark.anyio
async def test_scheduler_page(client):
    """Test scheduler page renders."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_shows_status(client):
    """Test scheduler page shows scheduler status."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Page should contain scheduler info
    assert "scheduler" in resp.text.lower() or "планировщик" in resp.text.lower()


@pytest.mark.anyio
async def test_scheduler_page_with_message(client):
    """Test scheduler page with message query param."""
    resp = await client.get("/scheduler/?msg=test_message")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_with_error(client):
    """Test scheduler page with error query param."""
    resp = await client.get("/scheduler/?error=shutting_down")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_start_scheduler_redirect(client):
    """Test start scheduler redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.start = AsyncMock()
        resp = await client.post("/scheduler/start", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_start_scheduler_shutting_down(client):
    """Test start scheduler when shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/start", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location


@pytest.mark.anyio
async def test_stop_scheduler_redirect(client):
    """Test stop scheduler redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.stop = AsyncMock()
        resp = await client.post("/scheduler/stop", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_trigger_collection_redirect(client):
    """Test trigger collection redirects."""
    with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
        mock_svc.return_value.trigger_collection = AsyncMock()
        resp = await client.post("/scheduler/trigger", follow_redirects=False)
        assert resp.status_code == 303
        assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_trigger_collection_shutting_down(client):
    """Test trigger collection when shutting down."""
    client._transport.app.state.shutting_down = True
    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "error=shutting_down" in location


@pytest.mark.anyio
async def test_trigger_collection_enqueues(client):
    """Test trigger collection enqueues channels."""
    resp = await client.post("/scheduler/trigger", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "/scheduler" in location


@pytest.mark.anyio
async def test_cancel_task_redirect(client):
    """Test cancel task redirects."""
    # Create a task first
    db = client._transport.app.state.db
    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test Channel",
    )

    resp = await client.post(f"/scheduler/tasks/{task_id}/cancel", follow_redirects=False)
    assert resp.status_code == 303
    assert "/scheduler" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_cancel_task_nonexistent(client):
    """Test cancel nonexistent task."""
    resp = await client.post("/scheduler/tasks/999999/cancel", follow_redirects=False)
    # Should redirect even if task doesn't exist
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_scheduler_page_shows_tasks(client):
    """Test scheduler page shows collection tasks."""
    db = client._transport.app.state.db

    # Create a task
    await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test Channel",
    )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_shows_processed_message_label_for_pipeline_runs(client):
    db = client._transport.app.state.db

    pipeline_id = await db.repos.content_pipelines.add(
        pipeline=ContentPipeline(name="Reaction Pipeline", prompt_template=".", publish_mode="moderated"),
        source_channel_ids=[],
        targets=[],
    )
    run_id = await db.repos.generation_runs.create_run(pipeline_id, ".")
    await db.repos.generation_runs.save_result(
        run_id,
        "",
        {"result_kind": "processed_messages", "result_count": 3, "action_counts": {"react": 3}},
    )
    await db.repos.tasks.create_generic_task(
        task_type="pipeline_run",
        title="Reaction Pipeline",
        payload={"task_kind": "pipeline_run", "pipeline_id": pipeline_id, "dry_run": False, "since_hours": 24.0},
    )
    tasks = await db.get_collection_tasks(limit=5)
    task_id = next(t.id for t in tasks if t.note is None and t.task_type.value == "pipeline_run")
    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=3,
        note=f"Pipeline run id={run_id}",
    )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert "Обработано" in resp.text


@pytest.mark.anyio
async def test_scheduler_page_shows_search_log(client):
    """Test scheduler page shows search log."""
    db = client._transport.app.state.db

    # Log a search
    await db.log_search("+1234567890", "test query", 10)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_shows_collector_status(client):
    """Test scheduler page shows collector running status."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Should show collector status somewhere
    text_lower = resp.text.lower()
    assert "running" in text_lower or "остановлен" in text_lower or "запущен" in text_lower


@pytest.mark.anyio
async def test_scheduler_shows_collector_health_card_when_all_accounts_flooded(client):
    db = client._transport.app.state.db
    accounts = await db.get_accounts(active_only=False)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert "Здоровье коллектора" in resp.text
    assert "Flood Wait" in resp.text
    assert "Что делать" in resp.text


@pytest.mark.anyio
async def test_scheduler_overload_running_is_warning_not_flood_blocker(client):
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    collector = client._transport.app.state.collector
    scheduler = client._transport.app.state.scheduler
    pool.clients = {"+1234567890": MagicMock()}
    collector._running = True
    scheduler.update_interval(15)
    for i in range(101, 317):
        await db.add_channel(Channel(channel_id=i, title=f"Channel {i}"))
    task_id = await db.create_collection_task(channel_id=100, channel_title="Running Channel")
    await db.update_collection_task(task_id, CollectionTaskStatus.RUNNING, messages_collected=12)
    old_note = "Flood Wait: account +1234567890 unavailable"
    for i in range(2):
        old_task_id = await db.create_collection_task(channel_id=200 + i, channel_title=f"Old {i}")
        await db.update_collection_task(old_task_id, CollectionTaskStatus.COMPLETED, note=old_note)

    resp = await client.get("/scheduler/")

    assert resp.status_code == 200
    assert "Риск перегрузки" in resp.text
    assert "border-warning" in resp.text
    assert "border-danger" not in resp.text
    assert "Сейчас собирается:" in resp.text
    assert "Running Channel" in resp.text
    assert "собрано 12 сообщений" in resp.text
    assert "Недавние события недоступности" in resp.text
    assert "×2" in resp.text
    active_flood_reason = (
        "Почему сейчас не собираем:</strong> доступных аккаунтов нет или их недостаточно. Flood Wait активен"
    )
    assert active_flood_reason not in resp.text


@pytest.mark.anyio
async def test_scheduler_all_flooded_keeps_danger_current_reason(client):
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    pool.clients = {"+1234567890": MagicMock()}
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    for acc in await db.get_accounts(active_only=False):
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/")

    assert resp.status_code == 200
    assert "border-danger" in resp.text
    assert "Все аккаунты во Flood Wait" in resp.text
    active_flood_reason = (
        "Почему сейчас не собираем:</strong> доступных аккаунтов нет или их недостаточно. Flood Wait активен"
    )
    assert active_flood_reason in resp.text


@pytest.mark.anyio
async def test_scheduler_shows_interval(client):
    """Test scheduler page shows collection interval."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_empty_tasks(client):
    """Test scheduler page with no tasks."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_empty_search_log(client):
    """Test scheduler page with no search log."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_has_active_tasks_flag(client):
    """Test scheduler page has active tasks detection."""
    db = client._transport.app.state.db

    # Create pending task
    await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_run_display(client):
    """Test scheduler displays last run time."""
    # Set last_run on scheduler
    from datetime import datetime

    client._transport.app.state.scheduler._last_run = datetime.now()

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_stats_display(client):
    """Test scheduler displays last stats."""
    client._transport.app.state.scheduler._last_stats = {"collected": 100}

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_search_run_display(client):
    """Test scheduler displays last search run time."""
    from datetime import datetime

    client._transport.app.state.scheduler._last_search_run = datetime.now()

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_last_search_stats_display(client):
    """Test scheduler displays last search stats."""
    client._transport.app.state.scheduler._last_search_stats = {"queries": 5}

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_interval_minutes(client):
    """Test scheduler displays interval minutes."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_search_interval_minutes(client):
    """Test scheduler displays search interval minutes."""
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_start_scheduler_calls_service(client):
    """Test start scheduler enqueues reconcile command."""
    db = client._transport.app.state.db
    await client.post("/scheduler/start")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "scheduler.reconcile"


@pytest.mark.anyio
async def test_stop_scheduler_calls_service(client):
    """Test stop scheduler enqueues reconcile command."""
    db = client._transport.app.state.db
    await client.post("/scheduler/stop")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "scheduler.reconcile"


@pytest.mark.anyio
async def test_start_scheduler_sets_autostart_flag(client):
    """POST /scheduler/start persists scheduler_autostart=1 to DB."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/start", follow_redirects=False)
    assert resp.status_code == 303
    value = await db.get_setting("scheduler_autostart")
    assert value == "1"


@pytest.mark.anyio
async def test_stop_scheduler_clears_autostart_flag(client):
    """POST /scheduler/stop persists scheduler_autostart=0 to DB."""
    db = client._transport.app.state.db
    await db.set_setting("scheduler_autostart", "1")
    resp = await client.post("/scheduler/stop", follow_redirects=False)
    assert resp.status_code == 303
    value = await db.get_setting("scheduler_autostart")
    assert value == "0"


@pytest.mark.anyio
async def test_trigger_collection_calls_service(client):
    """Test trigger collection calls collection service."""
    mock_service = MagicMock()
    mock_result = MagicMock()
    mock_result.queued_count = 0
    mock_result.skipped_existing_count = 0
    mock_result.total_candidates = 0
    mock_service.enqueue_all_channels = AsyncMock(return_value=mock_result)

    with patch(
        "src.web.routes.scheduler.deps.collection_service",
        return_value=mock_service,
    ):
        await client.post("/scheduler/trigger")
        mock_service.enqueue_all_channels.assert_called_once()


@pytest.mark.anyio
async def test_cancel_task_calls_queue(client):
    """Test cancel task calls queue cancel."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )

    resp = await client.post(f"/scheduler/tasks/{task_id}/cancel")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_clear_pending_collect_tasks_redirects_and_deletes_only_pending_channel_tasks(client):
    db = client._transport.app.state.db

    pending_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Pending Channel",
    )
    running_id = await db.create_collection_task(
        channel_id=-1001234567891,
        channel_title="Running Channel",
    )
    await db.update_collection_task(running_id, CollectionTaskStatus.RUNNING)
    await db.create_stats_task(StatsAllTaskPayload(channel_ids=[-1001234567890]))

    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=pending_collect_tasks_deleted" in resp.headers["location"]
    assert await db.get_collection_task(pending_id) is None
    assert (await db.get_collection_task(running_id)).status == CollectionTaskStatus.RUNNING


@pytest.mark.anyio
async def test_clear_pending_collect_tasks_empty_queue_redirects(client):
    resp = await client.post(
        "/scheduler/tasks/clear-pending-collect",
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=pending_collect_tasks_empty" in resp.headers["location"]


@pytest.mark.anyio
async def test_scheduler_page_shows_clear_pending_collect_button(client):
    db = client._transport.app.state.db
    await db.create_collection_task(
        channel_id=-1001234567890, channel_title="Pending Channel",
    )

    resp = await client.get("/scheduler/")

    assert resp.status_code == 200
    assert 'action="/scheduler/tasks/clear-pending-collect"' in resp.text
    assert "Очистить очередь загрузки" in resp.text


@pytest.mark.anyio
async def test_scheduler_page_hides_clear_button_when_no_pending(client):
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert 'action="/scheduler/tasks/clear-pending-collect"' not in resp.text


@pytest.mark.anyio
async def test_scheduler_page_with_completed_task(client):
    """Test scheduler page shows completed task status."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_with_failed_task(client):
    """Test scheduler page shows failed task status."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.FAILED, error="Test error")

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_page_with_cancelled_task(client):
    """Test scheduler page shows cancelled task status."""
    db = client._transport.app.state.db

    task_id = await db.create_collection_task(
        channel_id=-1001234567890,
        channel_title="Test",
    )
    await db.update_collection_task(task_id, CollectionTaskStatus.CANCELLED)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_trigger_then_get_scheduler_with_pending_tasks(client):
    """Trigger collect-all then follow redirect to scheduler page with pending tasks.

    Reproduces the 500 'No response returned' scenario where the scheduler
    page fails to render when pending tasks exist after triggering collection.
    """
    db = client._transport.app.state.db

    # Seed several channels so enqueue_all_channels creates tasks
    for i in range(3):
        await db.add_channel(Channel(
            channel_id=-(1001000000 + i),
            title=f"Channel {i}",
            username=f"ch{i}",
            channel_type="channel",
        ))

    # Trigger collection (follow redirect to GET /scheduler/?msg=...)
    resp = await client.post("/scheduler/trigger")
    assert resp.status_code == 200
    assert "Планировщик" in resp.text


@pytest.mark.anyio
async def test_scheduler_page_with_many_pending_tasks(client):
    """Scheduler page renders correctly with many pending tasks."""
    db = client._transport.app.state.db

    for i in range(10):
        await db.create_collection_task(
            channel_id=-(1001000000 + i),
            channel_title=f"Channel {i}",
            channel_username=f"ch{i}",
        )

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    assert "Channel 0" in resp.text


# ── Dry-run notification tests ──────────────────────────────────────


@pytest.mark.anyio
async def test_dry_run_no_queries(client):
    """Dry-run with no notification queries shows empty state."""
    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "Нет активных запросов" in resp.text


@pytest.mark.anyio
async def test_dry_run_excludes_inactive_queries(client):
    """Dry-run excludes queries with is_active=False."""
    db = client._transport.app.state.db
    await db.repos.search_queries.add(SearchQuery(
        query="active_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))
    await db.repos.search_queries.add(SearchQuery(
        query="inactive_query", notify_on_collect=True, is_active=False, is_fts=False,
    ))
    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "inactive_query" not in resp.text


@pytest.mark.anyio
async def test_dry_run_excludes_disabled_scheduler_job(client):
    """Dry-run excludes queries whose scheduler job is disabled."""
    db = client._transport.app.state.db
    # Create a completed collection task so dry-run has a time window
    task_id = await db.create_collection_task(channel_id=-1001234567890, channel_title="Test")
    await db.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    await db.repos.search_queries.add(SearchQuery(
        query="enabled_job_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))
    disabled_id = await db.repos.search_queries.add(SearchQuery(
        query="disabled_job_query", notify_on_collect=True, is_active=True, is_fts=False,
    ))
    # Disable the scheduler job for the second query
    await db.repos.settings.set_setting(f"scheduler_job_disabled:sq_{disabled_id}", "1")

    resp = await client.post("/scheduler/dry-run-notifications")
    assert resp.status_code == 200
    assert "enabled_job_query" in resp.text
    assert "disabled_job_query" not in resp.text


# ── Issue #463: semantic scheduler cell assertions ───────────────────────────


async def _seed_pipeline_run_for_scheduler(
    db,
    *,
    pipeline_id: int = 1,
    generated_text: str = "",
    metadata: dict | None = None,
    messages_collected: int = 0,
) -> int:
    """Seed a completed pipeline_run task + generation_run; return task_id."""
    from src.models import CollectionTaskStatus, CollectionTaskType, PipelineRunTaskPayload

    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt")
    await db.repos.generation_runs.save_result(run_id, generated_text, metadata or {})

    task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.PIPELINE_RUN,
        payload=PipelineRunTaskPayload(pipeline_id=pipeline_id),
    )
    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=messages_collected,
        note=f"Pipeline run id={run_id}",
    )
    return task_id


def _find_scheduler_row_by_task_id(soup, task_id):
    """Find a scheduler table row for the given task_id.

    For running/pending tasks, matches the cancel-form action.
    For completed tasks (no form), falls back to searching the /pipelines/ links
    or other task-id-bearing attributes. If none, returns None.
    """
    form = soup.find(
        "form",
        attrs={
            "action": lambda v: v is not None and f"/scheduler/tasks/{task_id}/cancel" in v
        },
    )
    if form:
        return form.find_parent("tr")
    # Completed tasks have no cancel form — fall back to rows where the task
    # metadata is embedded elsewhere. The current template does not stamp a
    # data-task-id, so we can't locate by id alone for COMPLETED rows. Callers
    # should assert on page-level text in that case.
    return None


def _scheduler_desktop_rows(soup):
    """Return all <tr> rows inside the Tasks desktop table."""
    # The tasks table is preceded by card-header "Задачи"; select tbody rows.
    tables = soup.select("table.tga-table-striped")
    rows: list = []
    for table in tables:
        tbody = table.find("tbody")
        if tbody is None:
            continue
        rows.extend(tbody.find_all("tr"))
    return rows


def _pipeline_row(soup):
    """Return the <tr> whose first <td> badge text equals 'pipeline_run' label.

    The task-type badge is rendered via `task_type_label(t)` in the template;
    for PIPELINE_RUN that returns a Russian label. We look for rows whose first
    cell contains any of the expected labels.
    """
    for row in _scheduler_desktop_rows(soup):
        tds = row.find_all("td")
        if not tds:
            continue
        first_text = tds[0].get_text(strip=True, separator=" ")
        if "pipeline" in first_text.lower() or "пайплайн" in first_text.lower():
            return row
    return None


def _non_pipeline_rows(soup):
    return [
        r
        for r in _scheduler_desktop_rows(soup)
        if (
            r.find_all("td")
            and "pipeline" not in r.find_all("td")[0].get_text(strip=True).lower()
            and "пайплайн" not in r.find_all("td")[0].get_text(strip=True).lower()
        )
    ]


@pytest.mark.anyio
async def test_scheduler_renders_processed_label_for_action_only_run(base_app, route_client):
    """Action-only pipeline run should render «Обработано» label + action count."""
    from bs4 import BeautifulSoup

    _, db, _ = base_app
    await _seed_pipeline_run_for_scheduler(
        db,
        pipeline_id=1,
        generated_text="",
        metadata={
            "citations": [],
            "action_counts": {"react": 5},
            "result_kind": "processed_messages",
            "result_count": 5,
        },
        messages_collected=5,
    )

    resp = await route_client.get("/scheduler/?status=all")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.text, "html.parser")
    row = _pipeline_row(soup)
    assert row is not None, "scheduler must render the pipeline_run task row"
    result_cell_text = row.find_all("td")[3].get_text(strip=True, separator=" ")
    assert "Обработано" in result_cell_text, (
        f"Expected 'Обработано' in result cell, got: {result_cell_text!r}"
    )
    assert "5" in result_cell_text


@pytest.mark.anyio
async def test_scheduler_renders_generation_label_for_generation_run(base_app, route_client):
    from bs4 import BeautifulSoup

    _, db, _ = base_app
    await _seed_pipeline_run_for_scheduler(
        db,
        pipeline_id=1,
        generated_text="draft",
        metadata={
            "citations": [{"id": 1}, {"id": 2}, {"id": 3}],
            "result_kind": "generated_items",
            "result_count": 3,
        },
        messages_collected=3,
    )

    resp = await route_client.get("/scheduler/?status=all")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.text, "html.parser")
    row = _pipeline_row(soup)
    assert row is not None
    result_cell_text = row.find_all("td")[3].get_text(strip=True, separator=" ")
    assert "Сгенерировано" in result_cell_text, (
        f"Expected 'Сгенерировано' in result cell, got: {result_cell_text!r}"
    )
    assert "3" in result_cell_text


@pytest.mark.anyio
async def test_scheduler_shows_warning_badge_when_run_has_node_errors(base_app, route_client):
    """Issue #463: when metadata.node_errors is non-empty, scheduler must render
    a warning badge with count next to result_count, so users see why 0.
    """
    from bs4 import BeautifulSoup

    _, db, _ = base_app
    await _seed_pipeline_run_for_scheduler(
        db,
        pipeline_id=1,
        generated_text="",
        metadata={
            "citations": [],
            "result_kind": "processed_messages",
            "result_count": 0,
            "node_errors": [
                {"node_id": "react", "code": "flood_wait", "detail": "..."}
            ],
        },
        messages_collected=0,
    )

    resp = await route_client.get("/scheduler/?status=all")
    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    row = _pipeline_row(soup)
    assert row is not None
    cell_html = str(row.find_all("td")[3])
    assert "⚠" in cell_html or "pipe-run-warning" in cell_html


@pytest.mark.anyio
async def test_scheduler_mixed_page_non_pipeline_tasks_unaffected(base_app, route_client):
    """When a pipeline_run coexists with a plain non-pipeline task on /scheduler,
    the non-pipeline row must keep its plain messages_collected display
    (no spurious «Обработано»/«Сгенерировано» labels).
    """
    from bs4 import BeautifulSoup

    from src.models import CollectionTaskStatus, CollectionTaskType

    _, db, _ = base_app

    # Pipeline-run task (generation semantics).
    await _seed_pipeline_run_for_scheduler(
        db,
        pipeline_id=1,
        generated_text="draft",
        metadata={
            "citations": [{"id": 1}, {"id": 2}],
            "result_kind": "generated_items",
            "result_count": 2,
        },
        messages_collected=2,
    )

    # Plain non-pipeline task sharing the same scheduler page.
    plain_task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.STATS_ALL,
    )
    await db.repos.tasks.update_collection_task(
        plain_task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=42,
    )

    resp = await route_client.get("/scheduler/?status=all")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.text, "html.parser")
    pipe_row = _pipeline_row(soup)
    plain_rows = _non_pipeline_rows(soup)

    assert pipe_row is not None, "pipeline row missing"
    assert plain_rows, "plain (non-pipeline) row missing"
    pipe_text = pipe_row.find_all("td")[3].get_text(strip=True, separator=" ")
    # pick the plain row whose result cell contains '42' (our seeded value)
    plain_text = None
    for r in plain_rows:
        cell = r.find_all("td")[3].get_text(strip=True, separator=" ")
        if "42" in cell:
            plain_text = cell
            break
    assert plain_text is not None, (
        "Could not find non-pipeline row with messages_collected=42"
    )

    assert "Сгенерировано" in pipe_text
    # Plain (non-pipeline) row must NOT inherit the pipeline-specific labels.
    assert "Сгенерировано" not in plain_text
    assert "Обработано" not in plain_text
    assert "42" in plain_text


@pytest.mark.anyio
async def test_scheduler_shows_flood_wait_countdown(client):
    """Test that scheduler page shows flood wait countdown in hours and minutes."""
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    accounts = await db.get_accounts(active_only=False)

    # Put account in pool so it appears in connected_active_accounts
    pool.clients = {acc.phone: MagicMock() for acc in accounts}

    # Set flood wait for 3 hours in future (round number avoids second-boundary races)
    future = datetime.now(timezone.utc) + timedelta(hours=3)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Check that countdown is displayed — "3 ч 0 мин" or similar
    assert " ч " in resp.text or " мин)" in resp.text


@pytest.mark.anyio
async def test_scheduler_hides_countdown_if_too_short(client):
    """Test that countdown is hidden if less than 60 seconds remain."""
    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    accounts = await db.get_accounts(active_only=False)

    # Put account in pool so it appears in connected_active_accounts
    pool.clients = {acc.phone: MagicMock() for acc in accounts}

    # Set flood wait for only 30 seconds (below the 60s threshold, shouldn't show countdown)
    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # Countdown should not appear for very short waits
    assert "(0 мин)" not in resp.text
