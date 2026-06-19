from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database.repositories.accounts import AccountSessionDecryptError
from src.runtime.worker import _publish_snapshots, _publish_worker_down_snapshot, _run_worker_async
from src.services.notification_target_service import NotificationTargetStatus
from src.web.embedded_worker import EmbeddedWorker


def _make_container(**overrides):
    container = MagicMock()

    pool = MagicMock()
    pool.clients = overrides.get("clients", {"+1234": MagicMock()})
    pool._dialogs_cache = overrides.get("dialogs_cache", {})
    pool._active_leases = overrides.get("active_leases", {})
    pool._premium_flood_wait_until = overrides.get("premium_flood_waits", {})
    pool._session_overrides = overrides.get("session_overrides", {})
    container.pool = pool

    collector = MagicMock()
    collector.is_running = overrides.get("collector_running", False)
    container.collector = collector

    scheduler = MagicMock()
    scheduler.is_running = overrides.get("scheduler_running", False)
    scheduler.interval_minutes = overrides.get("scheduler_interval", 60)
    scheduler.get_potential_jobs = AsyncMock(return_value=[])
    container.scheduler = scheduler

    container.db = MagicMock()
    container.db.get_accounts = AsyncMock(return_value=overrides.get("accounts", []))
    container.db.repos.runtime_snapshots = MagicMock()
    container.db.repos.runtime_snapshots.upsert_snapshot = AsyncMock()

    target_service = MagicMock()
    target_status = NotificationTargetStatus(
        mode="bot",
        state=overrides.get("target_state", "available"),
        message="ok",
    )
    target_service.describe_target = AsyncMock(return_value=target_status)
    container.notification_target_service = target_service

    config = MagicMock()
    config.notifications.bot_name_prefix = "bot_"
    config.notifications.bot_username_prefix = "bot_"
    container.config = config

    return container


async def test_publish_snapshots_writes_heartbeat():
    container = _make_container()
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif = MagicMock()
        mock_notif.get_status = AsyncMock(return_value=None)
        mock_notif_svc.return_value = mock_notif
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    types = [c[0][0].snapshot_type for c in calls]
    assert "worker_heartbeat" in types
    assert "accounts_status" in types
    assert "pool_counters" in types
    assert "collector_status" in types
    assert "scheduler_status" in types
    assert "scheduler_jobs" in types
    assert "notification_target_status" in types


async def test_publish_snapshots_accounts_status():
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    container = _make_container(
        clients={"+111": MagicMock(), "+222": MagicMock()},
        accounts=[
            SimpleNamespace(phone="+111", flood_wait_until=None),
            SimpleNamespace(phone="+222", flood_wait_until=future),
        ],
    )
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    accounts_call = [c for c in calls if c[0][0].snapshot_type == "accounts_status"][0]
    payload = accounts_call[0][0].payload
    assert payload["connected_count"] == 2
    assert payload["connected_phones"] == ["+111", "+222"]
    assert payload["available_phones"] == ["+111"]
    assert payload["flood_waits"] == {"+222": future.isoformat()}
    assert "timestamp" in payload


async def test_publish_snapshots_collector_status():
    container = _make_container(collector_running=True, clients={"+1": MagicMock()})
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    collector_call = [c for c in calls if c[0][0].snapshot_type == "collector_status"][0]
    payload = collector_call[0][0].payload
    assert payload["is_running"] is True
    assert payload["state"] == "healthy"


async def test_publish_snapshots_collector_no_accounts():
    container = _make_container(clients={}, collector_running=False)
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    collector_call = [c for c in calls if c[0][0].snapshot_type == "collector_status"][0]
    payload = collector_call[0][0].payload
    assert payload["state"] == "no_connected_active"


async def test_publish_snapshots_scheduler_status():
    container = _make_container(scheduler_running=True, scheduler_interval=30)
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    sched_call = [c for c in calls if c[0][0].snapshot_type == "scheduler_status"][0]
    payload = sched_call[0][0].payload
    assert payload["is_running"] is True
    assert payload["interval_minutes"] == 30


async def test_publish_snapshots_scheduler_jobs():
    container = _make_container()
    container.scheduler.get_potential_jobs = AsyncMock(return_value=[{"name": "collect_all"}])
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    jobs_call = [c for c in calls if c[0][0].snapshot_type == "scheduler_jobs"][0]
    assert jobs_call[0][0].payload["jobs"] == [{"name": "collect_all"}]


async def test_publish_snapshots_notification_bot():
    container = _make_container()
    mock_bot = MagicMock()
    mock_bot.bot_username = "test_bot"
    mock_bot.bot_id = 123
    mock_bot.created_at = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif = MagicMock()
        mock_notif.get_status = AsyncMock(return_value=mock_bot)
        mock_notif_svc.return_value = mock_notif
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    notif_call = [c for c in calls if c[0][0].snapshot_type == "notification_target_status"][0]
    bot_payload = notif_call[0][0].payload["bot"]
    assert bot_payload["configured"] is True
    assert bot_payload["bot_username"] == "test_bot"


async def test_publish_snapshots_notification_target_unavailable():
    container = _make_container(target_state="not_configured")
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    notif_call = [c for c in calls if c[0][0].snapshot_type == "notification_target_status"][0]
    bot_payload = notif_call[0][0].payload["bot"]
    assert bot_payload["configured"] is False


async def test_publish_snapshots_notification_bot_exception():
    container = _make_container(target_state="available")
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif = MagicMock()
        mock_notif.get_status = AsyncMock(side_effect=Exception("bot error"))
        mock_notif_svc.return_value = mock_notif
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    notif_call = [c for c in calls if c[0][0].snapshot_type == "notification_target_status"][0]
    bot_payload = notif_call[0][0].payload["bot"]
    assert bot_payload["configured"] is False


async def test_publish_snapshots_notification_bot_cancelled_is_nonfatal(caplog):
    container = _make_container(target_state="available")
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif = MagicMock()
        mock_notif.get_status = AsyncMock(side_effect=asyncio.CancelledError)
        mock_notif_svc.return_value = mock_notif

        caplog.set_level("WARNING", logger="src.runtime.worker")
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    notif_call = [c for c in calls if c[0][0].snapshot_type == "notification_target_status"][0]
    bot_payload = notif_call[0][0].payload["bot"]
    assert bot_payload["configured"] is False
    assert "Notification bot snapshot refresh was cancelled; continuing worker" in caplog.text


async def test_publish_worker_down_snapshot_for_decrypt_failure():
    container = _make_container()
    exc = AccountSessionDecryptError(phone="+1234", status="key_mismatch")

    await _publish_worker_down_snapshot(container, exc)

    snapshot = container.db.repos.runtime_snapshots.upsert_snapshot.call_args[0][0]
    assert snapshot.snapshot_type == "worker_heartbeat"
    assert snapshot.payload["status"] == "worker_down"
    assert snapshot.payload["reason"] == "telegram_session_decrypt_failed"
    assert snapshot.payload["decrypt_status"] == "key_mismatch"
    assert snapshot.payload["action"] == "restore_key_or_relogin"


async def test_worker_loop_continues_after_transient_snapshot_cancel():
    from src.config import AppConfig

    container = MagicMock()
    # The publish timeout is read from config now, so a real AppConfig is needed
    # (a MagicMock would feed asyncio.wait_for a non-numeric timeout).
    config = AppConfig()
    publish_calls = 0
    worker_task: asyncio.Task[None] | None = None

    async def publish_snapshot(_container, *, stop_event=None):
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == 1:
            raise asyncio.CancelledError
        assert worker_task is not None
        worker_task.cancel()
        raise asyncio.CancelledError

    with (
        patch("src.runtime.worker.build_worker_container", AsyncMock(return_value=container)),
        patch("src.runtime.worker.start_container", AsyncMock()),
        patch("src.runtime.worker.stop_container", AsyncMock()) as stop_container,
        patch("src.runtime.worker._publish_snapshots", new=publish_snapshot),
        patch("src.runtime.worker.HEARTBEAT_INTERVAL_SEC", 0.001),
    ):
        worker_task = asyncio.create_task(_run_worker_async(config))
        with pytest.raises(asyncio.CancelledError):
            await worker_task

    assert publish_calls == 2
    stop_container.assert_awaited_once_with(container)


async def test_embedded_worker_stop_suppresses_cancelled_snapshot_publish():
    from src.config import AppConfig

    container = MagicMock()
    worker = EmbeddedWorker(AppConfig())

    with (
        patch("src.web.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("src.web.embedded_worker.start_container", AsyncMock()),
        patch("src.web.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch(
            "src.web.embedded_worker._publish_snapshots",
            AsyncMock(side_effect=asyncio.CancelledError),
        ) as publish_snapshots,
    ):
        await worker.start()
        for _ in range(10):
            if publish_snapshots.await_count:
                break
            await asyncio.sleep(0)

        stop_container.assert_not_awaited()
        await worker.stop(timeout=1.0)

    stop_container.assert_awaited_once_with(container)
    assert worker.container is None


async def test_embedded_worker_retries_after_hanging_snapshot_publish():
    from src.config import AppConfig

    container = MagicMock()
    # The publish timeout now comes from config, not a hard-coded module
    # constant — a single source of truth instead of the old duplicate.
    config = AppConfig()
    config.scheduler.snapshot_publish_timeout_sec = 0.01
    worker = EmbeddedWorker(config)
    publish_started = asyncio.Event()
    publish_calls = 0

    async def hanging_publish(_container, *, stop_event=None):
        nonlocal publish_calls
        publish_calls += 1
        publish_started.set()
        await asyncio.Event().wait()

    with (
        patch("src.web.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("src.web.embedded_worker.start_container", AsyncMock()),
        patch("src.web.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("src.web.embedded_worker._publish_snapshots", new=hanging_publish),
        patch("src.web.embedded_worker.HEARTBEAT_INTERVAL_SEC", 0.001),
    ):
        await worker.start()
        try:
            await asyncio.wait_for(publish_started.wait(), timeout=0.05)
            await asyncio.sleep(0.05)
            assert publish_calls >= 2
        finally:
            await worker.stop(timeout=0.01)

    stop_container.assert_awaited_once_with(container)


async def test_embedded_worker_happy_path_becomes_ready_then_stops_cleanly():
    from src.config import AppConfig

    container = MagicMock()
    published = asyncio.Event()

    async def publish_once(_container, *, stop_event=None):
        published.set()

    worker = EmbeddedWorker(AppConfig())
    with (
        patch("src.web.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("src.web.embedded_worker.start_container", AsyncMock()) as start_container,
        patch("src.web.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("src.web.embedded_worker._publish_snapshots", new=publish_once),
        patch("src.web.embedded_worker.HEARTBEAT_INTERVAL_SEC", 0.001),
    ):
        await worker.start()
        assert await worker.wait_ready(timeout=1.0) is True
        assert worker.agent_ready is True
        assert worker.startup_failed is False
        assert worker.container is container
        await worker.stop(timeout=1.0)

    start_container.assert_awaited_once_with(container)
    stop_container.assert_awaited_once_with(container)
    assert worker.container is None
    assert worker.agent_ready is False
    assert published.is_set()


async def test_embedded_worker_startup_decrypt_error_publishes_worker_down():
    from src.config import AppConfig

    container = MagicMock()
    exc = AccountSessionDecryptError(phone="+1234", status="key_mismatch")
    worker = EmbeddedWorker(AppConfig())
    with (
        patch("src.web.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("src.web.embedded_worker.start_container", AsyncMock(side_effect=exc)),
        patch("src.web.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch(
            "src.web.embedded_worker._publish_worker_down_snapshot", AsyncMock()
        ) as publish_down,
        patch("src.web.embedded_worker._publish_snapshots", AsyncMock()) as publish_snapshots,
    ):
        await worker.start()
        await worker.stop(timeout=1.0)

    assert worker.startup_failed is True
    assert worker.startup_error == str(exc)
    publish_down.assert_awaited_once_with(container, exc)
    publish_snapshots.assert_not_awaited()  # never enters the heartbeat loop
    stop_container.assert_awaited_once_with(container)
    assert worker.container is None


async def test_embedded_worker_startup_generic_error_sets_banner():
    from src.config import AppConfig

    container = MagicMock()
    worker = EmbeddedWorker(AppConfig())
    with (
        patch("src.web.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("src.web.embedded_worker.start_container", AsyncMock(side_effect=RuntimeError("boom"))),
        patch("src.web.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("src.web.embedded_worker._publish_snapshots", AsyncMock()) as publish_snapshots,
    ):
        await worker.start()
        await worker.stop(timeout=1.0)

    assert worker.startup_failed is True
    assert "Embedded worker failed to start" in (worker.startup_error or "")
    publish_snapshots.assert_not_awaited()
    stop_container.assert_awaited_once_with(container)
    assert worker.container is None


async def test_embedded_worker_start_twice_raises_and_stop_without_start_is_noop():
    from src.config import AppConfig

    worker = EmbeddedWorker(AppConfig())
    # stop() before start() is a no-op
    await worker.stop(timeout=1.0)

    with (
        patch("src.web.embedded_worker.build_worker_container", AsyncMock(return_value=MagicMock())),
        patch("src.web.embedded_worker.start_container", AsyncMock()),
        patch("src.web.embedded_worker.stop_container", AsyncMock()),
        patch("src.web.embedded_worker._publish_snapshots", AsyncMock()),
        patch("src.web.embedded_worker.HEARTBEAT_INTERVAL_SEC", 0.001),
    ):
        await worker.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                await worker.start()
        finally:
            await worker.stop(timeout=1.0)
