from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.runtime.worker import _publish_snapshots
from src.services.notification_target_service import NotificationTargetStatus


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
    container = _make_container(clients={"+111": MagicMock(), "+222": MagicMock()})
    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    calls = container.db.repos.runtime_snapshots.upsert_snapshot.call_args_list
    accounts_call = [c for c in calls if c[0][0].snapshot_type == "accounts_status"][0]
    payload = accounts_call[0][0].payload
    assert payload["connected_count"] == 2
    assert payload["connected_phones"] == ["+111", "+222"]


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
