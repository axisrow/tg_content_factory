"""Worker assembly and lifecycle integration through the runtime entrypoint."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tg_agent.config import AppConfig, DatabaseConfig
from tg_agent.database import Database
from tg_agent.runtime.bootstrap import build_worker_container, start_container, stop_container
from tg_agent.runtime.container import RuntimeContainer
from tg_agent.runtime.log_handler import LogBuffer
from tg_agent.runtime.worker import _publish_worker_heartbeat_snapshot


@pytest.mark.anyio
async def test_worker_lifecycle_persists_heartbeat_without_templates(tmp_path):
    config = AppConfig(database=DatabaseConfig(path=str(tmp_path / "worker.db")))
    container = await build_worker_container(config, log_buffer=LogBuffer())
    assert isinstance(container, RuntimeContainer)
    assert not hasattr(container, "templates")
    try:
        await start_container(container)
        await _publish_worker_heartbeat_snapshot(container, datetime.now(timezone.utc))
        heartbeat = await container.db.repos.runtime_snapshots.get_snapshot("worker_heartbeat")
        assert heartbeat is not None
        assert heartbeat.payload["status"] == "alive"
    finally:
        await stop_container(container)
        await stop_container(container)

    reopened = Database(config.database.path)
    await reopened.initialize()
    try:
        heartbeat = await reopened.repos.runtime_snapshots.get_snapshot("worker_heartbeat")
        assert heartbeat is not None
        assert heartbeat.payload["status"] == "alive"
    finally:
        await reopened.close()
