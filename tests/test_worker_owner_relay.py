"""The worker daemon publishes its relay for the project's CLI processes (#1868)."""

from __future__ import annotations

import stat
from types import SimpleNamespace
from unittest.mock import MagicMock

from tg_agent.config import AppConfig
from tg_agent.runtime.worker import _start_owner_relay
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.relay import read_endpoint, relay_endpoint_path


async def test_worker_with_real_connections_publishes_a_private_relay(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "project.db")
    pool = MagicMock(spec=ClientPool)
    pool.clients = {}

    handle = await _start_owner_relay(SimpleNamespace(pool=pool), config)
    path = relay_endpoint_path(config.database.path)
    try:
        assert handle is not None
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert read_endpoint(path).port == handle.endpoint.port
    finally:
        await handle.stop()
    assert not path.exists()


async def test_worker_without_own_connections_publishes_nothing(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "project.db")

    assert await _start_owner_relay(SimpleNamespace(pool=object()), config) is None
    assert not relay_endpoint_path(config.database.path).exists()
