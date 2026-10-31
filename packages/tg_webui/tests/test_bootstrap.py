"""Tests for start_container bootstrap behavior."""

# Web-panel tests split out of tests/test_bootstrap.py (#1720 wave 3).

from __future__ import annotations

import pytest
from telethon import TelegramClient

from tg_agent.config import AppConfig, DatabaseConfig
from tg_agent.runtime.log_handler import LogBuffer
from tg_agent.runtime.snapshots import SnapshotClientPool
from tg_webui.bootstrap import build_web_container


@pytest.mark.anyio
async def test_build_web_container_does_not_connect_telethon(tmp_path, monkeypatch):
    """Regression for #444 Test Plan: web startup without internet must not
    initiate a single Telegram connect/reconnect.

    We verify two things:
    1. The web container uses SnapshotClientPool, not live ClientPool.
    2. No TelegramClient.connect() call was made while building the container.
    """
    connect_calls: list[object] = []

    real_connect = TelegramClient.connect

    async def _tracking_connect(self, *args, **kwargs):
        connect_calls.append(self)
        return await real_connect(self, *args, **kwargs)

    monkeypatch.setattr(TelegramClient, "connect", _tracking_connect)

    config = AppConfig(database=DatabaseConfig(path=str(tmp_path / "test.db")))
    log_buffer = LogBuffer()

    container = await build_web_container(config, log_buffer=log_buffer)
    try:
        assert container.runtime_mode == "web"
        assert isinstance(container.pool, SnapshotClientPool)
        assert connect_calls == [], (
            f"web container must not connect to Telegram during build; "
            f"got {len(connect_calls)} connect call(s)"
        )
    finally:
        await container.db.close()


@pytest.mark.anyio
async def test_build_web_container_enables_semantic_search(tmp_path):
    """Web SearchBundle must be built via from_database so the numpy probe runs and
    semantic/hybrid search is available — the positional ctor left it False (#837/2)."""
    config = AppConfig(database=DatabaseConfig(path=str(tmp_path / "test.db")))
    container = await build_web_container(config, log_buffer=LogBuffer())
    try:
        assert container.search_engine.semantic_available is True
    finally:
        await container.db.close()
