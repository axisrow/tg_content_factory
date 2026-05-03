"""Tests for start_container bootstrap behavior."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon import TelegramClient

from src.config import AppConfig, DatabaseConfig
from src.database import Database
from src.database.repositories.accounts import AccountSessionDecryptError
from src.search.engine import SearchEngine
from src.web.bootstrap import build_web_container, start_container
from src.web.log_handler import LogBuffer
from src.web.runtime_shims import SnapshotClientPool


def _make_container(db: Database) -> MagicMock:
    """Build a minimal AppContainer mock backed by a real Database."""
    container = MagicMock()
    container.runtime_mode = "worker"
    container.db = db
    container.auth.is_configured = False
    container.pool = MagicMock()
    container.collection_queue = None
    container.unified_dispatcher = None
    container.ai_search = MagicMock()
    container.ai_search.initialize = MagicMock()
    container.agent_manager = None
    container.channel_bundle.fail_running_collection_tasks_on_startup = AsyncMock(return_value=0)
    container.photo_task_service.recover_running = AsyncMock(return_value=0)
    container.db.repos.generation_runs.reset_running_on_startup = AsyncMock(return_value=0)
    return container


@pytest.mark.anyio
async def test_start_container_autostarts_scheduler_when_flag_set(tmp_path):
    """start_container calls scheduler.start() when scheduler_autostart=1 is in DB."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    await db.set_setting("scheduler_autostart", "1")

    container = _make_container(db)
    scheduler_mock = AsyncMock()
    scheduler_mock.load_settings = AsyncMock()
    container.scheduler = scheduler_mock

    try:
        await start_container(container)
        scheduler_mock.start.assert_called_once()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_no_autostart_when_flag_absent(tmp_path):
    """start_container does not call scheduler.start() when flag is not set."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    # Do not set scheduler_autostart

    container = _make_container(db)
    scheduler_mock = AsyncMock()
    scheduler_mock.load_settings = AsyncMock()
    container.scheduler = scheduler_mock

    try:
        await start_container(container)
        scheduler_mock.start.assert_not_called()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_no_autostart_when_flag_zero(tmp_path):
    """start_container does not call scheduler.start() when scheduler_autostart=0."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    await db.set_setting("scheduler_autostart", "0")

    container = _make_container(db)
    scheduler_mock = AsyncMock()
    scheduler_mock.load_settings = AsyncMock()
    container.scheduler = scheduler_mock

    try:
        await start_container(container)
        scheduler_mock.start.assert_not_called()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_calls_load_settings(tmp_path):
    """start_container always calls scheduler.load_settings() on startup."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    container = _make_container(db)
    scheduler_mock = AsyncMock()
    scheduler_mock.load_settings = AsyncMock()
    container.scheduler = scheduler_mock

    try:
        await start_container(container)
        scheduler_mock.load_settings.assert_called_once()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_does_not_fail_running_channel_tasks(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    container = _make_container(db)
    scheduler_mock = AsyncMock()
    scheduler_mock.load_settings = AsyncMock()
    container.scheduler = scheduler_mock

    try:
        await start_container(container)
        container.channel_bundle.fail_running_collection_tasks_on_startup.assert_not_called()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_web_mode_skips_telegram_runtime(tmp_path):
    """Web runtime should not initialize Telegram pool or worker dispatchers."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    container = _make_container(db)
    container.runtime_mode = "web"
    container.auth.is_configured = True
    container.pool.initialize = AsyncMock()
    container.pool.warm_all_dialogs = AsyncMock()
    container.scheduler = AsyncMock()
    container.scheduler.load_settings = AsyncMock()
    container.unified_dispatcher = AsyncMock()
    container.unified_dispatcher.start = AsyncMock()

    try:
        await start_container(container)
        container.pool.initialize.assert_not_called()
        container.unified_dispatcher.start.assert_not_called()
        container.scheduler.load_settings.assert_not_called()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_worker_mode_initializes_runtime(tmp_path):
    """Worker runtime should initialize Telegram pool and dispatcher startup paths."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    container = _make_container(db)
    container.runtime_mode = "worker"
    container.auth.is_configured = True
    container.pool.initialize = AsyncMock()
    container.pool.warm_all_dialogs = AsyncMock()
    container.scheduler = AsyncMock()
    container.scheduler.load_settings = AsyncMock()
    container.unified_dispatcher = AsyncMock()
    container.unified_dispatcher.start = AsyncMock()

    try:
        await start_container(container)
        container.pool.initialize.assert_awaited_once()
        container.unified_dispatcher.start.assert_awaited_once()
        container.scheduler.load_settings.assert_awaited_once()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_start_container_empty_pool_skips_collection_requeue_and_schedules_retry(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    container = _make_container(db)
    container.runtime_mode = "worker"
    container.auth.is_configured = True
    container.pool.initialize = AsyncMock()
    container.pool.clients = {}
    container.pool.warm_all_dialogs = AsyncMock()
    container.collection_queue = MagicMock()
    container.collection_queue.requeue_startup_tasks = AsyncMock(return_value=0)
    container.collection_queue.start_db_pull = MagicMock()
    container.bg_tasks = set()
    container.scheduler = AsyncMock()
    container.scheduler.load_settings = AsyncMock()
    container.unified_dispatcher = AsyncMock()
    container.unified_dispatcher.start = AsyncMock()

    try:
        await start_container(container)
        container.collection_queue.requeue_startup_tasks.assert_not_called()
        container.collection_queue.start_db_pull.assert_called_once()
        assert any(task.get_name() == "telegram_pool_reconnect_retry" for task in container.bg_tasks)
    finally:
        for task in list(container.bg_tasks):
            task.cancel()
        await asyncio.gather(*container.bg_tasks, return_exceptions=True)
        await db.close()


@pytest.mark.anyio
async def test_start_container_continues_when_pool_init_decrypt_fails(tmp_path, caplog):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    container = _make_container(db)
    container.runtime_mode = "worker"
    container.auth.is_configured = True
    container.pool.initialize = AsyncMock(
        side_effect=AccountSessionDecryptError(phone="+7000", status="key_mismatch")
    )
    container.pool.warm_all_dialogs = AsyncMock()
    container.scheduler = AsyncMock()
    container.scheduler.load_settings = AsyncMock()
    container.unified_dispatcher = AsyncMock()
    container.unified_dispatcher.start = AsyncMock()
    container.agent_manager = MagicMock()
    container.agent_manager.refresh_settings_cache = AsyncMock()

    try:
        with caplog.at_level("WARNING"):
            await start_container(container)
        assert "telegram pool degraded by session decrypt failure" in caplog.text
        container.agent_manager.refresh_settings_cache.assert_awaited_once_with(preflight=True)
        container.agent_manager.initialize.assert_called_once()
        container.pool.warm_all_dialogs.assert_not_called()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_search_engine_accepts_none_pool(tmp_path):
    """SearchEngine built with pool=None (web-mode) must not raise on check_search_quota."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        engine = SearchEngine(db, pool=None)
        assert engine._telegram._pool is None
        quota = await engine.check_search_quota("test")
        assert quota is None
    finally:
        await db.close()


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
