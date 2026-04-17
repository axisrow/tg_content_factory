"""Tests for start_container bootstrap behavior."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database import Database
from src.search.engine import SearchEngine
from src.web.bootstrap import start_container


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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
