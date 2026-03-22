"""Tests for start_container bootstrap behavior."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database import Database
from src.web.bootstrap import start_container


def _make_container(db: Database) -> MagicMock:
    """Build a minimal AppContainer mock backed by a real Database."""
    container = MagicMock()
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
