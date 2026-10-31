# Web-panel tests split out of tests/test_runtime_worker.py (#1720 wave 3).
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tg_agent.database.repositories.accounts import AccountSessionDecryptError
from tg_webui.embedded_worker import EmbeddedWorker


async def test_embedded_worker_stop_suppresses_cancelled_snapshot_publish():
    from tg_agent.config import AppConfig

    container = MagicMock()
    worker = EmbeddedWorker(AppConfig())
    publish_started = asyncio.Event()

    async def cancelled_publish(_container, *, stop_event=None):
        # Signal that the publish step actually ran before it raises, so the test
        # can await a deterministic event instead of polling with a fixed yield
        # budget that flakes under coverage/xdist (#883).
        publish_started.set()
        raise asyncio.CancelledError

    with (
        patch("tg_webui.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("tg_webui.embedded_worker.start_container", AsyncMock()),
        patch("tg_webui.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("tg_webui.embedded_worker._publish_snapshots", new=cancelled_publish),
    ):
        await worker.start()
        await asyncio.wait_for(publish_started.wait(), timeout=2.0)

        stop_container.assert_not_awaited()
        await worker.stop(timeout=1.0)

    stop_container.assert_awaited_once_with(container)
    assert worker.container is None


async def test_embedded_worker_retries_after_hanging_snapshot_publish():
    from tg_agent.config import AppConfig

    container = MagicMock()
    # The publish timeout now comes from config, not a hard-coded module
    # constant — a single source of truth instead of the old duplicate.
    config = AppConfig()
    config.scheduler.snapshot_publish_timeout_sec = 0.01
    worker = EmbeddedWorker(config)
    publish_calls = 0
    second_publish = asyncio.Event()

    async def hanging_publish(_container, *, stop_event=None):
        nonlocal publish_calls
        publish_calls += 1
        # The first publish hangs and is killed by the per-publish timeout; the
        # loop must retry. Fire on the second invocation so the test can await
        # the retry deterministically instead of sleeping a fixed window and
        # hoping two cycles elapsed (#883).
        if publish_calls >= 2:
            second_publish.set()
        await asyncio.Event().wait()

    with (
        patch("tg_webui.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("tg_webui.embedded_worker.start_container", AsyncMock()),
        patch("tg_webui.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("tg_webui.embedded_worker._publish_snapshots", new=hanging_publish),
        patch("tg_webui.embedded_worker.HEARTBEAT_INTERVAL_SEC", 0.001),
    ):
        await worker.start()
        try:
            await asyncio.wait_for(second_publish.wait(), timeout=2.0)
            assert publish_calls >= 2
        finally:
            await worker.stop(timeout=1.0)

    stop_container.assert_awaited_once_with(container)


async def test_embedded_worker_happy_path_becomes_ready_then_stops_cleanly():
    from tg_agent.config import AppConfig

    container = MagicMock()
    published = asyncio.Event()

    async def publish_once(_container, *, stop_event=None):
        published.set()

    worker = EmbeddedWorker(AppConfig())
    with (
        patch("tg_webui.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("tg_webui.embedded_worker.start_container", AsyncMock()) as start_container,
        patch("tg_webui.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("tg_webui.embedded_worker._publish_snapshots", new=publish_once),
        patch("tg_webui.embedded_worker.HEARTBEAT_INTERVAL_SEC", 0.001),
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
    from tg_agent.config import AppConfig

    container = MagicMock()
    exc = AccountSessionDecryptError(phone="+1234", status="key_mismatch")
    worker = EmbeddedWorker(AppConfig())
    with (
        patch("tg_webui.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("tg_webui.embedded_worker.start_container", AsyncMock(side_effect=exc)),
        patch("tg_webui.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch(
            "tg_webui.embedded_worker._publish_worker_down_snapshot", AsyncMock()
        ) as publish_down,
        patch("tg_webui.embedded_worker._publish_snapshots", AsyncMock()) as publish_snapshots,
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
    from tg_agent.config import AppConfig

    container = MagicMock()
    worker = EmbeddedWorker(AppConfig())
    with (
        patch("tg_webui.embedded_worker.build_worker_container", AsyncMock(return_value=container)),
        patch("tg_webui.embedded_worker.start_container", AsyncMock(side_effect=RuntimeError("boom"))),
        patch("tg_webui.embedded_worker.stop_container", AsyncMock()) as stop_container,
        patch("tg_webui.embedded_worker._publish_snapshots", AsyncMock()) as publish_snapshots,
    ):
        await worker.start()
        await worker.stop(timeout=1.0)

    assert worker.startup_failed is True
    assert "Embedded worker failed to start" in (worker.startup_error or "")
    publish_snapshots.assert_not_awaited()
    stop_container.assert_awaited_once_with(container)
    assert worker.container is None


async def test_embedded_worker_start_twice_raises_and_stop_without_start_is_noop():
    from tg_agent.config import AppConfig

    worker = EmbeddedWorker(AppConfig())
    # stop() before start() is a no-op
    await worker.stop(timeout=1.0)

    with (
        patch("tg_webui.embedded_worker.build_worker_container", AsyncMock(return_value=MagicMock())),
        patch("tg_webui.embedded_worker.start_container", AsyncMock()),
        patch("tg_webui.embedded_worker.stop_container", AsyncMock()),
        patch("tg_webui.embedded_worker._publish_snapshots", AsyncMock()),
        patch("tg_webui.embedded_worker.HEARTBEAT_INTERVAL_SEC", 0.001),
    ):
        await worker.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                await worker.start()
        finally:
            await worker.stop(timeout=1.0)
