"""Enqueued work wakes the worker's queue loops at once (#1868).

Before, every loop slept a fixed poll interval between empty claims (1 s for
``telegram_commands``, 3–5 s for task rows), so each enqueue waited for the next
tick. The poll stays only as a safety net.
"""

from __future__ import annotations

import asyncio
import os
import time
from types import SimpleNamespace

import pytest

from tg_agent.cli import runtime, worker_handoff
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.runtime import wake
from tg_agent.runtime.worker import _wake_op
from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher
from tg_agent.services.telegram_command_service import TelegramCommandService
from tg_agent.telegram.relay import relay_endpoint_path, serve_pool_relay

pytestmark = pytest.mark.aiosqlite_serial


@pytest.fixture(autouse=True)
def no_remote_hook():
    wake.set_remote(None)
    yield
    wake.set_remote(None)


async def test_notify_sets_every_subscriber_of_the_queue_only():
    commands = wake.subscribe(wake.TELEGRAM_COMMANDS)
    tasks_a = wake.subscribe(wake.TASKS)
    tasks_b = wake.subscribe(wake.TASKS)
    try:
        await wake.notify(wake.TASKS)
        assert tasks_a.is_set() and tasks_b.is_set()
        assert not commands.is_set()
    finally:
        for queue, event in ((wake.TELEGRAM_COMMANDS, commands), (wake.TASKS, tasks_a), (wake.TASKS, tasks_b)):
            wake.unsubscribe(queue, event)


async def test_wait_returns_on_notify_or_timeout():
    event = wake.subscribe(wake.TASKS)
    try:
        started = time.monotonic()
        await wake.wait(event, 0.05)
        assert time.monotonic() - started >= 0.04

        asyncio.get_running_loop().call_later(0.01, wake.notify_local, wake.TASKS)
        started = time.monotonic()
        await wake.wait(event, 5.0)
        assert time.monotonic() - started < 1.0
    finally:
        wake.unsubscribe(wake.TASKS, event)


async def test_remote_hook_failures_never_break_an_enqueue():
    async def broken(_queue):
        raise ConnectionError("daemon gone")

    wake.set_remote(broken)

    @wake.notifies(wake.TASKS)
    async def enqueue():
        return 7

    assert await enqueue() == 7


async def test_enqueued_command_is_picked_up_without_waiting_for_the_poll(tmp_path):
    db = Database(str(tmp_path / "wake.db"))
    await db.initialize()
    dispatcher = TelegramCommandDispatcher(db, pool=None)

    async def instant(command_type, payload):
        return {"ok": True}

    dispatcher._dispatch = instant
    await dispatcher.start()
    service = TelegramCommandService(db)
    try:
        await asyncio.sleep(0.05)  # let the loop reach its idle wait
        latencies = []
        for index in range(5):
            started = time.monotonic()
            command_id = await service.enqueue(
                "bench.noop", payload={"index": index}, requested_by="test", deduplicate=False
            )
            while (await service.get(command_id)).status.value != "succeeded":
                await asyncio.sleep(0.005)
            latencies.append(time.monotonic() - started)
    finally:
        await dispatcher.stop()
        await db.close()
    # The poll interval is 1 s; a woken loop answers in milliseconds.
    assert max(latencies) < 0.5, latencies


async def test_cli_enqueue_wakes_the_daemon_through_its_relay(tmp_path, monkeypatch):
    config = AppConfig()
    config.database.path = str(tmp_path / "project.db")
    owner_pool = SimpleNamespace(clients={})
    handle = await serve_pool_relay(owner_pool, relay_endpoint_path(config.database.path), ops={"wake": _wake_op})
    monkeypatch.setattr(worker_handoff, "serve_is_running", lambda _config: True)
    daemon_side = wake.subscribe(wake.TASKS)
    try:
        runtime._install_daemon_waker(config)
        hook = wake._remote
        assert hook is not None
        # Only the remote half: the event can be set by the daemon's wake op alone.
        await hook(wake.TASKS)
        assert daemon_side.is_set()
        assert handle.endpoint.pid == os.getpid()
    finally:
        wake.unsubscribe(wake.TASKS, daemon_side)
        await handle.stop()
