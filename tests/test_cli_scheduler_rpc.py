"""CLI ``scheduler trigger`` / ``task-cancel`` / ``clear-pending`` hand-off (#1568).

The CLI must not open a Telegram pool for these: it writes the immediate DB
effect and enqueues a telegram_command for the live worker, acking with the
command id. Fake-first: real :memory: Database, no Telegram, no worker.
"""

from __future__ import annotations

import pytest
import typer

from tg_agent.cli.commands import scheduler as scheduler_cmd
from tg_agent.models import TelegramCommandStatus


class _NoCloseDb:
    """Pass-through shell so ``*_impl``'s ``await db.close()`` can't kill the fixture DB."""

    def __init__(self, db):
        self._db = db

    def __getattr__(self, name):
        return getattr(self._db, name)

    async def close(self):
        pass  # the fixture owns the connection lifecycle


@pytest.fixture
async def patch_init_db(db, monkeypatch):
    """Point the CLI ``runtime.init_db`` at the shared :memory: test database."""

    async def _fake_init_db(_config_path):
        return None, _NoCloseDb(db)

    monkeypatch.setattr(scheduler_cmd.runtime, "init_db", _fake_init_db)
    return db


async def test_trigger_enqueues_worker_command(patch_init_db, monkeypatch, capsys):
    async def _no_pool(*_args, **_kwargs):
        raise AssertionError("trigger_impl must not open a Telegram pool (#1568)")

    monkeypatch.setattr(scheduler_cmd.runtime, "init_pool", _no_pool)

    await scheduler_cmd.trigger_impl("config.yaml")
    out = capsys.readouterr().out

    commands = await patch_init_db.repos.telegram_commands.list_commands(
        command_type="scheduler.trigger_collection"
    )
    assert len(commands) == 1
    assert commands[0].status == TelegramCommandStatus.PENDING
    assert f"#{commands[0].id}" in out
    assert "Enqueued" not in out and "serve" not in out


async def test_task_cancel_flips_row_and_enqueues_interrupt(patch_init_db, capsys):
    db = patch_init_db
    task_id = await db.repos.tasks.create_collection_task(100, "chan")

    await scheduler_cmd.task_cancel_impl("config.yaml", task_id=task_id)
    out = capsys.readouterr().out

    task_row = await db.repos.tasks.get_collection_task(task_id)
    assert task_row is not None and task_row.status.value == "cancelled"
    commands = await db.repos.telegram_commands.list_commands(
        command_type="collection.cancel_task"
    )
    assert len(commands) == 1
    assert commands[0].payload == {"task_id": task_id}
    assert f"#{commands[0].id}" in out


async def test_task_cancel_unknown_id_enqueues_nothing(patch_init_db, capsys):
    # A missing target is a failure: exit 1, the reason on stderr (#1933).
    with pytest.raises(typer.Exit) as excinfo:
        await scheduler_cmd.task_cancel_impl("config.yaml", task_id=424242)
    assert excinfo.value.exit_code == 1
    assert "not found" in capsys.readouterr().err

    assert (
        await patch_init_db.repos.telegram_commands.list_commands(
            command_type="collection.cancel_task"
        )
        == []
    )


async def test_clear_pending_deletes_rows_and_enqueues_drain(patch_init_db, capsys):
    db = patch_init_db
    await db.repos.tasks.create_collection_task(100, "a")
    await db.repos.tasks.create_collection_task(200, "b")

    await scheduler_cmd.clear_pending_impl("config.yaml")
    out = capsys.readouterr().out

    assert "Cleared 2" in out
    commands = await db.repos.telegram_commands.list_commands(
        command_type="collection.clear_pending"
    )
    assert len(commands) == 1
    assert f"#{commands[0].id}" in out
