"""CLI collection commands must hand work to a running worker, not compete with it.

`serve` (web plus the embedded worker) owns the Telegram sessions: every account's
``data/telegram_sessions/<phone>.session`` file is opened for the process lifetime.
A CLI process that builds its own ``ClientPool`` opens the very same files — the
path is derived from the phone number alone — and dies with
``sqlite3.OperationalError: database is locked`` inside telethon's
``process_entities``.

The queue that fixes this already exists: ``CollectionService`` writes a PENDING
row when it has no in-process queue, and the worker's ``_db_pull_loop`` picks such
rows up every ``DB_PULL_INTERVAL_SEC`` seconds. These tests pin the CLI to that
path whenever a managed ``serve`` process is live.
"""

from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pydantic.root_model  # noqa: F401
import pytest

from src.config import AppConfig
from src.models import Channel, CollectionTaskStatus

pytestmark = pytest.mark.aiosqlite_serial


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _ensure_db_open(db):
    """Re-open the CLI database if the command closed it in its ``finally``."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


@pytest.fixture
def channel_cli(cli_db, cli_init_patch):
    with cli_init_patch(
        cli_db,
        "src.cli.commands.channel.runtime.init_db",
        config=AppConfig(),
    ):
        yield cli_db


def _add_channel(db, channel_id: int, title: str) -> int:
    return asyncio.run(db.add_channel(Channel(channel_id=channel_id, title=title)))


def test_collect_hands_off_to_worker_when_serve_is_running(channel_cli, capsys):
    """With `serve` alive the CLI must enqueue instead of opening its own pool.

    This is the regression: the direct path opens the session files `serve`
    already holds, which is what produced `database is locked` in production.
    """
    db = channel_cli
    pk = _add_channel(db, 5001, "HandOff")

    init_pool = AsyncMock()
    with (
        patch("src.cli.commands.channel.worker_handoff.serve_is_running", return_value=True),
        patch("src.cli.commands.channel.runtime.init_pool", init_pool),
    ):
        from src.cli.commands.channel import run

        run(_ns(channel_action="collect", identifier=str(pk), full=False, direct=False, wait=False))

    init_pool.assert_not_awaited()

    _ensure_db_open(db)
    tasks = asyncio.run(db.get_collection_tasks(limit=5))
    assert [t.channel_id for t in tasks] == [5001]
    assert tasks[0].status == CollectionTaskStatus.PENDING

    assert "очередь" in capsys.readouterr().out.lower()


def test_collect_runs_directly_when_serve_is_not_running(channel_cli, capsys):
    """Without a live server the CLI keeps collecting in-process as before."""
    db = channel_cli
    pk = _add_channel(db, 5002, "DirectRun")

    pool = AsyncMock()
    pool.clients = {"+70001112233": MagicMock()}
    pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db_arg):
        from src.telegram.auth import TelegramAuth

        return TelegramAuth(0, ""), pool

    with (
        patch("src.cli.commands.channel.worker_handoff.serve_is_running", return_value=False),
        patch("src.cli.commands.channel.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.channel.Collector") as collector_cls,
    ):
        collector_cls.return_value.collect_single_channel = AsyncMock(return_value=7)
        from src.cli.commands.channel import run

        run(_ns(channel_action="collect", identifier=str(pk), full=False, direct=False, wait=False))

    assert "Collected 7 messages" in capsys.readouterr().out

    _ensure_db_open(db)
    tasks = asyncio.run(db.get_collection_tasks(limit=1))
    assert tasks[0].status == CollectionTaskStatus.COMPLETED
    assert tasks[0].messages_collected == 7


def test_direct_flag_overrides_running_serve(channel_cli, capsys):
    """`--direct` is the deliberate escape hatch for debugging a live server."""
    db = channel_cli
    pk = _add_channel(db, 5003, "ForcedDirect")

    pool = AsyncMock()
    pool.clients = {"+70001112233": MagicMock()}
    pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db_arg):
        from src.telegram.auth import TelegramAuth

        return TelegramAuth(0, ""), pool

    with (
        patch("src.cli.commands.channel.worker_handoff.serve_is_running", return_value=True),
        patch("src.cli.commands.channel.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.channel.Collector") as collector_cls,
    ):
        collector_cls.return_value.collect_single_channel = AsyncMock(return_value=3)
        from src.cli.commands.channel import run

        run(_ns(channel_action="collect", identifier=str(pk), full=False, direct=True, wait=False))

    assert "Collected 3 messages" in capsys.readouterr().out


def test_handoff_reports_already_active_task(channel_cli, capsys):
    """A duplicate hand-off must say so rather than silently creating nothing.

    ``create_collection_task_if_not_active`` is race-safe and returns ``None``
    when an active task exists; the CLI has to surface that distinction.
    """
    db = channel_cli
    pk = _add_channel(db, 5004, "Duplicate")

    with (
        patch("src.cli.commands.channel.worker_handoff.serve_is_running", return_value=True),
        patch("src.cli.commands.channel.runtime.init_pool", AsyncMock()),
    ):
        from src.cli.commands.channel import run

        args = dict(channel_action="collect", identifier=str(pk), full=False, direct=False, wait=False)
        run(_ns(**args))
        capsys.readouterr()
        _ensure_db_open(db)
        run(_ns(**args))

    assert "уже" in capsys.readouterr().out.lower()

    _ensure_db_open(db)
    tasks = asyncio.run(db.get_collection_tasks(limit=5))
    assert len(tasks) == 1


def test_worker_picks_up_a_cli_enqueued_task(channel_cli):
    """Close the loop: the row the CLI writes is one the worker actually ingests.

    Guards against a hand-off that merely inserts a row the pull loop filters out
    (wrong task_type, wrong status), which would strand the task forever.
    """
    db = channel_cli
    pk = _add_channel(db, 5005, "PulledByWorker")

    with (
        patch("src.cli.commands.channel.worker_handoff.serve_is_running", return_value=True),
        patch("src.cli.commands.channel.runtime.init_pool", AsyncMock()),
    ):
        from src.cli.commands.channel import run

        run(_ns(channel_action="collect", identifier=str(pk), full=False, direct=False, wait=False))

    _ensure_db_open(db)
    pending = asyncio.run(db.get_pending_channel_tasks())
    assert [t.channel_id for t in pending] == [5005]
