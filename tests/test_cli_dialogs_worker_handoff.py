"""`dialogs` actions must go through the command queue while a server is running.

Same root cause as the collection hand-off: `serve` owns every
``data/telegram_sessions/<phone>.session`` file, so a CLI process that builds its
own ``ClientPool`` opens them a second time and risks ``database is locked``.

The ``telegram_commands`` queue already covers these operations — the web UI
enqueues ``dialogs.send`` and friends, and ``TelegramCommandDispatcher`` executes
them inside the worker. These tests pin the CLI to that same queue.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.dialogs import run_with_dependencies
from src.config import AppConfig
from tests.helpers import cli_ns as _ns

pytestmark = pytest.mark.aiosqlite_serial


def _mock_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    return pool


def _run(args, cli_db, *, serve_running: bool, pool=None):
    config = AppConfig()
    used_pool = pool or _mock_pool()

    async def fake_init_db(_):
        return config, cli_db

    async def fake_init_pool(_, __):
        from src.telegram.auth import TelegramAuth

        return TelegramAuth(0, ""), used_pool

    with (
        patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool) as init_pool,
        patch(
            "src.cli.commands.dialogs.worker_handoff.serve_is_running",
            return_value=serve_running,
        ),
    ):
        run_with_dependencies(args)
    return init_pool


def _commands(db):
    # The dispatcher closes the DB in its `finally`; tests reopen to inspect it.
    if db._connection.db is None:
        asyncio.run(db.initialize())
    return asyncio.run(db.repos.telegram_commands.list_commands(limit=10))


def test_send_enqueues_command_when_serve_is_running(cli_db, capsys):
    """`dialogs send` must queue the message rather than open its own connection."""
    init_pool = _run(
        _ns(
            dialogs_action="send",
            phone="+1234567890",
            recipient="100111",
            text="hello",
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    init_pool.assert_not_called()

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.send"]
    assert commands[0].payload["recipient"] == "100111"
    assert commands[0].payload["text"] == "hello"
    assert commands[0].payload["phone"] == "+1234567890"

    assert "очередь" in capsys.readouterr().out.lower()


def test_join_enqueues_command_when_serve_is_running(cli_db, capsys):
    """A second action proves the mapping is table-driven, not special-cased."""
    _run(
        _ns(dialogs_action="join", phone="+1234567890", target="@somechannel", yes=True, direct=False),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.join"]
    assert commands[0].payload["target"] == "@somechannel"


def test_send_runs_directly_when_serve_is_not_running(cli_db):
    """Offline behaviour is unchanged — the pool is still built and used."""
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    init_pool = _run(
        _ns(
            dialogs_action="send",
            phone="+1234567890",
            recipient="100111",
            text="hello",
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=False,
        pool=pool,
    )

    init_pool.assert_called_once()
    assert _commands(cli_db) == []


def test_direct_flag_bypasses_the_queue(cli_db):
    """`--direct` keeps the escape hatch consistent with `channel collect`."""
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    init_pool = _run(
        _ns(
            dialogs_action="send",
            phone="+1234567890",
            recipient="100111",
            text="hello",
            yes=True,
            direct=True,
        ),
        cli_db,
        serve_running=True,
        pool=pool,
    )

    init_pool.assert_called_once()
    assert _commands(cli_db) == []


def test_read_only_actions_still_run_locally(cli_db):
    """Listing and cache inspection have no queue equivalent and must not hang.

    Only actions with a real ``dialogs.*`` command type are handed over; local
    read-only views keep working against the DB as before.
    """
    pool = _mock_pool()
    pool.get_dialogs_for_phone = AsyncMock(return_value=[])

    init_pool = _run(
        _ns(dialogs_action="cache-status", phone="", direct=False),
        cli_db,
        serve_running=True,
        pool=pool,
    )

    init_pool.assert_called_once()
    assert _commands(cli_db) == []


def test_confirmation_is_still_required_before_enqueue(cli_db):
    """Declining the prompt must not queue anything.

    The hand-off happens after the confirmation gate, so a refused action leaves
    no trace — otherwise "no" would still send the message, just later.
    """
    with patch("builtins.input", return_value="n"):
        _run(
            _ns(
                dialogs_action="send",
                phone="+1234567890",
                recipient="100111",
                text="hello",
                yes=False,
                direct=False,
            ),
            cli_db,
            serve_running=True,
        )

    assert _commands(cli_db) == []


def test_direct_env_var_is_honoured(monkeypatch):
    """`dialogs` shares one bridge across 27 leaves, so its escape hatch is env-based."""
    from src.cli import worker_handoff

    monkeypatch.delenv(worker_handoff.DIRECT_ENV_VAR, raising=False)
    assert worker_handoff.direct_requested() is False

    for value in ("1", "true", "YES"):
        monkeypatch.setenv(worker_handoff.DIRECT_ENV_VAR, value)
        assert worker_handoff.direct_requested() is True

    monkeypatch.setenv(worker_handoff.DIRECT_ENV_VAR, "0")
    assert worker_handoff.direct_requested() is False


def test_serve_is_running_survives_a_broken_pid_file(tmp_path, monkeypatch):
    """A malformed PID file must degrade to "no server", never crash the command."""
    from src.cli import worker_handoff

    config = AppConfig()
    bad_pid = tmp_path / "broken.pid"
    bad_pid.write_text("not-a-number\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: bad_pid)

    assert worker_handoff.serve_is_running(config) is False
