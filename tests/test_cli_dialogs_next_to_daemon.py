"""`dialogs` actions run on a pool, also next to a live daemon (#1868).

The CLI used to queue most actions into ``telegram_commands`` for the worker (1 s
poll, ``--wait`` polling for the result) and to refuse the rest while the daemon
ran. With the daemon's relay the pool runs on the daemon's own connection, so
every action runs synchronously here and nothing is queued. DB-only views never
open a pool at all.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

from tests.helpers import cli_ns as _ns
from tg_agent.cli.commands.dialogs import run_with_dependencies
from tg_agent.cli.worker_handoff import serve_is_running as _real_serve_is_running
from tg_agent.config import AppConfig

pytestmark = pytest.mark.aiosqlite_serial


def _mock_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    return pool


def _run(args, cli_db, *, daemon_running: bool, pool=None):
    config = AppConfig()
    used_pool = pool or _mock_pool()

    async def fake_init_db(_):
        return config, cli_db

    async def fake_init_pool(_, __, **_kwargs):
        from tg_shared.auth import TelegramAuth

        return TelegramAuth(0, ""), used_pool

    with (
        patch("tg_agent.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db),
        patch("tg_agent.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool) as init_pool,
        patch("tg_agent.cli.worker_handoff.serve_is_running", return_value=daemon_running),
    ):
        run_with_dependencies(args)
    return init_pool


def _commands(db):
    # The dispatcher closes the DB in its `finally`; tests reopen to inspect it.
    if db._connection.db is None:
        asyncio.run(db.initialize())
    return asyncio.run(db.repos.telegram_commands.list_commands(limit=10))


@pytest.mark.parametrize("daemon_running", [True, False], ids=["daemon", "no-daemon"])
def test_send_runs_on_the_pool_and_never_queues(cli_db, daemon_running):
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    with pytest.raises(typer.Exit):
        _run(
            _ns(dialogs_action="send", phone="+1234567890", recipient="100111", text="hello", yes=True),
            cli_db,
            daemon_running=daemon_running,
            pool=pool,
        )

    # The pool was built and torn down through the normal lifecycle (#1486:
    # the unavailable-client error exits non-zero after the teardown).
    pool.disconnect_all.assert_awaited_once()
    assert _commands(cli_db) == []


@pytest.mark.parametrize(
    "ns_kwargs",
    [
        {"dialogs_action": "cache-status", "phone": ""},
        {
            "dialogs_action": "queue",
            "queue_action": "status",
            "command_type": None,
            "phone": None,
            "limit": 20,
        },
    ],
    ids=["cache-status", "queue-status"],
)
@pytest.mark.parametrize("daemon_running", [True, False], ids=["daemon", "no-daemon"])
def test_db_only_actions_never_open_a_pool(cli_db, capsys, ns_kwargs, daemon_running):
    init_pool = _run(_ns(**ns_kwargs), cli_db, daemon_running=daemon_running, pool=_mock_pool())

    init_pool.assert_not_called()
    assert _commands(cli_db) == []
    out = capsys.readouterr().out
    assert "No cached dialogs." in out or "Total: 0" in out


def test_malformed_permission_values_fail_before_connecting(cli_db):
    init_pool = AsyncMock()
    with (
        patch("tg_agent.cli.commands.dialogs.runtime.init_pool", init_pool),
        pytest.raises(typer.BadParameter),
    ):
        run_with_dependencies(
            _ns(
                dialogs_action="edit-permissions",
                phone="+1234567890",
                chat_id="-100123",
                user_id="42",
                until_date=None,
                send_messages="maybe",
                send_media=None,
                yes=True,
            )
        )
    init_pool.assert_not_called()


def test_serve_is_running_survives_a_broken_pid_file(tmp_path, monkeypatch):
    """A malformed PID file must degrade to "no server", never crash the command."""
    from tg_agent.cli import worker_handoff

    config = AppConfig()
    bad_pid = tmp_path / "broken.pid"
    bad_pid.write_text("not-a-number\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: bad_pid)

    assert worker_handoff.serve_is_running(config) is False


def test_serve_is_running_is_false_for_no_worker_split_deployment(tmp_path, monkeypatch):
    """`serve --no-worker` must not look like a valid hand-off target.

    Regression (Codex, PR #1324 round 2): `serve_is_running` only checked the
    command line for `tg_agent.main serve`, not whether `--no-worker` was passed.
    In the documented split-deployment mode ("For split deployments (Docker/k8s)
    pass --no-worker and run `worker` separately" — CLAUDE.md), a CLI process
    that hands off while only a workerless `serve` is running would queue
    commands nothing ever picks up, silently reporting success. Fail safe:
    treat this as "no reliable worker" so the caller falls back to the direct
    path instead of enqueuing into a black hole.

    Uses the real function (`_real_serve_is_running`, imported at module load
    before the autouse `no_running_server` fixture patches the module
    attribute) — that fixture exists precisely to stop tests from exercising
    this logic by accident, so it must be bypassed here on purpose.
    """
    from tg_agent.cli import process_control, worker_handoff

    config = AppConfig()
    pid_path = tmp_path / "serve.pid"
    pid_path.write_text("4321\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: pid_path)

    with (
        patch.object(process_control, "is_process_alive", return_value=True),
        patch.object(process_control, "_process_command", return_value="python -m tg_agent.main serve --no-worker"),
    ):
        assert _real_serve_is_running(config) is False


def test_serve_is_running_is_true_for_plain_serve(tmp_path, monkeypatch):
    """Plain `serve` (the documented default: web + embedded worker) still hands off."""
    from tg_agent.cli import process_control, worker_handoff

    config = AppConfig()
    pid_path = tmp_path / "serve.pid"
    pid_path.write_text("4321\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: pid_path)

    with (
        patch.object(process_control, "is_process_alive", return_value=True),
        patch.object(process_control, "_process_command", return_value="python -m tg_agent.main serve"),
    ):
        assert _real_serve_is_running(config) is True


def test_serve_is_running_survives_process_command_raising(tmp_path, monkeypatch):
    """`_process_command` raising (e.g. PermissionError reading /proc) must not crash the command.

    Regression (Claude's built-in /review, PR #1324 post-audit re-review):
    the try/except in `serve_is_running` only wrapped `read_pid(...)`, not
    the `process_control._process_command(pid).split()` call added in
    round 2 — an unexpected exception there (the underlying `os.kill`/
    `/proc` read/`ps` subprocess call is not guaranteed exception-free on
    every platform) would propagate uncaught, contradicting the
    docstring's "Never raises" guarantee.
    """
    from tg_agent.cli import process_control, worker_handoff

    config = AppConfig()
    pid_path = tmp_path / "serve.pid"
    pid_path.write_text("4321\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: pid_path)

    with (
        patch.object(process_control, "is_process_alive", return_value=True),
        patch.object(process_control, "_process_command", side_effect=PermissionError("denied")),
    ):
        assert _real_serve_is_running(config) is False
