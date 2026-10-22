"""A live daemon owns the Telegram sessions: the CLI must not open a second pool (#1521).

``runtime.init_pool()`` is the shared entry point every inline-Telegram CLI command
goes through. Before #1521 it never looked at the daemon, so with ``tg-agent
restart`` running a command such as ``messages read --live`` built its own
``ClientPool``, waited out the per-session lock grace (#1548) for every account
and then degraded into "No connected accounts" — or, before the lock existed,
opened a second MTProto connection on the daemon's auth key.

Contract pinned here: while a hand-off-capable daemon is live, ``init_pool``
refuses before any ``TelegramAuth``/``ClientPool`` is built, with a non-zero
exit and an actionable message. ``allow_live_daemon=True`` (callers with their
own ``--direct`` flag, or the frozen long-lived surfaces) and the
``TGCF_CLI_DIRECT=1`` escape hatch keep the in-process path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tg_agent.cli import runtime, worker_handoff
from tg_agent.cli.main import main
from tg_agent.config import AppConfig

pytestmark = pytest.mark.aiosqlite_serial


@pytest.fixture
def daemon_live(monkeypatch):
    monkeypatch.setattr(worker_handoff, "serve_is_running", lambda _config: True)
    monkeypatch.setattr(worker_handoff, "running_daemon_pid", lambda _config: 4242, raising=False)
    monkeypatch.delenv(worker_handoff.DIRECT_ENV_VAR, raising=False)


@pytest.fixture
def pool_spy():
    """Replace the pool/auth constructors; record whether anything was built."""
    pool = MagicMock()
    pool.initialize = AsyncMock()
    pool.disconnect_all = AsyncMock()
    pool.clients = {}
    with patch.object(runtime, "ClientPool", return_value=pool) as pool_cls, patch.object(
        runtime, "TelegramAuth"
    ) as auth_cls:
        yield pool_cls, auth_cls, pool


def _db():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    return db


def _config() -> AppConfig:
    config = AppConfig()
    config.telegram.api_id = 1
    config.telegram.api_hash = "hash"
    return config


async def test_init_pool_refuses_while_daemon_owns_sessions(daemon_live, pool_spy):
    pool_cls, auth_cls, pool = pool_spy

    with pytest.raises(runtime.DaemonOwnsSessionsError) as excinfo:
        await runtime.init_pool(_config(), _db())

    assert excinfo.value.exit_code == 1
    message = excinfo.value.format_message()
    assert "PID 4242" in message
    assert "tg-agent stop" in message
    auth_cls.assert_not_called()
    pool_cls.assert_not_called()
    pool.initialize.assert_not_awaited()


async def test_init_pool_allow_live_daemon_keeps_direct_path(daemon_live, pool_spy):
    pool_cls, _auth_cls, pool = pool_spy

    _auth, got = await runtime.init_pool(_config(), _db(), allow_live_daemon=True)

    assert got is pool
    pool_cls.assert_called_once()
    pool.initialize.assert_awaited_once()


async def test_init_pool_env_escape_hatch_keeps_direct_path(daemon_live, pool_spy, monkeypatch):
    _pool_cls, _auth_cls, pool = pool_spy
    monkeypatch.setenv(worker_handoff.DIRECT_ENV_VAR, "1")

    _auth, got = await runtime.init_pool(_config(), _db())

    assert got is pool
    pool.initialize.assert_awaited_once()


async def test_init_pool_without_daemon_is_unchanged(pool_spy):
    # the autouse ``no_running_server`` fixture pins serve_is_running to False
    _pool_cls, _auth_cls, pool = pool_spy

    _auth, got = await runtime.init_pool(_config(), _db(), phones=["+1"])

    assert got is pool
    pool.initialize.assert_awaited_once_with(phones=["+1"])


def _run_main(argv: list[str]):
    with patch("sys.argv", ["main.py", *argv]):
        try:
            main()
        except SystemExit as exc:
            return exc.code
    return 0


@pytest.mark.parametrize(
    "argv",
    [
        ["messages", "read", "@somechannel", "--live"],
        ["account", "info"],
        ["notification", "status"],
        ["search", "query", "--mode", "telegram"],
        ["dialogs", "list"],
    ],
    ids=["messages-read-live", "account-info", "notification-status", "search-telegram", "dialogs-list"],
)
def test_cli_commands_refuse_without_opening_a_pool(argv, cli_env, daemon_live, pool_spy, capsys):
    """End to end through ``main()``: exit 1, message on stderr, no pool built."""
    pool_cls, auth_cls, _pool = pool_spy

    code = _run_main(argv)

    assert code == 1
    captured = capsys.readouterr()
    assert "owns the Telegram sessions" in captured.err
    assert "Traceback" not in captured.err + captured.out
    auth_cls.assert_not_called()
    pool_cls.assert_not_called()


def test_dialogs_queue_status_still_works_next_to_the_daemon(cli_env, daemon_live, pool_spy, capsys):
    """The hand-off's own status view is DB-only: no refusal, no pool."""
    pool_cls, auth_cls, _pool = pool_spy

    code = _run_main(["dialogs", "queue", "status"])

    assert code == 0
    assert "Total: 0" in capsys.readouterr().out
    auth_cls.assert_not_called()
    pool_cls.assert_not_called()
