"""Where a CLI process gets its Telegram connections (#1868).

``runtime.init_pool()`` is the shared entry point every live CLI command goes
through. The rule:

* no daemon → the CLI connects the accounts itself, on the main key;
* a live daemon → the pool runs on the daemon's connections through its relay
  (no second authorization, no session lock, no refusal);
* a live daemon without a relay (it predates this version) → a hard error that
  asks for one restart. A daemon that is still starting gets a short grace.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tg_agent.cli import runtime, worker_handoff
from tg_agent.cli.main import main
from tg_agent.config import AppConfig
from tg_agent.telegram.relay import (
    RELAY_PROTOCOL,
    RelayEndpoint,
    relay_endpoint_path,
    serve_pool_relay,
    write_endpoint,
)

pytestmark = pytest.mark.aiosqlite_serial

PHONE = "+10000000001"


@pytest.fixture
def pool_spy():
    """Replace the pool/auth constructors; record what the pool was built with."""
    pool = MagicMock()
    pool.initialize = AsyncMock()
    pool.disconnect_all = AsyncMock()
    pool.clients = {}
    with patch.object(runtime, "ClientPool", return_value=pool) as pool_cls, patch.object(
        runtime, "TelegramAuth"
    ) as auth_cls:
        yield pool_cls, auth_cls, pool


@pytest.fixture
def config(tmp_path) -> AppConfig:
    config = AppConfig()
    config.telegram.api_id = 1
    config.telegram.api_hash = "hash"
    config.database.path = str(tmp_path / "project.db")
    return config


@pytest.fixture
def daemon(monkeypatch):
    """A live daemon of this test process; ``age`` is how long ago it started."""
    state = SimpleNamespace(pid=os.getpid(), age=5.0)
    monkeypatch.setattr(worker_handoff, "serve_is_running", lambda _config: True)
    monkeypatch.setattr(worker_handoff, "running_daemon_pid", lambda _config: state.pid)
    monkeypatch.setattr(worker_handoff, "_daemon_age_sec", lambda _config: state.age)
    monkeypatch.setattr(worker_handoff, "_RELAY_POLL_SEC", 0.02)
    return state


def _db():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    return db


async def test_without_daemon_the_cli_connects_itself(config, pool_spy):
    # the autouse ``no_running_server`` fixture pins serve_is_running to False
    pool_cls, _auth_cls, pool = pool_spy

    _auth, got = await runtime.init_pool(config, _db(), phones=["+1"])

    assert got is pool
    assert pool_cls.call_args.kwargs["relay"] is None
    pool.initialize.assert_awaited_once_with(phones=["+1"])


async def test_next_to_a_daemon_the_pool_runs_on_its_relay(config, daemon, pool_spy):
    pool_cls, _auth_cls, pool = pool_spy
    owner_pool = SimpleNamespace(clients={PHONE: SimpleNamespace(raw_client=object())})
    handle = await serve_pool_relay(owner_pool, relay_endpoint_path(config.database.path))
    try:
        _auth, got = await runtime.init_pool(config, _db())
        relay = pool_cls.call_args.kwargs["relay"]
        assert got is pool
        assert relay is not None and relay.phones == [PHONE]
        await relay.close()
    finally:
        await handle.stop()


async def test_a_daemon_without_relay_is_a_hard_error_asking_for_restart(config, daemon, pool_spy):
    pool_cls, auth_cls, _pool = pool_spy
    daemon.age = 3600.0  # long running, still no relay: an older version

    with pytest.raises(runtime.DaemonRelayUnavailableError) as excinfo:
        await runtime.init_pool(config, _db())

    message = excinfo.value.format_message()
    assert excinfo.value.exit_code == 1
    assert f"PID {daemon.pid}" in message
    assert "tg-agent restart" in message
    pool_cls.assert_not_called()
    auth_cls.assert_not_called()


async def test_a_legacy_serve_daemon_fails_at_once_without_the_grace_wait(config, daemon, pool_spy, monkeypatch):
    pool_cls, _auth_cls, _pool = pool_spy
    daemon.age = 5.0  # inside the startup grace: a worker daemon would get time to publish its relay
    monkeypatch.setattr(worker_handoff, "daemon_is_legacy_panel", lambda _config: True)
    started = asyncio.get_running_loop().time()

    with pytest.raises(runtime.DaemonRelayUnavailableError) as excinfo:
        await runtime.init_pool(config, _db())

    assert asyncio.get_running_loop().time() - started < 1.0
    message = excinfo.value.format_message()
    assert "legacy `serve` panel" in message and "tg-agent restart" in message
    pool_cls.assert_not_called()


async def test_generation_runs_without_telegram_next_to_a_legacy_serve_daemon(
    config, daemon, pool_spy, monkeypatch, capsys
):
    """Generation needs no Telegram: a daemon without a usable relay does not stop it (#1521)."""
    from tg_agent.cli.pipeline.generation import _generation_pool

    pool_cls, _auth_cls, _pool = pool_spy
    monkeypatch.setattr(worker_handoff, "daemon_is_legacy_panel", lambda _config: True)

    assert await _generation_pool(config, _db()) is None

    err = capsys.readouterr().err
    assert "legacy `serve` panel" in err and "without Telegram access" in err
    pool_cls.assert_not_called()


def test_the_legacy_panel_is_recognised_by_its_command(config, monkeypatch):
    monkeypatch.setattr(worker_handoff, "read_pid", lambda _path: 4242)
    monkeypatch.setattr(worker_handoff.process_control, "_process_command",
                        lambda _pid: "/venv/bin/python -m tg_agent.main serve --web-pass x")
    assert worker_handoff.daemon_is_legacy_panel(config)
    monkeypatch.setattr(worker_handoff.process_control, "_process_command",
                        lambda _pid: "/venv/bin/python -m tg_agent.main --config serve.yaml worker")
    assert not worker_handoff.daemon_is_legacy_panel(config)


async def test_a_starting_daemon_gets_a_grace_to_publish_its_relay(config, daemon, pool_spy):
    pool_cls, _auth_cls, _pool = pool_spy
    owner_pool = SimpleNamespace(clients={PHONE: SimpleNamespace(raw_client=object())})
    handles = []

    async def publish_later():
        await asyncio.sleep(0.1)
        handles.append(await serve_pool_relay(owner_pool, relay_endpoint_path(config.database.path)))

    publisher = asyncio.create_task(publish_later())
    try:
        await runtime.init_pool(config, _db())
        relay = pool_cls.call_args.kwargs["relay"]
        assert relay.phones == [PHONE]
        await relay.close()
    finally:
        await publisher
        for handle in handles:
            await handle.stop()


async def test_a_relay_of_another_version_is_a_hard_error(config, daemon, pool_spy):
    pool_cls, _auth_cls, _pool = pool_spy
    write_endpoint(
        relay_endpoint_path(config.database.path),
        RelayEndpoint(port=1, token="x", pid=daemon.pid, protocol=RELAY_PROTOCOL + 1),
    )

    with pytest.raises(runtime.DaemonRelayUnavailableError) as excinfo:
        await runtime.init_pool(config, _db())

    assert "tg-agent restart" in excinfo.value.format_message()
    pool_cls.assert_not_called()


def test_daemon_age_comes_from_the_owner_record(config, tmp_path, monkeypatch):
    started = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()
    monkeypatch.setattr(
        worker_handoff, "read_process_owner", lambda _path: {"started_at": started, "pid": 1}
    )
    assert worker_handoff._daemon_age_sec(config) > 3600


def _run_main(argv: list[str]):
    with patch("sys.argv", ["main.py", *argv]):
        try:
            main()
        except SystemExit as exc:
            return exc.code
    return 0


def test_live_command_next_to_an_old_daemon_exits_with_the_restart_hint(cli_env, daemon, pool_spy, capsys):
    """End to end through ``main()``: exit 1, message on stderr, no pool built."""
    pool_cls, auth_cls, _pool = pool_spy
    daemon.age = 3600.0

    code = _run_main(["messages", "read", "@somechannel", "--live"])

    assert code == 1
    captured = capsys.readouterr()
    assert "tg-agent restart" in captured.err
    assert "Traceback" not in captured.err + captured.out
    auth_cls.assert_not_called()
    pool_cls.assert_not_called()


def test_dialogs_queue_status_never_opens_a_pool(cli_env, daemon, pool_spy, capsys):
    """The queue view is DB-only: no relay, no pool, daemon or not."""
    pool_cls, auth_cls, _pool = pool_spy

    code = _run_main(["dialogs", "queue", "status"])

    assert code == 0
    assert "Total: 0" in capsys.readouterr().out
    auth_cls.assert_not_called()
    pool_cls.assert_not_called()
