"""Tests for the worker daemon commands: worker, stop, restart (cli/commands/{worker,server_control}.py)."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.helpers import cli_ns
from tg_agent.cli.commands.server_control import run_restart, run_stop


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return cli_ns(**defaults)


def make_app_config():
    cfg = MagicMock()
    cfg.database.path = "data/test.db"
    return cfg


@pytest.fixture(autouse=True)
def isolate_pid_file_candidates(monkeypatch):
    """Daemon lifecycle tests never probe the real machine's PID files.

    stop/restart and worker enumerate PID-file candidates (#1698);
    pin them to one throwaway path so the suite stays machine-independent.
    """
    monkeypatch.setattr(
        "tg_agent.cli.process_control.pid_file_candidates",
        lambda config_path: [Path("/tmp/test.pid")],
    )


# ---------------------------------------------------------------------------
# worker
# ---------------------------------------------------------------------------


def test_worker_starts_runtime():
    from tg_agent.cli.commands.worker import run

    cfg = make_app_config()
    with patch("tg_agent.cli.commands.worker.load_config", return_value=cfg), \
         patch("tg_agent.cli.commands.worker.register_current_process"), \
         patch("tg_agent.runtime.worker.run_worker") as mock_run_worker, \
         patch("tg_agent.cli.commands.worker.unregister_current_process") as mock_unreg:
        run(_args())

    mock_run_worker.assert_called_once_with(cfg)
    mock_unreg.assert_called_once()


def test_worker_registers_pid_and_survives_keyboard_interrupt():
    """The managed worker daemon registers the PID file and unregisters on exit."""
    from tg_agent.cli.commands.worker import run

    cfg = make_app_config()
    with patch("tg_agent.cli.commands.worker.load_config", return_value=cfg), \
         patch("tg_agent.cli.commands.worker.pid_file_path", return_value="/tmp/test.pid"), \
         patch("tg_agent.cli.commands.worker.register_current_process") as mock_reg, \
         patch("tg_agent.runtime.worker.run_worker", side_effect=KeyboardInterrupt), \
         patch("tg_agent.cli.commands.worker.unregister_current_process") as mock_unreg:
        run(_args())  # KeyboardInterrupt is swallowed

    mock_reg.assert_called_once()
    mock_unreg.assert_called_once()


def test_worker_register_failure_exits():
    from tg_agent.cli.commands.worker import run

    cfg = make_app_config()
    with patch("tg_agent.cli.commands.worker.load_config", return_value=cfg), \
         patch("tg_agent.cli.commands.worker.register_current_process",
               side_effect=RuntimeError("already running")):
        with pytest.raises(SystemExit):
            run(_args())


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


def test_stop_success(capsys):
    outcome = MagicMock()
    outcome.message = "Server stopped."
    from tg_agent.cli.commands.server_control import StopResult
    outcome.result = StopResult.STOPPED
    with patch("tg_agent.cli.commands.server_control.stop_server", return_value=outcome):
        run_stop(_args())
    out = capsys.readouterr().out
    assert "подожду завершения активной задачи" in out
    assert "stopped" in out.lower()


def test_stop_process_control_error():
    from tg_agent.cli.commands.server_control import ProcessControlError
    with patch("tg_agent.cli.commands.server_control.stop_server", side_effect=ProcessControlError("no pid")):
        with pytest.raises(SystemExit):
            run_stop(_args())


def test_stop_timeout():
    outcome = MagicMock()
    outcome.message = "Timeout"
    from tg_agent.cli.commands.server_control import StopResult
    outcome.result = StopResult.TIMEOUT
    with patch("tg_agent.cli.commands.server_control.stop_server", return_value=outcome):
        with pytest.raises(SystemExit):
            run_stop(_args())


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------


def test_restart_success(capsys):
    outcome = MagicMock()
    outcome.message = "Stopped."
    from tg_agent.cli.commands.server_control import StopResult
    outcome.result = StopResult.STOPPED
    with patch("tg_agent.cli.commands.server_control.stop_server", return_value=outcome), \
         patch("tg_agent.cli.commands.worker.serve_worker") as mock_worker:
        run_restart(_args())
    out = capsys.readouterr().out
    assert "подожду завершения активной задачи" in out
    # restart stops the old daemon, then becomes the worker runtime.
    mock_worker.assert_called_once_with("config.yaml")


def test_restart_path_has_no_web_server():
    """Structural guard: the restart path must not host the web panel.

    The automation-first contract (#1446 successor): the default daemon is
    the worker runtime — no uvicorn, no legacy `serve_web` hop.
    """
    from pathlib import Path

    from tg_agent.cli.commands import server_control

    source = Path(server_control.__file__).read_text(encoding="utf-8")
    assert "uvicorn" not in source, "restart path must not host a web server"
    assert "serve_web" not in source, "restart must not reuse the legacy serve body"
