"""Tests for src/cli/commands/serve.py and server_control.py — CLI serve/stop/restart."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.commands.server_control import run_restart, run_stop
from tests.helpers import cli_ns


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return cli_ns(**defaults)


def make_app_config():
    cfg = MagicMock()
    cfg.web.password = "testpass"
    cfg.web.host = "0.0.0.0"
    cfg.web.port = 8080
    cfg.database.path = "data/test.db"
    return cfg


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


def test_serve_no_password():
    from src.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.password = ""
    with patch("src.cli.commands.serve.load_config", return_value=cfg):
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))


def test_serve_register_fails():
    from src.cli.commands.serve import run
    cfg = make_app_config()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=MagicMock()), \
         patch("src.cli.commands.serve.register_current_process", side_effect=RuntimeError("already running")):
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))


def test_serve_starts_server():
    from src.cli.commands.serve import run
    cfg = make_app_config()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=MagicMock()), \
         patch("src.cli.commands.serve.register_current_process"), \
         patch("src.cli.commands.serve.uvicorn") as mock_uv, \
         patch("src.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        run(_args(web_pass=None))
        mock_unreg.assert_called_once()


def test_serve_with_web_pass_override():
    from src.cli.commands.serve import run
    cfg = make_app_config()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=MagicMock()), \
         patch("src.cli.commands.serve.register_current_process"), \
         patch("src.cli.commands.serve.uvicorn") as mock_uv, \
         patch("src.cli.commands.serve.unregister_current_process"):
        mock_uv.run = MagicMock()
        run(_args(web_pass="newpass"))
    assert cfg.web.password == "newpass"


def test_worker_starts_runtime():
    from src.cli.commands.worker import run

    cfg = make_app_config()
    with patch("src.cli.commands.worker.load_config", return_value=cfg), \
         patch("src.cli.commands.worker.run_worker") as mock_run_worker:
        run(_args())

    mock_run_worker.assert_called_once_with(cfg)


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


def test_stop_success(capsys):
    cfg = make_app_config()
    outcome = MagicMock()
    outcome.message = "Server stopped."
    from src.cli.commands.server_control import StopResult
    outcome.result = StopResult.STOPPED
    with patch("src.cli.commands.server_control.load_config", return_value=cfg), \
         patch("src.cli.commands.server_control.stop_server", return_value=outcome), \
         patch("src.cli.commands.server_control.pid_file_path", return_value="/tmp/test.pid"):
        run_stop(_args())
    assert "stopped" in capsys.readouterr().out.lower()


def test_stop_process_control_error():
    cfg = make_app_config()
    from src.cli.commands.server_control import ProcessControlError
    with patch("src.cli.commands.server_control.load_config", return_value=cfg), \
         patch("src.cli.commands.server_control.stop_server", side_effect=ProcessControlError("no pid")), \
         patch("src.cli.commands.server_control.pid_file_path", return_value="/tmp/test.pid"):
        with pytest.raises(SystemExit):
            run_stop(_args())


def test_stop_timeout():
    cfg = make_app_config()
    outcome = MagicMock()
    outcome.message = "Timeout"
    from src.cli.commands.server_control import StopResult
    outcome.result = StopResult.TIMEOUT
    with patch("src.cli.commands.server_control.load_config", return_value=cfg), \
         patch("src.cli.commands.server_control.stop_server", return_value=outcome), \
         patch("src.cli.commands.server_control.pid_file_path", return_value="/tmp/test.pid"):
        with pytest.raises(SystemExit):
            run_stop(_args())


# ---------------------------------------------------------------------------
# restart
# ---------------------------------------------------------------------------


def test_restart_success(capsys):
    cfg = make_app_config()
    outcome = MagicMock()
    outcome.message = "Stopped."
    from src.cli.commands.server_control import StopResult
    outcome.result = StopResult.STOPPED
    with patch("src.cli.commands.server_control.load_config", return_value=cfg), \
         patch("src.cli.commands.server_control.stop_server", return_value=outcome), \
         patch("src.cli.commands.server_control.pid_file_path", return_value="/tmp/test.pid"), \
         patch("src.cli.commands.serve.run") as mock_serve:
        run_restart(_args())
    mock_serve.assert_called_once()
