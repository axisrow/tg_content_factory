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
    app = MagicMock()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=app), \
         patch("src.cli.commands.serve.register_current_process"), \
         patch("src.cli.commands.serve.uvicorn") as mock_uv, \
         patch("src.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        run(_args(web_pass=None))
        assert app.state.embed_worker is True
        mock_unreg.assert_called_once()


def test_serve_no_worker_disables_embedded_worker():
    from src.cli.commands.serve import run
    cfg = make_app_config()
    app = MagicMock()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=app), \
         patch("src.cli.commands.serve.register_current_process"), \
         patch("src.cli.commands.serve.uvicorn") as mock_uv, \
         patch("src.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock()
        run(_args(web_pass=None, no_worker=True))

    assert app.state.embed_worker is False
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


# ---------------------------------------------------------------------------
# serve — #1303 non-loopback host + weak password fail-fast
# ---------------------------------------------------------------------------


def test_serve_non_loopback_with_weak_password_exits():
    """A non-loopback host with a known-weak password must refuse to start."""
    from src.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = "changeme"
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app") as mock_create_app, \
         patch("src.cli.commands.serve.uvicorn") as mock_uv:
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))
    mock_create_app.assert_not_called()
    mock_uv.run.assert_not_called()


def test_serve_loopback_with_weak_password_still_starts():
    """Local dev on 127.0.0.1 must keep working even with a weak password."""
    from src.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "127.0.0.1"
    cfg.web.password = "changeme"
    app = MagicMock()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=app), \
         patch("src.cli.commands.serve.register_current_process"), \
         patch("src.cli.commands.serve.uvicorn") as mock_uv, \
         patch("src.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        run(_args(web_pass=None))
    assert app.state.embed_worker is True
    mock_unreg.assert_called_once()


def test_serve_non_loopback_with_strong_password_starts_with_warning(caplog):
    """A non-loopback host with a strong password should start but warn."""
    from src.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = "a-strong-unique-password"
    app = MagicMock()
    with patch("src.cli.commands.serve.load_config", return_value=cfg), \
         patch("src.cli.commands.serve.create_app", return_value=app), \
         patch("src.cli.commands.serve.register_current_process"), \
         patch("src.cli.commands.serve.uvicorn") as mock_uv, \
         patch("src.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        with caplog.at_level("WARNING"):
            run(_args(web_pass=None))
    assert app.state.embed_worker is True
    mock_unreg.assert_called_once()
    assert any("not localhost" in record.message for record in caplog.records)


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
    out = capsys.readouterr().out
    assert "подожду завершения активной задачи" in out
    assert "stopped" in out.lower()


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
         patch("src.cli.commands.serve.serve_web") as mock_serve:
        run_restart(_args())
    out = capsys.readouterr().out
    assert "подожду завершения активной задачи" in out
    # restart stops the running server, then starts a fresh `serve` via the
    # shared serve_web body (no more Namespace hand-off).
    mock_serve.assert_called_once()
