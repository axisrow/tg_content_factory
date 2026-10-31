"""Tests for src/cli/commands/serve.py and server_control.py — CLI serve/stop/restart."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.helpers import cli_ns, patch_serve_create_app
from tg_agent.cli.commands.server_control import run_restart, run_stop


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


@pytest.fixture(autouse=True)
def isolate_pid_file_candidates(monkeypatch):
    """Daemon lifecycle tests never probe the real machine's PID files.

    stop/restart, worker and serve enumerate PID-file candidates (#1698);
    pin them to one throwaway path so the suite stays machine-independent.
    """
    monkeypatch.setattr(
        "tg_agent.cli.process_control.pid_file_candidates",
        lambda config_path: [Path("/tmp/test.pid")],
    )


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


def test_serve_no_password():
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.password = ""
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg):
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))


def test_serve_register_fails():
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app(return_value=MagicMock()), \
         patch("tg_agent.cli.commands.serve.register_current_process", side_effect=RuntimeError("already running")):
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))


def test_serve_starts_server():
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    app = MagicMock()
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app(return_value=app), \
         patch("tg_agent.cli.commands.serve.register_current_process"), \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv, \
         patch("tg_agent.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        run(_args(web_pass=None))
        assert app.state.embed_worker is True
        mock_unreg.assert_called_once()


def test_serve_no_worker_disables_embedded_worker():
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    app = MagicMock()
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app(return_value=app), \
         patch("tg_agent.cli.commands.serve.register_current_process"), \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv, \
         patch("tg_agent.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock()
        run(_args(web_pass=None, no_worker=True))

    assert app.state.embed_worker is False
    mock_unreg.assert_called_once()


def test_serve_with_web_pass_override():
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app(return_value=MagicMock()), \
         patch("tg_agent.cli.commands.serve.register_current_process"), \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv, \
         patch("tg_agent.cli.commands.serve.unregister_current_process"):
        mock_uv.run = MagicMock()
        run(_args(web_pass="newpass-strong-enough"))
    assert cfg.web.password == "newpass-strong-enough"


# ---------------------------------------------------------------------------
# serve — #1303 non-loopback host + weak password fail-fast
# ---------------------------------------------------------------------------


def test_serve_non_loopback_with_weak_password_exits():
    """A non-loopback host with a known-weak password must refuse to start."""
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = "changeme"
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app() as mock_create_app, \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv:
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))
    mock_create_app.assert_not_called()
    mock_uv.run.assert_not_called()


def test_serve_loopback_with_weak_password_still_starts():
    """Local dev on 127.0.0.1 must keep working even with a weak password."""
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "127.0.0.1"
    cfg.web.password = "changeme"
    app = MagicMock()
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app(return_value=app), \
         patch("tg_agent.cli.commands.serve.register_current_process"), \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv, \
         patch("tg_agent.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        run(_args(web_pass=None))
    assert app.state.embed_worker is True
    mock_unreg.assert_called_once()


def test_serve_non_loopback_with_short_password_exits():
    """A short password outside the 3-word denylist must still fail fast.

    Round-2 review finding (#1305): the denylist alone let '123456', 'x',
    etc. through as "strong" once the panel is actually network-reachable.
    """
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = "1234567"  # 7 chars — below the minimum, not in the denylist
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app() as mock_create_app, \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv:
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))
    mock_create_app.assert_not_called()
    mock_uv.run.assert_not_called()


@pytest.mark.parametrize("weak_password", ["12345678", "aaaaaaaa", "        "])
def test_serve_non_loopback_with_low_entropy_password_exits(weak_password):
    """A long-enough but low-entropy password (repeated char, all-digit
    sequence, all-whitespace) must still fail fast.

    Round-3 review finding (#1305): the length-8 minimum alone let
    '12345678', 'aaaaaaaa', and whitespace-only values through as "strong".
    """
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = weak_password
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app() as mock_create_app, \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv:
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))
    mock_create_app.assert_not_called()
    mock_uv.run.assert_not_called()


@pytest.mark.parametrize("weak_password", ["Aa1!b", "abcdefgh", "hgfedcba"])
def test_serve_non_loopback_rejects_short_or_partial_sequences(weak_password):
    from tg_agent.cli.commands.serve import run

    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = weak_password
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app() as mock_create_app, \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv:
        with pytest.raises(SystemExit):
            run(_args(web_pass=None))
    mock_create_app.assert_not_called()
    mock_uv.run.assert_not_called()


def test_serve_non_loopback_with_strong_password_starts_with_warning(caplog):
    """A non-loopback host with a strong password should start but warn."""
    from tg_agent.cli.commands.serve import run
    cfg = make_app_config()
    cfg.web.host = "0.0.0.0"
    cfg.web.password = "a-strong-unique-password"
    app = MagicMock()
    with patch("tg_agent.cli.commands.serve.load_config", return_value=cfg), \
         patch_serve_create_app(return_value=app), \
         patch("tg_agent.cli.commands.serve.register_current_process"), \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv, \
         patch("tg_agent.cli.commands.serve.unregister_current_process") as mock_unreg:
        mock_uv.run = MagicMock(side_effect=KeyboardInterrupt)
        with caplog.at_level("WARNING"):
            run(_args(web_pass=None))
    assert app.state.embed_worker is True
    mock_unreg.assert_called_once()
    assert any("not localhost" in record.message for record in caplog.records)


def test_worker_starts_runtime():
    from tg_agent.cli.commands.worker import run

    cfg = make_app_config()
    with patch("tg_agent.cli.commands.worker.load_config", return_value=cfg), \
         patch("tg_agent.cli.commands.worker.register_current_process"), \
         patch("tg_agent.cli.commands.worker.run_worker") as mock_run_worker, \
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
         patch("tg_agent.cli.commands.worker.run_worker", side_effect=KeyboardInterrupt), \
         patch("tg_agent.cli.commands.worker.unregister_current_process") as mock_unreg:
        run(_args())  # KeyboardInterrupt is swallowed — same as serve

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
    # restart stops the old daemon, then becomes the worker runtime — the
    # automation-first default has no web panel (serve is the explicit
    # legacy entry point).
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


def test_serve_without_tg_webui_exits_with_install_hint(caplog):
    """The web panel is an optional package (#1720 wave 3): without it `serve`
    exits 1 with an install hint before loading the config or binding a port."""
    import sys

    from tg_agent.cli.commands.serve import run

    load_config = MagicMock()
    with patch.dict(sys.modules, {"tg_webui": None, "tg_webui.app": None}), \
         patch("tg_agent.cli.commands.serve.load_config", load_config), \
         patch("tg_agent.cli.commands.serve.uvicorn") as mock_uv, \
         pytest.raises(SystemExit) as exc_info:
        run(_args(web_pass=None))

    assert exc_info.value.code == 1
    assert "pip install -e packages/tg_webui" in caplog.text
    load_config.assert_not_called()
    mock_uv.run.assert_not_called()
