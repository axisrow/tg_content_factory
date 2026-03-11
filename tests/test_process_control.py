from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.cli.process_control import (
    ProcessControlError,
    StopResult,
    ensure_server_not_running,
    pid_file_path,
    register_current_process,
    stop_server,
    unregister_current_process,
)
from src.config import AppConfig


def test_pid_file_path_uses_database_path_suffix():
    config = AppConfig()
    config.database.path = "data/custom.db"
    assert pid_file_path(config) == Path("data/custom.pid")


def test_register_and_unregister_current_process(tmp_path):
    path = tmp_path / "app.pid"

    register_current_process(path)
    assert path.read_text(encoding="utf-8").strip() == str(os.getpid())

    unregister_current_process(path)
    assert not path.exists()


def test_ensure_server_not_running_raises_for_live_server(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("src.cli.process_control.is_expected_server_process", return_value=True):
        with pytest.raises(ProcessControlError, match="Server already running"):
            ensure_server_not_running(path)


def test_ensure_server_not_running_cleans_stale_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("src.cli.process_control.is_expected_server_process", return_value=False):
        ensure_server_not_running(path)

    assert not path.exists()


def test_stop_server_without_pid_file(tmp_path):
    outcome = stop_server(tmp_path / "missing.pid")
    assert outcome.result is StopResult.NOT_RUNNING
    assert "not running" in outcome.message


def test_stop_server_cleans_stale_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("src.cli.process_control.is_process_alive", return_value=False):
        outcome = stop_server(path)

    assert outcome.result is StopResult.STALE_PID
    assert "stale PID file" in outcome.message
    assert not path.exists()


def test_stop_server_rejects_unmanaged_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("src.cli.process_control.is_process_alive", return_value=True):
        with patch("src.cli.process_control.is_expected_server_process", return_value=False):
            outcome = stop_server(path)

    assert outcome.result is StopResult.UNMANAGED
    assert "not a managed" in outcome.message
    assert path.exists()


def test_stop_server_stops_process_and_removes_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    alive_states = iter([True, False])

    with patch(
        "src.cli.process_control.is_process_alive",
        side_effect=lambda pid: next(alive_states),
    ):
        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("src.cli.process_control.os.kill") as mock_kill:
                outcome = stop_server(path, timeout_sec=0.5)

    assert outcome.result is StopResult.STOPPED
    assert "Server stopped" in outcome.message
    mock_kill.assert_called_once()
    assert not path.exists()


def test_stop_server_timeout(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("src.cli.process_control.is_process_alive", return_value=True):
        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("src.cli.process_control.os.kill"):
                outcome = stop_server(path, timeout_sec=0.0, kill_timeout_sec=0.0)

    assert outcome.result is StopResult.TIMEOUT
    assert "Timed out" in outcome.message
    assert path.exists()


def test_stop_server_force_kill_removes_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    alive_states = iter([True, False])

    with patch(
        "src.cli.process_control.is_process_alive",
        side_effect=lambda pid: next(alive_states),
    ):
        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("src.cli.process_control.os.kill") as mock_kill:
                outcome = stop_server(path, timeout_sec=0.0, kill_timeout_sec=0.0)

    assert outcome.result is StopResult.STOPPED
    assert "force kill" in outcome.message
    assert mock_kill.call_count == 2
    assert not path.exists()


def test_stop_server_handles_pid_disappearing_before_sigterm(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    def raise_lookup(pid, sig):
        raise ProcessLookupError

    with patch("src.cli.process_control.is_process_alive", return_value=True):
        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("src.cli.process_control.os.kill", side_effect=raise_lookup):
                outcome = stop_server(path)

    assert outcome.result is StopResult.STALE_PID
    assert "stale PID file" in outcome.message
    assert not path.exists()


def test_stop_server_permission_denied_on_sigterm(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("src.cli.process_control.is_process_alive", return_value=True):
        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("src.cli.process_control.os.kill", side_effect=PermissionError):
                with pytest.raises(ProcessControlError, match="Permission denied"):
                    stop_server(path)


def test_stop_server_permission_denied_on_sigkill(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    def kill_side_effect(pid, sig):
        if sig == 15:
            return None
        raise PermissionError

    with patch("src.cli.process_control.is_process_alive", return_value=True):
        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("src.cli.process_control.os.kill", side_effect=kill_side_effect):
                with pytest.raises(ProcessControlError, match="Permission denied"):
                    stop_server(path, timeout_sec=0.0, kill_timeout_sec=0.0)
