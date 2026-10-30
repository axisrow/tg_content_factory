from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from tg_agent.cli.process_control import (
    ProcessControlError,
    StopResult,
    ensure_server_not_running,
    is_expected_server_process,
    pid_file_candidates,
    pid_file_path,
    register_current_process,
    stop_server,
    unregister_current_process,
)
from tg_agent.config import AppConfig

# --------------------------------------------------------------------------- #
# pid_file_candidates (#1698/#1714): every PID file this project's daemon
# may be registered at — the store it would use today plus every store a
# switch (global enable/migrate, legacy markers) moved it away from.
# --------------------------------------------------------------------------- #


def test_pid_file_candidates_without_marker_is_single_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  path: local.db\n", encoding="utf-8")

    assert pid_file_candidates("config.yaml") == [Path("local.pid")]


def test_pid_file_candidates_include_legacy_store_marker(tmp_path, monkeypatch):
    """A pre-#1724 daemon keeps its PID file in the legacy marker's store."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  path: local.db\n", encoding="utf-8")
    legacy_store = tmp_path / "legacy-store"
    (tmp_path / ".tg-agent-global").write_text(str(legacy_store), encoding="utf-8")

    assert pid_file_candidates("config.yaml") == [Path("local.pid"), legacy_store / "tg_search.pid"]


def test_pid_file_candidates_include_pre_marker_database_path(tmp_path, monkeypatch):
    """A binding with database_path redirects the current store; the config
    file's own database path stays a candidate for the pre-switch daemon."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  path: moved.db\n", encoding="utf-8")
    binding = {
        "version": 2,
        "project_id": "00000000-0000-0000-0000-000000000001",
        "store_root": str(tmp_path / "store"),
        "token_file": str(tmp_path / "token.json"),
        "database_path": "destination.db",
    }
    (tmp_path / ".tg-agent-global").write_text(json.dumps(binding), encoding="utf-8")

    assert pid_file_candidates("config.yaml") == [Path("destination.pid"), Path("moved.pid")]


def test_pid_file_candidates_deduplicate_equal_paths(tmp_path, monkeypatch):
    """A binding without database_path leaves the store unchanged: one candidate."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  path: local.db\n", encoding="utf-8")
    binding = {
        "version": 2,
        "project_id": "00000000-0000-0000-0000-000000000001",
        "store_root": str(tmp_path / "store"),
        "token_file": str(tmp_path / "token.json"),
    }
    (tmp_path / ".tg-agent-global").write_text(json.dumps(binding), encoding="utf-8")

    assert pid_file_candidates("config.yaml") == [Path("local.pid")]


@pytest.fixture(autouse=True)
def managed_mock_process_identity(monkeypatch):
    """PID 123 denotes a managed mock; real-process tests keep actual provenance."""
    from tg_agent.cli import process_control as control

    read_owner = control.read_process_owner
    start_identity = control.process_start_identity
    executable = control.process_executable
    binary = str(Path(sys.executable).resolve())

    def owner(path):
        recorded = read_owner(path)
        if recorded is not None or control.read_pid(path) != 123:
            return recorded
        return {"pid": 123, "role": "worker", "project_root": str(Path.cwd().resolve()),
                "store_root": str(path.resolve().parent), "executable": binary,
                "start_identity": "managed-test-process-123"}

    monkeypatch.setattr(control, "read_process_owner", owner)
    monkeypatch.setattr(control, "process_start_identity",
                        lambda pid: "managed-test-process-123" if pid == 123 else start_identity(pid))
    monkeypatch.setattr(control, "process_executable", lambda pid: binary if pid == 123 else executable(pid))


def test_pid_file_path_uses_database_path_suffix():
    config = AppConfig()
    config.database.path = "data/custom.db"
    assert pid_file_path(config) == Path("data/custom.pid")


@pytest.mark.parametrize("daemon", ["serve", "worker", "restart"])
def test_expected_server_process_matches_every_managed_daemon(daemon, monkeypatch):
    """serve / worker / restart all own the sessions and the PID file."""
    monkeypatch.setattr(
        "tg_agent.cli.process_control._process_command",
        lambda pid: f"/usr/bin/python -m tg_agent.main {daemon}",
    )
    assert is_expected_server_process(os.getpid()) is True


def test_expected_server_process_rejects_unrelated_command(monkeypatch):
    monkeypatch.setattr(
        "tg_agent.cli.process_control._process_command",
        lambda pid: "/usr/bin/python -m tg_agent.main dialogs list",
    )
    assert is_expected_server_process(os.getpid()) is False


def test_register_and_unregister_current_process(tmp_path):
    path = tmp_path / "app.pid"

    register_current_process(path)
    assert path.read_text(encoding="utf-8").strip() == str(os.getpid())

    unregister_current_process(path)
    assert not path.exists()


def test_ensure_server_not_running_raises_for_live_server(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
        with pytest.raises(ProcessControlError, match="Server already running"):
            ensure_server_not_running(path)


def test_ensure_server_not_running_removes_own_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text(f"{os.getpid()}\n", encoding="utf-8")

    ensure_server_not_running(path)

    assert not path.exists()


def test_ensure_server_not_running_cleans_stale_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=False):
        ensure_server_not_running(path)

    assert not path.exists()


def test_stop_server_without_pid_file(tmp_path):
    outcome = stop_server(tmp_path / "missing.pid")
    assert outcome.result is StopResult.NOT_RUNNING
    assert "not running" in outcome.message


def test_stop_server_cleans_stale_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=False):
        outcome = stop_server(path)

    assert outcome.result is StopResult.STALE_PID
    assert "stale PID file" in outcome.message
    assert not path.exists()


def test_stop_server_rejects_unmanaged_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=False):
            outcome = stop_server(path)

    assert outcome.result is StopResult.UNMANAGED
    assert "not a managed" in outcome.message
    assert path.exists()


def test_stop_server_stops_process_and_removes_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    alive_states = iter([True, False])

    with patch(
        "tg_agent.cli.process_control.is_process_alive",
        side_effect=lambda pid: next(alive_states),
    ):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.os.kill") as mock_kill:
                outcome = stop_server(path, timeout_sec=0.5)

    assert outcome.result is StopResult.STOPPED
    assert "Server stopped" in outcome.message
    mock_kill.assert_called_once()
    assert not path.exists()


def test_stop_server_timeout(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.os.kill"):
                outcome = stop_server(path, timeout_sec=0.0, kill_timeout_sec=0.0)

    assert outcome.result is StopResult.TIMEOUT
    assert "Timed out" in outcome.message
    assert path.exists()


def test_stop_server_force_kill_removes_pid(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    alive_states = iter([True, False])

    with patch(
        "tg_agent.cli.process_control.is_process_alive",
        side_effect=lambda pid: next(alive_states),
    ):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.os.kill") as mock_kill:
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

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.os.kill", side_effect=raise_lookup):
                outcome = stop_server(path)

    assert outcome.result is StopResult.STALE_PID
    assert "stale PID file" in outcome.message
    assert not path.exists()


def test_stop_server_permission_denied_on_sigterm(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.os.kill", side_effect=PermissionError):
                with pytest.raises(ProcessControlError, match="Permission denied"):
                    stop_server(path)


def test_stop_server_permission_denied_on_sigkill(tmp_path):
    path = tmp_path / "app.pid"
    path.write_text("123\n", encoding="utf-8")

    def kill_side_effect(pid, sig):
        if sig == 15:
            return None
        raise PermissionError

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.os.kill", side_effect=kill_side_effect):
                with pytest.raises(ProcessControlError, match="Permission denied"):
                    stop_server(path, timeout_sec=0.0, kill_timeout_sec=0.0)


# --------------------------------------------------------------------------- #
# stop_server(force=True) — the operator escape hatch (#1714): provenance
# verification is skipped, the managed-process check is not.
# --------------------------------------------------------------------------- #


def _write_owner_record(path: Path, *, project_root: Path) -> None:
    owner = {
        "pid": 123,
        "role": "worker",
        "project_root": str(project_root),
        "store_root": str(path.resolve().parent),
        "executable": str(Path(sys.executable).resolve()),
        "executable_observed": str(Path(sys.executable).resolve()),
        "start_identity": "foreign-start-identity",
    }
    path.with_name(path.name + ".owner.json").write_text(json.dumps(owner), encoding="utf-8")


def test_stop_server_force_signals_despite_foreign_provenance(tmp_path):
    """A daemon owned by another project is refused normally; --force signals it."""
    path = tmp_path / "legacy.pid"
    path.write_text("123\n", encoding="utf-8")
    _write_owner_record(path, project_root=tmp_path / "other-project")
    alive_states = iter([True, False])

    with patch("tg_agent.cli.process_control.is_process_alive",
               side_effect=lambda pid: next(alive_states)):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.process_start_identity",
                       return_value="foreign-start-identity"):
                with patch("tg_agent.cli.process_control.os.kill") as mock_kill:
                    outcome = stop_server(path, timeout_sec=0.5, force=True)

    assert outcome.result is StopResult.STOPPED
    mock_kill.assert_called_once()
    assert not path.exists()


def test_stop_server_without_force_refuses_foreign_provenance(tmp_path):
    path = tmp_path / "legacy.pid"
    path.write_text("123\n", encoding="utf-8")
    _write_owner_record(path, project_root=tmp_path / "other-project")

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.process_start_identity",
                       return_value="foreign-start-identity"):
                with patch("tg_agent.cli.process_control.os.kill") as mock_kill:
                    outcome = stop_server(path)

    assert outcome.result is StopResult.UNMANAGED
    assert "belongs to project" in outcome.message
    assert path.exists()
    mock_kill.assert_not_called()


def test_stop_server_force_still_refuses_unmanaged_process(tmp_path):
    """--force never signals a process that is not a managed tg-agent daemon."""
    path = tmp_path / "foreign.pid"
    path.write_text("123\n", encoding="utf-8")

    with patch("tg_agent.cli.process_control.is_process_alive", return_value=True):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=False):
            with patch("tg_agent.cli.process_control.os.kill") as mock_kill:
                outcome = stop_server(path, force=True)

    assert outcome.result is StopResult.UNMANAGED
    mock_kill.assert_not_called()
    assert path.exists()


def test_stop_server_force_kills_wedged_daemon_without_owner_record(tmp_path):
    """--force must reach SIGKILL for the wedged daemon it exists for.

    Regression guard (PR #1741 review): the post-timeout force-kill guard used
    to compare against the owner record — which --force deliberately never
    read — so SIGKILL was always refused with "changed during shutdown".
    """
    path = tmp_path / "wedged.pid"
    path.write_text("123\n", encoding="utf-8")
    alive_states = iter([True, True, False])

    with patch("tg_agent.cli.process_control.is_process_alive",
               side_effect=lambda pid: next(alive_states)):
        with patch("tg_agent.cli.process_control.is_expected_server_process", return_value=True):
            with patch("tg_agent.cli.process_control.process_start_identity",
                       return_value="wedged-start-identity"):
                with patch("tg_agent.cli.process_control.os.kill") as mock_kill:
                    outcome = stop_server(path, timeout_sec=0.05, kill_timeout_sec=0.05,
                                          force=True)

    assert outcome.result is StopResult.STOPPED
    assert "force kill" in outcome.message
    assert mock_kill.call_count == 2  # SIGTERM, then the force SIGKILL
    assert not path.exists()
