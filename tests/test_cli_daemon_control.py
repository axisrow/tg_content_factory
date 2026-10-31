"""Fake-PID-file daemon lifecycle scenarios (#1698, #1714).

No live Telegram and no real daemons: every scenario builds throwaway
project/store directories in ``tmp_path``, registers fake PID files (PID 123
with a real ``owner.json`` record shaped exactly like the one
``register_current_process`` writes) and mocks the process probes plus
``os.kill``. The production Typer app is driven through ``CliRunner``.
"""

from __future__ import annotations

import json
import signal
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tg_agent.cli.typer_app import app

runner = CliRunner()

FAKE_PID = 123
FAKE_IDENTITY = "fake-start-identity-123"


def _project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated project directory: own config.yaml, cwd."""
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.chdir(project)
    (project / "config.yaml").write_text(
        "database:\n  path: data/tg_search.db\n", encoding="utf-8"
    )
    return project


def _legacy_store(tmp_path: Path, project: Path) -> Path:
    """Legacy plain-path marker (the pre-#1724 world) pointing at its own store."""
    legacy_store = tmp_path / "legacy-store"
    legacy_store.mkdir()
    (project / ".tg-agent-global").write_text(str(legacy_store), encoding="utf-8")
    return legacy_store


def _write_fake_registration(pid_path: Path, *, project_root: Path | None = None) -> Path:
    """PID file + owner.json exactly as register_current_process writes them."""
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(f"{FAKE_PID}\n", encoding="utf-8")
    executable = str(Path(sys.executable).resolve())
    owner = {
        "pid": FAKE_PID,
        "role": "worker",
        "project_root": str((project_root or Path.cwd()).resolve()),
        "store_root": str(pid_path.resolve().parent),
        "executable": executable,
        "executable_observed": executable,
        "started_at": "2026-10-30T00:00:00+00:00",
        "start_identity": FAKE_IDENTITY,
    }
    owner_path = pid_path.with_name(pid_path.name + ".owner.json")
    owner_path.write_text(json.dumps(owner), encoding="utf-8")
    return owner_path


@pytest.fixture
def fake_daemon(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
    """PID 123 is a live managed daemon; every signal is recorded, never sent."""
    from tg_agent.cli import process_control as control

    kills: list[tuple[int, int]] = []
    probes = {"n": 0}

    def alive(pid: int) -> bool:
        # First probe (liveness gate): up. The post-SIGTERM probe: exited.
        probes["n"] += 1
        return probes["n"] == 1

    monkeypatch.setattr(control, "is_process_alive", alive)
    monkeypatch.setattr(control, "is_expected_server_process", lambda pid: pid == FAKE_PID)
    monkeypatch.setattr(control, "process_start_identity", lambda pid: FAKE_IDENTITY)
    monkeypatch.setattr(
        control, "process_executable", lambda pid: str(Path(sys.executable).resolve())
    )
    monkeypatch.setattr(control.os, "kill", lambda pid, sig: kills.append((pid, sig)))
    return kills


# --------------------------------------------------------------------------- #
# #1698 — stop/restart reach a daemon registered at a previous store path
# --------------------------------------------------------------------------- #


def test_stop_terminates_daemon_registered_at_legacy_store(
    tmp_path, monkeypatch, fake_daemon
):
    """#1698 AC2: the pre-marker daemon's PID file moved with an old store;
    `stop` must terminate it instead of reporting "not running"."""
    project = _project(tmp_path, monkeypatch)
    legacy_store = _legacy_store(tmp_path, project)
    legacy_pid = _write_fake_registration(legacy_store / "tg_search.pid")

    result = runner.invoke(app, ["stop"])

    assert result.exit_code == 0, result.output
    assert fake_daemon == [(FAKE_PID, signal.SIGTERM)]
    assert not legacy_pid.exists()


def test_stop_without_daemon_names_every_checked_pid_file(
    tmp_path, monkeypatch, fake_daemon
):
    """A clean down state stays exit 0 and shows which PID files were probed."""
    project = _project(tmp_path, monkeypatch)
    legacy_store = _legacy_store(tmp_path, project)

    result = runner.invoke(app, ["stop"])

    assert result.exit_code == 0, result.output
    assert fake_daemon == []
    assert str(legacy_store / "tg_search.pid") in result.output
    assert "data/tg_search.pid" in result.output


def test_restart_stops_previous_store_daemon_before_starting(
    tmp_path, monkeypatch, fake_daemon
):
    """#1698 AC1: the documented enable → restart flow stops the orphaned
    daemon first, so exactly one worker starts (no SessionLockError cascade)."""
    project = _project(tmp_path, monkeypatch)
    legacy_store = _legacy_store(tmp_path, project)
    legacy_pid = _write_fake_registration(legacy_store / "tg_search.pid")
    started: list[object] = []
    monkeypatch.setattr(
        "tg_agent.cli.commands.worker.run_worker", lambda cfg: started.append(cfg)
    )

    result = runner.invoke(app, ["restart"])

    assert result.exit_code == 0, result.output
    assert fake_daemon == [(FAKE_PID, signal.SIGTERM)]
    assert not legacy_pid.exists()
    assert started, "restart must start the replacement worker"


# --------------------------------------------------------------------------- #
# #1714 — provenance: a foreign caller is refused, --force is the operator
# escape hatch, status is read-only, a failed restart start phase is loud.
# --------------------------------------------------------------------------- #


def test_stop_from_foreign_project_refuses_and_daemon_survives(
    tmp_path, monkeypatch, fake_daemon
):
    """#1714 AC1: another directory's stop/restart must not kill the daemon;
    the refusal names the owning project (provenance output)."""
    project = _project(tmp_path, monkeypatch)
    owner_project = tmp_path / "owner-project"
    owner_project.mkdir()
    pid_path = _write_fake_registration(
        project / "data" / "tg_search.pid", project_root=owner_project
    )

    result = runner.invoke(app, ["stop"])

    assert result.exit_code == 1
    assert str(owner_project) in result.output
    assert fake_daemon == [], "no signal may reach a foreign-owned daemon"
    assert pid_path.exists()


def test_stop_force_overrides_provenance_refusal(tmp_path, monkeypatch, fake_daemon):
    """#1714 proposal 1: an explicit operator --force overrides the refusal."""
    project = _project(tmp_path, monkeypatch)
    owner_project = tmp_path / "owner-project"
    owner_project.mkdir()
    pid_path = _write_fake_registration(
        project / "data" / "tg_search.pid", project_root=owner_project
    )

    result = runner.invoke(app, ["stop", "--force"])

    assert result.exit_code == 0, result.output
    assert fake_daemon == [(FAKE_PID, signal.SIGTERM)]
    assert not pid_path.exists()


def test_daemon_status_reports_live_daemon_with_provenance(
    tmp_path, monkeypatch, fake_daemon
):
    """#1714 AC2: status tells agents who owns the running daemon, without
    touching it (no signal, exit 0)."""
    project = _project(tmp_path, monkeypatch)
    legacy_store = _legacy_store(tmp_path, project)
    _write_fake_registration(legacy_store / "tg_search.pid")

    result = runner.invoke(app, ["daemon", "status"])

    assert result.exit_code == 0, result.output
    assert "daemon: up" in result.output
    assert str(FAKE_PID) in result.output
    assert str(project) in result.output
    assert str(legacy_store) in result.output
    assert "daemon: down" not in result.output


def test_daemon_status_down_is_exit_zero(tmp_path, monkeypatch, fake_daemon):
    project = _project(tmp_path, monkeypatch)
    _legacy_store(tmp_path, project)

    result = runner.invoke(app, ["daemon", "status"])

    assert result.exit_code == 0, result.output
    assert "daemon: down" in result.output
    assert "data/tg_search.pid" in result.output
    assert str(tmp_path / "legacy-store" / "tg_search.pid") in result.output


def test_restart_failing_start_phase_is_loud(tmp_path, monkeypatch, fake_daemon):
    """#1714 AC3: a restart whose start phase fails must never end silently
    in a zero-daemon state."""
    _project(tmp_path, monkeypatch)

    def failing_start(config_path: str) -> None:
        raise SystemExit(1)

    monkeypatch.setattr(
        "tg_agent.cli.commands.worker.serve_worker", failing_start
    )

    result = runner.invoke(app, ["restart"])

    assert result.exit_code == 1
    assert "no daemon is running" in result.output.lower()
    assert "daemon status" in result.output


def test_worker_start_refuses_while_project_daemon_lives_on_previous_store(
    tmp_path, monkeypatch, fake_daemon, caplog
):
    """#1698 AC3: a bare worker start must not double-spawn next to a live
    daemon still registered on a previous store path (the degraded
    connected_count=0 outcome)."""
    project = _project(tmp_path, monkeypatch)
    legacy_store = _legacy_store(tmp_path, project)
    legacy_pid = legacy_store / "tg_search.pid"
    _write_fake_registration(legacy_pid)
    started: list[object] = []
    monkeypatch.setattr(
        "tg_agent.cli.commands.worker.run_worker", lambda cfg: started.append(cfg)
    )

    with caplog.at_level("ERROR"):
        result = runner.invoke(app, ["worker"])

    assert result.exit_code == 1
    assert started == [], "a second daemon must not start"
    assert not (project / "data" / "tg_search.pid").exists()
    assert "Refusing to start" in caplog.text
    assert str(legacy_pid) in caplog.text
