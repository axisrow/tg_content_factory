"""A project must not stop another project's daemon (#1714)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tg_agent.cli import process_control as control


def test_foreign_project_cannot_stop_registered_process(tmp_path, monkeypatch):
    owner = tmp_path / "owner"
    caller = tmp_path / "caller"
    owner.mkdir()
    caller.mkdir()
    monkeypatch.chdir(owner)
    path = tmp_path / "shared.pid"
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        monkeypatch.setattr(control.os, "getpid", lambda: child.pid)
        control.register_current_process(path)
        monkeypatch.setattr(control, "is_expected_server_process", lambda pid: pid == child.pid)
        monkeypatch.setattr(control, "is_process_alive", lambda pid: child.poll() is None)
        monkeypatch.chdir(caller)
        outcome = control.stop_server(path, timeout_sec=1, kill_timeout_sec=1)
        assert outcome.result == control.StopResult.UNMANAGED
        assert str(owner) in outcome.message
        assert child.poll() is None
        assert path.exists()
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_registration_records_owner_and_cleanup_removes_it(tmp_path):
    path = tmp_path / "worker.pid"
    control.register_current_process(path)
    owner = control.read_process_owner(path)
    assert owner["pid"] == os.getpid()
    assert owner["project_root"] == str(Path.cwd().resolve())
    assert owner["executable"] == str(Path(sys.executable).resolve())
    control.unregister_current_process(path)
    assert not control.owner_file_path(path).exists()


def test_legacy_numeric_pid_without_provenance_is_not_signalled(tmp_path, monkeypatch):
    path = tmp_path / "legacy.pid"
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        path.write_text(str(child.pid))
        monkeypatch.setattr(control, "is_expected_server_process", lambda pid: True)
        outcome = control.stop_server(path, timeout_sec=0.1)
        assert outcome.result is control.StopResult.UNMANAGED
        assert "provenance" in outcome.message
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_corrupt_owner_record_refuses_stop(tmp_path, monkeypatch):
    path = tmp_path / "worker.pid"
    control.register_current_process(path)
    control.owner_file_path(path).write_text("{broken")
    monkeypatch.setattr(control, "is_expected_server_process", lambda pid: True)
    with pytest.raises(control.ProcessControlError, match="owner"):
        control.stop_server(path)
    assert path.exists()


def test_owner_project_can_stop_registered_process(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "worker.pid"
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        with monkeypatch.context() as registration:
            registration.setattr(control.os, "getpid", lambda: child.pid)
            control.register_current_process(path)
        monkeypatch.setattr(control, "is_expected_server_process", lambda pid: pid == child.pid)
        monkeypatch.setattr(control, "is_process_alive", lambda pid: child.poll() is None)
        outcome = control.stop_server(path, timeout_sec=1)
        assert outcome.result == control.StopResult.STOPPED
        assert child.poll() is not None
        assert not path.exists()
        assert not control.owner_file_path(path).exists()
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_stop_accepts_the_process_own_ps_executable_view(tmp_path, monkeypatch):
    """macOS ps comm may show the Python.app binary; the recorded self-view must win."""
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "worker.pid"
    bundle_view = "/Library/Frameworks/Python.framework/Versions/3.13/Resources/Python.app/Contents/MacOS/Python"
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        with monkeypatch.context() as registration:
            registration.setattr(control.os, "getpid", lambda: child.pid)
            registration.setattr(control, "process_executable", lambda pid: bundle_view)
            control.register_current_process(path)
        assert control.read_process_owner(path)["executable_observed"] == bundle_view
        monkeypatch.setattr(control, "is_expected_server_process", lambda pid: pid == child.pid)
        monkeypatch.setattr(control, "is_process_alive", lambda pid: child.poll() is None)
        monkeypatch.setattr(control, "process_executable", lambda pid: bundle_view)
        outcome = control.stop_server(path, timeout_sec=1)
        assert outcome.result == control.StopResult.STOPPED
        assert child.poll() is not None
    finally:
        child.terminate()
        child.wait(timeout=5)
