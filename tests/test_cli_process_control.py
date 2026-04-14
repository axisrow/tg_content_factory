"""Tests for src/cli/process_control.py — process management helpers."""

from __future__ import annotations

import os
import signal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.cli.process_control import (
    ProcessControlError,
    StopResult,
    ensure_server_not_running,
    is_expected_server_process,
    is_process_alive,
    pid_file_path,
    read_pid,
    register_current_process,
    remove_pid_file,
    stop_server,
    unregister_current_process,
)
from src.config import AppConfig

# ---------------------------------------------------------------------------
# is_process_alive
# ---------------------------------------------------------------------------


class TestIsProcessAlive:
    def test_own_process_is_alive(self):
        assert is_process_alive(os.getpid()) is True

    def test_nonexistent_pid(self):
        # Pick a PID that almost certainly does not exist
        assert is_process_alive(2999999) is False


# ---------------------------------------------------------------------------
# read_pid
# ---------------------------------------------------------------------------


class TestReadPid:
    def test_reads_valid_pid(self, tmp_path):
        p = tmp_path / "test.pid"
        p.write_text("12345\n")
        assert read_pid(p) == 12345

    def test_returns_none_for_missing_file(self, tmp_path):
        p = tmp_path / "missing.pid"
        assert read_pid(p) is None

    def test_returns_none_for_empty_file(self, tmp_path):
        p = tmp_path / "empty.pid"
        p.write_text("")
        assert read_pid(p) is None

    def test_raises_on_non_numeric(self, tmp_path):
        p = tmp_path / "bad.pid"
        p.write_text("not-a-number")
        with pytest.raises(ProcessControlError, match="Invalid PID file"):
            read_pid(p)


# ---------------------------------------------------------------------------
# pid_file_path
# ---------------------------------------------------------------------------


class TestPidFilePath:
    def test_swaps_extension(self):
        config = AppConfig()
        config.database.path = "/tmp/data/app.db"
        result = pid_file_path(config)
        assert result == Path("/tmp/data/app.pid")


# ---------------------------------------------------------------------------
# remove_pid_file
# ---------------------------------------------------------------------------


class TestRemovePidFile:
    def test_removes_existing(self, tmp_path):
        p = tmp_path / "x.pid"
        p.write_text("1\n")
        remove_pid_file(p)
        assert not p.exists()

    def test_no_error_if_missing(self, tmp_path):
        p = tmp_path / "missing.pid"
        remove_pid_file(p)  # should not raise


# ---------------------------------------------------------------------------
# is_expected_server_process
# ---------------------------------------------------------------------------


class TestIsExpectedServerProcess:
    def test_rejects_zero_pid(self):
        assert is_expected_server_process(0) is False

    def test_rejects_negative_pid(self):
        assert is_expected_server_process(-1) is False

    def test_rejects_dead_pid(self):
        assert is_expected_server_process(2999999) is False

    @patch("src.cli.process_control._process_command", return_value="python -m src.main serve")
    @patch("src.cli.process_control.is_process_alive", return_value=True)
    def test_accepts_matching_command(self, _mock_alive, _mock_cmd):
        assert is_expected_server_process(1234) is True

    @patch("src.cli.process_control._process_command", return_value="python -m other_app")
    @patch("src.cli.process_control.is_process_alive", return_value=True)
    def test_rejects_non_matching_command(self, _mock_alive, _mock_cmd):
        assert is_expected_server_process(1234) is False

    @patch("src.cli.process_control._process_command", return_value="")
    @patch("src.cli.process_control.is_process_alive", return_value=True)
    def test_rejects_empty_command(self, _mock_alive, _mock_cmd):
        assert is_expected_server_process(1234) is False


# ---------------------------------------------------------------------------
# _process_command (Linux-specific, tested via mock on non-Linux)
# ---------------------------------------------------------------------------


class TestProcessCommand:
    @patch("sys.platform", "linux")
    def test_linux_reads_proc_cmdline(self, tmp_path):
        from src.cli.process_control import _process_command

        fake_proc = tmp_path / "1234"
        fake_proc.mkdir()
        (fake_proc / "cmdline").write_bytes(b"python\x00-m\x00src.main\x00serve\x00")
        with patch.object(Path, "__truediv__", return_value=fake_proc / "cmdline"):
            # Directly test by patching the path construction
            pass
        # Test the actual function with a real /proc path mock
        with patch("src.cli.process_control.Path") as mock_path_cls:
            mock_path = MagicMock()
            mock_path_cls.return_value.__truediv__.return_value.__truediv__.return_value = mock_path
            mock_path.read_bytes.return_value = b"python\x00-m\x00src.main\x00serve\x00"
            result = _process_command(1234)
            assert "python" in result
            assert "src.main" in result

    @patch("sys.platform", "linux")
    def test_linux_proc_not_found(self):
        from src.cli.process_control import _process_command

        with patch("src.cli.process_control.Path") as mock_path_cls:
            mock_path = MagicMock()
            mock_path_cls.return_value.__truediv__.return_value.__truediv__.return_value = mock_path
            mock_path.read_bytes.side_effect = FileNotFoundError
            result = _process_command(1234)
            assert result == ""


# ---------------------------------------------------------------------------
# ensure_server_not_running
# ---------------------------------------------------------------------------


class TestEnsureServerNotRunning:
    def test_no_pid_file(self, tmp_path):
        p = tmp_path / "missing.pid"
        ensure_server_not_running(p)  # should not raise

    def test_own_pid_is_cleaned_up(self, tmp_path):
        p = tmp_path / "self.pid"
        p.write_text(f"{os.getpid()}\n")
        ensure_server_not_running(p)
        assert not p.exists()

    @patch("src.cli.process_control.is_expected_server_process", return_value=True)
    def test_raises_if_other_server_running(self, _mock, tmp_path):
        p = tmp_path / "other.pid"
        p.write_text("99999\n")
        with pytest.raises(ProcessControlError, match="Server already running"):
            ensure_server_not_running(p)

    @patch("src.cli.process_control.is_expected_server_process", return_value=False)
    def test_removes_stale_pid(self, _mock, tmp_path):
        p = tmp_path / "stale.pid"
        p.write_text("99999\n")
        ensure_server_not_running(p)
        assert not p.exists()


# ---------------------------------------------------------------------------
# register_current_process
# ---------------------------------------------------------------------------


class TestRegisterCurrentProcess:
    @patch("src.cli.process_control.ensure_server_not_running")
    def test_writes_own_pid(self, _mock_ensure, tmp_path):
        p = tmp_path / "reg.pid"
        register_current_process(p)
        assert p.read_text().strip() == str(os.getpid())

    @patch("src.cli.process_control.ensure_server_not_running")
    def test_creates_parent_dirs(self, _mock_ensure, tmp_path):
        p = tmp_path / "deep" / "nested" / "reg.pid"
        register_current_process(p)
        assert p.exists()


# ---------------------------------------------------------------------------
# unregister_current_process
# ---------------------------------------------------------------------------


class TestUnregisterCurrentProcess:
    def test_removes_own_pid(self, tmp_path):
        p = tmp_path / "unreg.pid"
        p.write_text(f"{os.getpid()}\n")
        unregister_current_process(p)
        assert not p.exists()

    def test_noop_if_different_pid(self, tmp_path):
        p = tmp_path / "other.pid"
        p.write_text("99999\n")
        unregister_current_process(p)
        assert p.exists()  # not our PID, leave it

    def test_noop_if_file_missing(self, tmp_path):
        p = tmp_path / "missing.pid"
        unregister_current_process(p)  # should not raise

    def test_no_error_on_read_failure(self, tmp_path):
        p = tmp_path / "unreg.pid"
        p.write_text(f"{os.getpid()}\n")
        with patch("src.cli.process_control.read_pid", side_effect=OSError("boom")):
            unregister_current_process(p)  # must not raise


# ---------------------------------------------------------------------------
# stop_server
# ---------------------------------------------------------------------------


class TestStopServer:
    def test_not_running_no_pid_file(self, tmp_path):
        p = tmp_path / "no.pid"
        outcome = stop_server(p)
        assert outcome.result == StopResult.NOT_RUNNING

    @patch("src.cli.process_control.is_process_alive", return_value=False)
    def test_stale_pid_file(self, _mock, tmp_path):
        p = tmp_path / "stale.pid"
        p.write_text("99999\n")
        outcome = stop_server(p)
        assert outcome.result == StopResult.STALE_PID
        assert not p.exists()

    @patch("src.cli.process_control.is_expected_server_process", return_value=False)
    @patch("src.cli.process_control.is_process_alive", return_value=True)
    def test_unmanaged_process(self, _mock_alive, _mock_expected, tmp_path):
        p = tmp_path / "unmanaged.pid"
        p.write_text("99999\n")
        outcome = stop_server(p)
        assert outcome.result == StopResult.UNMANAGED

    @patch("src.cli.process_control.is_expected_server_process", return_value=True)
    @patch("src.cli.process_control.is_process_alive", side_effect=[True, False])
    @patch("os.kill")
    def test_stops_gracefully(self, mock_kill, _mock_alive, _mock_expected, tmp_path):
        p = tmp_path / "grace.pid"
        p.write_text("99999\n")
        outcome = stop_server(p)
        assert outcome.result == StopResult.STOPPED
        mock_kill.assert_called_once_with(99999, signal.SIGTERM)
        assert not p.exists()

    @patch("src.cli.process_control.is_expected_server_process", return_value=True)
    @patch("src.cli.process_control.is_process_alive", side_effect=[True, True, True, False])
    @patch("os.kill")
    def test_force_kills_after_timeout(self, mock_kill, _mock_alive, _mock_expected, tmp_path):
        p = tmp_path / "force.pid"
        p.write_text("99998\n")
        outcome = stop_server(p, timeout_sec=0.05, kill_timeout_sec=0.05)
        assert outcome.result == StopResult.STOPPED
        # SIGTERM first, then SIGKILL
        assert mock_kill.call_count == 2

    @patch("src.cli.process_control.is_expected_server_process", return_value=True)
    @patch("src.cli.process_control.is_process_alive", return_value=True)
    @patch("os.kill")
    def test_timeout_if_never_dies(self, mock_kill, _mock_alive, _mock_expected, tmp_path):
        p = tmp_path / "hung.pid"
        p.write_text("99997\n")
        outcome = stop_server(p, timeout_sec=0.05, kill_timeout_sec=0.05)
        assert outcome.result == StopResult.TIMEOUT

    @patch("src.cli.process_control.is_expected_server_process", return_value=True)
    @patch("src.cli.process_control.is_process_alive", side_effect=[True, False])
    @patch("os.kill", side_effect=ProcessLookupError)
    def test_term_raises_process_lookup(self, mock_kill, _mock_alive, _mock_expected, tmp_path):
        p = tmp_path / "gone.pid"
        p.write_text("99996\n")
        outcome = stop_server(p)
        assert outcome.result == StopResult.STALE_PID
        assert not p.exists()

    @patch("src.cli.process_control.is_expected_server_process", return_value=True)
    @patch("src.cli.process_control.is_process_alive", return_value=True)
    @patch("os.kill", side_effect=PermissionError("nope"))
    def test_term_raises_permission_error(self, mock_kill, _mock_alive, _mock_expected, tmp_path):
        p = tmp_path / "perm.pid"
        p.write_text("99995\n")
        with pytest.raises(ProcessControlError, match="Permission denied"):
            stop_server(p)
