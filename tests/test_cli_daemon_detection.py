"""Regression coverage for managed module and console-script daemon invocations."""

from __future__ import annotations

import importlib.util
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tg_agent.cli import process_control, worker_handoff

DAEMON_COMMANDS = [
    f"{entry} {options}{command}{suffix}"
    for entry in (
        "python -m tg_agent.main",
        "/venv/bin/python3.11 /venv/bin/tg-agent",
        "/venv/bin/tg-agent",
        "python -u -m tg_agent.main",
        "python -uB -Wignore -Xutf8 -m tg_agent.main",
        "python -W ignore -X utf8 -m tg_agent.main",
        "python -Xdev -Wignore -uB /venv/bin/tg-agent",
    )
    for options in ("", "--config prod.yaml ", "--config=prod.yaml ", "--config 'my config.yaml' ")
    for command, suffix in (("worker", ""), ("restart", ""), ("serve", " --no-worker"))
]


class TestDaemonDetection(unittest.TestCase):
    def test_supported_invocations(self):
        for command in DAEMON_COMMANDS:
            with (
                self.subTest(command=command),
                patch.object(process_control, "_process_command", return_value=command),
                patch.object(process_control, "is_process_alive", return_value=True),
            ):
                self.assertTrue(process_control.is_expected_server_process(1234))

    def test_rejects_unrelated_and_incomplete_invocations(self):
        for command in (
            "python -m other_app worker",
            "python -m tg_agent.main stop",
            "python -m tg_agent.main --config worker",
            "tg-agent --config restart stop",
            "tg-agent dialogs send worker",
            "tg-agent --version worker",
            "tg-agent --unknown worker",
            "tg-agent --config",
            "python -c 'print(1)' -m tg_agent.main worker",
            "echo tg-agent worker",
            "python script.py -m tg_agent.main worker",
            "tg-agent-helper worker",
            "tg-agent 'unterminated",
        ):
            with (
                self.subTest(command=command),
                patch.object(process_control, "_process_command", return_value=command),
                patch.object(process_control, "is_process_alive", return_value=True),
            ):
                self.assertFalse(process_control.is_expected_server_process(1234))

    def test_linux_preserves_argument_boundaries(self):
        argv = ["/my venv/bin/python", "/my venv/bin/tg-agent", "--config", "worker config.yaml", "worker"]
        with (
            patch.object(process_control.sys, "platform", "linux"),
            patch.object(Path, "read_bytes", return_value=("\0".join(argv) + "\0").encode()),
        ):
            self.assertEqual(shlex.split(process_control._process_command(1234)), argv)

    def test_pid_guard_keeps_live_daemon_pid(self):
        for command in DAEMON_COMMANDS:
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "app.pid"
                pid_file.write_text("1234\n")
                with (
                    patch.object(process_control.os, "getpid", return_value=5678),
                    patch.object(process_control, "_process_command", return_value=command),
                    patch.object(process_control, "is_process_alive", return_value=True),
                    self.assertRaises(process_control.ProcessControlError),
                ):
                    process_control.register_current_process(pid_file)
                self.assertEqual(pid_file.read_text(), "1234\n")

    def test_stop_uses_real_detection(self):
        for command in DAEMON_COMMANDS:
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "app.pid"
                pid_file.write_text("1234\n")
                with (
                    patch.object(process_control, "_process_command", return_value=command),
                    patch.object(process_control, "is_process_alive", side_effect=[True, True, False]),
                    patch.object(process_control, "read_process_owner", return_value={
                        "pid": 1234, "role": "worker", "project_root": str(Path.cwd().resolve()),
                        "store_root": str(pid_file.resolve().parent), "executable": "/synthetic/python",
                        "start_identity": "synthetic-start", "command": command,
                    }),
                    patch.object(process_control, "process_executable", return_value="/synthetic/python"),
                    patch.object(process_control, "process_start_identity", return_value="synthetic-start"),
                    patch.object(os, "kill") as kill,
                ):
                    self.assertEqual(process_control.stop_server(pid_file).result, process_control.StopResult.STOPPED)
                kill.assert_called_once_with(1234, signal.SIGTERM)
                self.assertFalse(pid_file.exists())

    def test_handoff_uses_real_detection(self):
        # tests/conftest.py's autouse fixture replaces the public function.
        # Load the production module privately so this regression exercises it.
        spec = importlib.util.spec_from_file_location("daemon_handoff_fresh", worker_handoff.__file__)
        assert spec is not None and spec.loader is not None
        handoff = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handoff)
        for command in DAEMON_COMMANDS:
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "app.pid"
                pid_file.write_text("1234\n")
                with (
                    patch.object(handoff, "pid_file_path", return_value=pid_file),
                    patch.object(process_control, "_process_command", return_value=command),
                    patch.object(process_control, "is_process_alive", return_value=True),
                ):
                    self.assertEqual(handoff.serve_is_running(None), "--no-worker" not in command)

    def test_ps_preserves_standard_venv_entrypoints_with_spaces(self):
        for executable, arguments in (
            ("/my venv/bin/python3", "-uB -Wignore -m tg_agent.main --config prod.yaml worker"),
            ("/my venv/bin/python3", "/my venv/bin/tg-agent --config=prod.yaml restart"),
            ("/my venv/bin/tg-agent", "worker"),
        ):
            command = executable + " " + arguments
            with (
                self.subTest(command=command),
                patch.object(process_control.sys, "platform", "darwin"),
                patch.object(process_control.subprocess, "run", side_effect=[
                    Mock(returncode=0, stdout=command + "\n"),
                    Mock(returncode=0, stdout=executable + "\n"),
                ]),
                patch.object(process_control, "is_process_alive", return_value=True),
            ):
                self.assertTrue(process_control.is_expected_server_process(1234))

    def test_ps_does_not_treat_python_arguments_as_executable(self):
        with (
            patch.object(process_control.sys, "platform", "darwin"),
            patch.object(process_control.subprocess, "run", side_effect=[
                Mock(returncode=0, stdout="/usr/bin/echo /my venv/bin/python3 -m tg_agent.main worker"),
                Mock(returncode=0, stdout="/usr/bin/echo"),
            ]),
            patch.object(process_control, "is_process_alive", return_value=True),
        ):
            self.assertFalse(process_control.is_expected_server_process(1234))

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires real /proc argv")
    def test_live_python_option_forms(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "tg_agent"
            package.mkdir()
            (package / "__init__.py").write_text("")
            # No project runtime or Telegram connections: only a harmless process
            # with the production module argv shape and real Python flags.
            (package / "main.py").write_text(
                "from pathlib import Path; Path('ready').touch(); import time; time.sleep(60)"
            )
            ready = Path(directory) / "ready"
            for options in (("-Wignore",), ("-Xutf8",), ("-uB",)):
                with self.subTest(options=options):
                    ready.unlink(missing_ok=True)
                    process = subprocess.Popen(
                        [sys.executable, *options, "-m", "tg_agent.main", "--config", "prod.yaml", "worker"],
                        cwd=directory,
                        env={**os.environ, "PYTHONPATH": directory},
                    )
                    try:
                        deadline = time.monotonic() + 10
                        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                            time.sleep(0.01)
                        self.assertTrue(ready.exists(), "Test child failed to finish exec/startup")
                        self.assertTrue(process_control.is_expected_server_process(process.pid))
                    finally:
                        process.terminate()
                        process.wait(timeout=10)
