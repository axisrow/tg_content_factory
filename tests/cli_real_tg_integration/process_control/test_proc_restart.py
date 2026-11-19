"""`restart` — stop the managed daemon and become its replacement worker.

1. Spawn `worker` and wait until it registers its PID.
2. Spawn `python -m tg_agent.main restart`. It stops the worker and blocks as
   the replacement daemon; its cmdline stays `tg_agent.main restart`.
3. Verify the replacement is recognized as managed (pid file + cmdline) and
   tear it down through a final `stop`.
"""
import subprocess

import pytest

from tests.cli_real_tg_integration.conftest import (
    read_pid_file,
    skip_if_server_pid_exists,
    wait_for_pid_file,
)

pytestmark = pytest.mark.real_tg_manual


def _process_command(pid: int) -> str:
    result = subprocess.run(
        ["ps", "-o", "command=", "-p", str(pid)],
        capture_output=True,
        check=False,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


@pytest.mark.timeout(360)
def test_proc_restart_replaces_the_worker(run_cli_popen, cli_real_cli_env):
    skip_if_server_pid_exists(cli_real_cli_env)
    proc = run_cli_popen("worker")
    if not wait_for_pid_file(cli_real_cli_env.pid_path, proc.pid, timeout=10.0):
        proc.terminate()
        try:
            _, stderr_text = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr_text = proc.communicate(timeout=5)
        pytest.fail(
            f"`worker` did not register PID {proc.pid} in {cli_real_cli_env.pid_path}; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )
    if proc.poll() is not None:
        pytest.fail("`worker` exited before `restart`")
    if read_pid_file(cli_real_cli_env.pid_path) != proc.pid:
        pytest.fail("the pre-restart PID file is not owned by the worker this test started")

    restart_proc = run_cli_popen("restart")

    # The restart subprocess becomes the replacement daemon: the old worker
    # must exit and the new PID is the restart process itself.
    try:
        proc.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate(timeout=5)
        pytest.fail("`restart` did not stop the original worker")

    if not wait_for_pid_file(cli_real_cli_env.pid_path, restart_proc.pid, timeout=30.0):
        restart_proc.terminate()
        try:
            _, stderr_text = restart_proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            restart_proc.kill()
            _, stderr_text = restart_proc.communicate(timeout=5)
        pytest.fail(
            f"`restart` did not register PID {restart_proc.pid} in {cli_real_cli_env.pid_path}; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )

    if restart_proc.poll() is not None:
        pytest.fail("`restart` exited before final `stop`; the PID may belong to another process")
    if read_pid_file(cli_real_cli_env.pid_path) != restart_proc.pid:
        pytest.fail("post-restart PID file is not owned by the restart subprocess")
    command = _process_command(restart_proc.pid)
    if "tg_agent.main" not in command or "restart" not in command.split():
        pytest.fail(
            f"replacement daemon is not a `tg_agent.main restart` worker process: {command!r}"
        )

    stop_proc = run_cli_popen("stop", capture_stdout=True)
    try:
        _, restart_stderr = restart_proc.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        restart_proc.kill()
        _, restart_stderr = restart_proc.communicate(timeout=5)
        pytest.fail(
            f"restarted daemon did not exit after final `stop`: {restart_stderr[-500:]!r}"
        )
    try:
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        stop_proc.kill()
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=5)
        pytest.fail(
            "final `stop` did not return after the restarted daemon exited; "
            f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
        )
    assert stop_proc.returncode == 0, (
        f"final `stop` failed and the restarted daemon is leaked. "
        f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
    )
