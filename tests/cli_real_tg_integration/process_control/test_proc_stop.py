"""`stop` — terminate the running worker daemon via the PID file.

1. Spawn `worker` and wait until it registers its PID.
2. Spawn `python -m tg_agent.main stop`.
3. Assert that the worker exits and `stop` returns 0.
"""
import subprocess

import pytest

from tests.cli_real_tg_integration.conftest import (
    read_pid_file,
    skip_if_server_pid_exists,
    wait_for_pid_file,
)

pytestmark = pytest.mark.real_tg_manual


@pytest.mark.timeout(180)
def test_proc_stop_terminates_the_worker(run_cli_popen, cli_real_cli_env):
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
        pytest.fail("`worker` exited before `stop`")
    if read_pid_file(cli_real_cli_env.pid_path) != proc.pid:
        pytest.fail("the PID file is not owned by the worker this test started")

    stop_proc = run_cli_popen("stop", capture_stdout=True)
    try:
        proc.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        pytest.fail("`stop` did not actually terminate the worker")

    try:
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        stop_proc.kill()
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=5)
        pytest.fail(
            "`stop` did not return after the worker exited; "
            f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
        )

    assert stop_proc.returncode == 0, (
        f"`stop` exited {stop_proc.returncode}: "
        f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
    )
