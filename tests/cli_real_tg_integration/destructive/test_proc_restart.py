"""`restart` — stop the running serve and start a fresh one.

1. Spawn `serve --no-worker`, wait for /health 200.
2. Spawn `python -m src.main restart`. It internally invokes `stop` +
   `serve` again, then blocks as the replacement server process.
3. Tear down the new server through a final `stop`.
"""
import subprocess

import pytest
import yaml

from tests.cli_real_tg_integration.conftest import (
    read_pid_file,
    skip_if_server_pid_exists,
    wait_for_http_200,
    wait_for_pid_file,
)

pytestmark = pytest.mark.real_tg_manual


def _read_port(cli_real_cli_env) -> int:
    cfg = yaml.safe_load(cli_real_cli_env.config_path.read_text(encoding="utf-8")) or {}
    return int((cfg.get("web") or {}).get("port", 8080))


@pytest.mark.timeout(360)
def test_proc_restart_brings_serve_back(run_cli_popen, cli_real_cli_env):
    skip_if_server_pid_exists(cli_real_cli_env)
    port = _read_port(cli_real_cli_env)
    proc = run_cli_popen("serve", "--no-worker")
    if not wait_for_pid_file(cli_real_cli_env.pid_path, proc.pid, timeout=10.0):
        proc.terminate()
        try:
            _, stderr_text = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, stderr_text = proc.communicate(timeout=5)
        pytest.fail(
            f"`serve` did not register PID {proc.pid} in {cli_real_cli_env.pid_path}; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )
    if not wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0):
        proc.terminate()
        pytest.fail("`serve` registered its PID but /health never became ready before restart")
    if proc.poll() is not None:
        pytest.fail("`serve` exited before `restart`; /health may belong to another process")
    if read_pid_file(cli_real_cli_env.pid_path) != proc.pid:
        pytest.fail("pre-restart /health was not backed by the PID registered by this test")

    restart_proc = run_cli_popen("restart")

    # After restart, /health should respond again. The new server PID is
    # the restart subprocess itself; verify the PID changed to that process so
    # we do not accidentally pass against an unrelated server on the same port.
    try:
        proc.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate(timeout=5)
        pytest.fail("`restart` did not stop the original serve process")

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

    health_back = wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0)
    if restart_proc.poll() is not None:
        pytest.fail("`restart` exited before final `stop`; /health may belong to another process")
    if read_pid_file(cli_real_cli_env.pid_path) != restart_proc.pid:
        pytest.fail("post-restart /health was not backed by the PID registered by this test")
    stop_proc = run_cli_popen("stop", capture_stdout=True)
    try:
        _, restart_stderr = restart_proc.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        restart_proc.kill()
        _, restart_stderr = restart_proc.communicate(timeout=5)
        pytest.fail(
            f"restarted server did not exit after final `stop`: {restart_stderr[-500:]!r}"
        )
    try:
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        stop_proc.kill()
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=5)
        pytest.fail(
            "final `stop` did not return after the restarted server exited; "
            f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
        )
    assert stop_proc.returncode == 0, (
        f"final `stop` failed and the restarted server is leaked. "
        f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
    )
    assert health_back, "`restart` did not bring /health back online within 20s"
