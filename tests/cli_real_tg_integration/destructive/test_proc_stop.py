"""`stop` — terminate a running serve process via the PID file.

1. Spawn `serve --no-worker` and wait for /health.
2. Spawn `python -m src.main stop`.
3. Assert that the Popen handle eventually reports a poll() != None.
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


@pytest.mark.timeout(180)
def test_proc_stop_terminates_serve(run_cli_popen, cli_real_cli_env):
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
        pytest.fail("`serve` registered its PID but /health never became ready")
    if proc.poll() is not None:
        pytest.fail("`serve` exited before `stop`; /health may belong to another process")
    if read_pid_file(cli_real_cli_env.pid_path) != proc.pid:
        pytest.fail("`serve` health check was not backed by the PID registered by this test")

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
        pytest.fail("`stop` did not actually terminate the serve process")

    try:
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        stop_proc.kill()
        stop_stdout, stop_stderr = stop_proc.communicate(timeout=5)
        pytest.fail(
            "`stop` did not return after the serve process exited; "
            f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
        )

    assert stop_proc.returncode == 0, (
        f"`stop` exited {stop_proc.returncode}: "
        f"stdout={stop_stdout!r} stderr={stop_stderr!r}"
    )
