"""`serve` — start the web app, poll /health, terminate.

Real Popen against the user's config.yaml (port + password come from there).
We:
1. Spawn `serve --no-worker` (no need to also boot the worker for this smoke).
2. Poll http://localhost:<port>/health until 200 (or timeout).
3. Send SIGTERM and verify the process exits cleanly.
"""
import pytest

from tests.cli_real_tg_integration.conftest import (
    read_pid_file,
    skip_if_server_pid_exists,
    wait_for_http_200,
    wait_for_pid_file,
)

pytestmark = pytest.mark.real_tg_manual


@pytest.mark.timeout(60)
def test_proc_serve_health_endpoint(run_cli_popen, cli_real_cli_env):
    import subprocess

    skip_if_server_pid_exists(cli_real_cli_env)
    port = cli_real_cli_env.web_port
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

    healthy = wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0)
    pid_still_matches = read_pid_file(cli_real_cli_env.pid_path) == proc.pid
    exited_early = proc.poll() is not None

    # communicate() = drain stderr + wait in one shot; safe to do here because
    # we no longer need the process. run_cli_popen's teardown is a no-op then
    # (poll() != None).
    proc.terminate()
    try:
        _, stderr_text = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr_text = proc.communicate(timeout=5)

    if not healthy:
        pytest.fail(
            f"`serve` did not serve /health within 20s on port {port}; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )
    if exited_early:
        pytest.fail(
            f"`serve` exited before test teardown; stderr tail: {(stderr_text or '')[-500:]!r}"
        )
    if not pid_still_matches:
        pytest.fail("`serve` health check was not backed by the PID registered by this test")
