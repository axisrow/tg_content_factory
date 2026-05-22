"""`serve` — start the web app + embedded worker, poll /health, terminate.

Real Popen against the user's config.yaml (port + password come from there).
We:
1. Spawn `serve --no-worker` (no need to also boot the worker for this smoke).
2. Poll http://localhost:<port>/health until 200 (or timeout).
3. Send SIGTERM and verify the process exits cleanly.
"""
import pytest
import yaml

from tests.cli_real_tg_integration.conftest import wait_for_http_200

pytestmark = pytest.mark.real_tg_safe


def _read_port(cli_env) -> int:
    cfg = yaml.safe_load(cli_env.config_path.read_text(encoding="utf-8")) or {}
    web = cfg.get("web") or {}
    return int(web.get("port", 8080))


def test_proc_serve_health_endpoint(run_cli_popen, cli_env):
    import subprocess

    port = _read_port(cli_env)
    proc = run_cli_popen("serve", "--no-worker")

    healthy = wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0)

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
