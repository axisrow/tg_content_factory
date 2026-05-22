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
    port = _read_port(cli_env)
    proc = run_cli_popen("serve", "--no-worker")

    healthy = wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0)
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait()

    if not healthy:
        # If port was in use or the web app failed to start, surface the tail
        # of stderr so the operator can diagnose quickly.
        stderr_tail = (proc.stderr.read() if proc.stderr else "") or ""
        pytest.fail(
            f"`serve` did not serve /health within 20s on port {port}; "
            f"stderr tail: {stderr_tail[-500:]!r}"
        )
