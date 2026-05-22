"""`restart` — stop the running serve and start a fresh one.

1. Spawn `serve --no-worker`, wait for /health 200.
2. Run `python -m src.main restart` synchronously. It internally invokes
   `stop` + `serve` again; the new serve is detached, but /health should
   still respond to a fresh request.
3. Tear down the new server through a final `stop`.
"""
import subprocess

import pytest
import yaml

from tests.cli_real_tg_integration.conftest import cli_run_direct, wait_for_http_200

pytestmark = pytest.mark.real_tg_safe


def _read_port(cli_env) -> int:
    cfg = yaml.safe_load(cli_env.config_path.read_text(encoding="utf-8")) or {}
    return int((cfg.get("web") or {}).get("port", 8080))


def test_proc_restart_brings_serve_back(run_cli_popen, run_cli, cli_env):
    port = _read_port(cli_env)
    proc = run_cli_popen("serve", "--no-worker")
    if not wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0):
        proc.terminate()
        pytest.skip("serve never started before restart — environment issue")

    restart_result = run_cli("restart", timeout=60)
    assert restart_result.returncode == 0, (
        f"`restart` exited {restart_result.returncode}: "
        f"stdout={restart_result.stdout!r} stderr={restart_result.stderr!r}"
    )

    # After restart, /health should respond again. The new server PID is
    # detached and not tracked by run_cli_popen, so we MUST verify the final
    # `stop` actually terminated it — otherwise the server keeps running on
    # the operator's machine indefinitely (run_cli_popen's teardown can only
    # reach the original `proc` we spawned, which is already dead).
    #
    # `stop` is invoked via cli_run_direct (not run_cli) so a timeout here
    # surfaces as pytest.fail, not pytest.skip — a skipped test would hide
    # the fact that the restarted server is still running.
    health_back = wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0)
    try:
        stop_result = cli_run_direct(cli_env, "stop", timeout=30)
    except subprocess.TimeoutExpired:
        pytest.fail(
            "final `stop` timed out; the restarted server is leaked — "
            "kill it manually."
        )
    assert stop_result.returncode == 0, (
        f"final `stop` failed and the restarted server is leaked. "
        f"stdout={stop_result.stdout!r} stderr={stop_result.stderr!r}"
    )
    assert health_back, "`restart` did not bring /health back online within 20s"
