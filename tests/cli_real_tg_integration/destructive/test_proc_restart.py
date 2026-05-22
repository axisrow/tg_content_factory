"""`restart` — stop the running serve and start a fresh one.

1. Spawn `serve --no-worker`, wait for /health 200.
2. Run `python -m src.main restart` synchronously. It internally invokes
   `stop` + `serve` again; the new serve is detached, but /health should
   still respond to a fresh request.
3. Tear down the new server through a final `stop`.
"""
import pytest
import yaml

from tests.cli_real_tg_integration.conftest import wait_for_http_200

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

    # After restart, /health should respond again (possibly from a new PID
    # that we no longer manage from this test — clean it up explicitly).
    try:
        if not wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0):
            pytest.fail("`restart` did not bring /health back online within 20s")
    finally:
        stop_result = run_cli("stop", timeout=30)
        if stop_result.returncode != 0:
            print(f"final `stop` cleanup failed: {stop_result.stderr!r}")
