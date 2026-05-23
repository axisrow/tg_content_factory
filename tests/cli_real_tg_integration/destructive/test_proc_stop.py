"""`stop` — terminate a running serve process via the PID file.

1. Spawn `serve --no-worker` and wait for /health.
2. Run `python -m src.main stop` synchronously.
3. Assert that the Popen handle eventually reports a poll() != None.
"""
import time

import pytest
import yaml

from tests.cli_real_tg_integration.conftest import wait_for_http_200

pytestmark = pytest.mark.real_tg_safe


def _read_port(cli_real_cli_env) -> int:
    cfg = yaml.safe_load(cli_real_cli_env.config_path.read_text(encoding="utf-8")) or {}
    return int((cfg.get("web") or {}).get("port", 8080))


def test_proc_stop_terminates_serve(run_cli_popen, run_cli, cli_real_cli_env):
    port = _read_port(cli_real_cli_env)
    proc = run_cli_popen("serve", "--no-worker")

    if not wait_for_http_200(f"http://127.0.0.1:{port}/health", timeout=20.0):
        proc.terminate()
        pytest.skip("serve never started — port collision or config issue")

    stop_result = run_cli("stop", timeout=30)
    assert stop_result.returncode == 0, (
        f"`stop` exited {stop_result.returncode}: "
        f"stdout={stop_result.stdout!r} stderr={stop_result.stderr!r}"
    )

    # Wait up to 10s for the serve Popen to actually exit.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        time.sleep(0.3)
    if proc.poll() is None:
        # `stop` returned 0 but serve is still alive. Drain stderr via
        # communicate() (not wait()) so a log-noisy server doesn't stall
        # on pipe backpressure mid-SIGTERM — same fix as in run_cli_popen.
        import subprocess

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
