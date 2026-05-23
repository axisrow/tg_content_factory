"""`worker` — standalone worker, polls for worker_heartbeat snapshot.

The worker publishes a `worker_heartbeat` row in runtime_snapshots on
startup and at a fixed interval afterwards (see src/runtime/worker.py:36).
We poll for that row, then SIGTERM the process.
"""
import pytest

from tests.cli_real_tg_integration.conftest import wait_for_db_row

pytestmark = pytest.mark.real_tg_safe


def test_proc_worker_publishes_heartbeat(run_cli_popen, cli_real_cli_env):
    import subprocess

    proc = run_cli_popen("worker")

    row = wait_for_db_row(
        cli_real_cli_env.db_path,
        "SELECT 1 FROM runtime_snapshots WHERE snapshot_type = ? LIMIT 1",
        ("worker_heartbeat",),
        timeout=20.0,
    )

    proc.terminate()
    try:
        _, stderr_text = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr_text = proc.communicate(timeout=5)

    if row is None:
        pytest.fail(
            "`worker` did not publish worker_heartbeat within 20s; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )
