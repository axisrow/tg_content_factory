"""`worker` — standalone worker, polls for worker_heartbeat snapshot.

The worker publishes a `worker_heartbeat` row in runtime_snapshots on
startup and at a fixed interval afterwards (see src/runtime/worker.py:36).
We poll for that row, then SIGTERM the process.
"""
import pytest

from tests.cli_real_tg_integration.conftest import wait_for_db_row

pytestmark = pytest.mark.real_tg_safe


def test_proc_worker_publishes_heartbeat(run_cli_popen, cli_env):
    proc = run_cli_popen("worker")

    row = wait_for_db_row(
        cli_env.db_path,
        "SELECT 1 FROM runtime_snapshots WHERE snapshot_type = ? LIMIT 1",
        ("worker_heartbeat",),
        timeout=20.0,
    )

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait()

    if row is None:
        stderr_tail = (proc.stderr.read() if proc.stderr else "") or ""
        pytest.fail(
            "`worker` did not publish worker_heartbeat within 20s; "
            f"stderr tail: {stderr_tail[-500:]!r}"
        )
