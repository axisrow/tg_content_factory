"""`scheduler start` — long-running scheduler daemon, Popen + polling.

Spawns the scheduler via Popen, waits up to 20s for a `scheduler_status`
snapshot row to appear in runtime_snapshots (the worker publishes one
periodically while alive), then terminates via SIGTERM. The terminate is
the explicit shutdown signal — there is no separate `scheduler stop` CLI
once `scheduler start` is running interactively.

Categorized as mutating: while the scheduler doesn't delete user data,
it republishes runtime_snapshots rows and can enqueue collection_tasks
if jobs are due. Both are reversible/transient state.
"""
import pytest

from tests.cli_real_tg_integration.conftest import sqlite_utc_now, wait_for_db_row

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(60)
def test_proc_scheduler_start_publishes_status(run_cli_popen, cli_real_cli_env):
    import subprocess

    started_at = sqlite_utc_now()
    proc = run_cli_popen("scheduler", "start")

    row = wait_for_db_row(
        cli_real_cli_env.db_path,
        """
        SELECT updated_at
        FROM runtime_snapshots
        WHERE snapshot_type = ?
          AND datetime(updated_at) >= datetime(?)
        LIMIT 1
        """,
        ("scheduler_status", started_at),
        timeout=20.0,
    )

    # communicate() drains stderr + waits in one shot — no risk of pipe stall.
    proc.terminate()
    try:
        _, stderr_text = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr_text = proc.communicate(timeout=5)

    if proc.returncode not in (0, -15, 143):
        # -15 / 143 = SIGTERM. 0 = clean exit on Ctrl+C handling.
        pytest.fail(
            f"`scheduler start` exited with {proc.returncode}; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )

    if row is None:
        pytest.fail("fresh scheduler_status snapshot never appeared")
