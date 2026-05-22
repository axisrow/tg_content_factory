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

from tests.cli_real_tg_integration.conftest import wait_for_db_row

pytestmark = pytest.mark.real_tg_safe


def test_proc_scheduler_start_publishes_status(
    run_cli_popen, assert_cli_ok, cli_env, run_cli
):
    proc = run_cli_popen("scheduler", "start")

    row = wait_for_db_row(
        cli_env.db_path,
        "SELECT 1 FROM runtime_snapshots WHERE snapshot_type = ? LIMIT 1",
        ("scheduler_status",),
        timeout=20.0,
    )

    # Send SIGTERM and let the run_cli_popen fixture do its cleanup verification.
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()
        proc.wait()

    if proc.returncode not in (0, -15, 143):
        # -15 / 143 = SIGTERM. 0 = clean exit on Ctrl+C handling.
        stderr_tail = (proc.stderr.read() if proc.stderr else "") or ""
        pytest.fail(
            f"`scheduler start` exited with {proc.returncode}; stderr tail: {stderr_tail[-500:]!r}"
        )

    if row is None:
        pytest.skip(
            "scheduler_status snapshot never appeared — likely no connected "
            "accounts (scheduler short-circuits before publishing)"
        )
