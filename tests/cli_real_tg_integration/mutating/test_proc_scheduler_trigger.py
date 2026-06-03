"""`scheduler trigger` — one-shot enqueue of all eligible channels.

Writes collection_tasks rows (pending) for every non-filtered channel. Cleanup
cancels ONLY the tasks this test created (diff of a before/after snapshot of
pending channel-collect task ids), never the global `scheduler clear-pending`
which would cancel every pending task in the queue — including ones another
process may have enqueued.
"""
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    cancel_collection_tasks,
    snapshot_pending_collection_task_ids,
)

pytestmark = pytest.mark.real_tg_safe


def test_proc_scheduler_trigger_enqueues(run_cli, assert_cli_ok, cli_real_cli_env):
    leak_msg: str | None = None
    before_ids, before_ok = snapshot_pending_collection_task_ids(cli_real_cli_env.db_path)
    try:
        result = run_cli("scheduler", "trigger")
        assert_cli_ok(result, allow_error_text=("No connected accounts",))
        combined = result.stdout + result.stderr
        # The handler prints "Enqueued N channels ..." or "No connected accounts."
        # Either is a legitimate outcome; we only require the command to exit 0
        # and emit one of those known phrases.
        assert (
            "Enqueued" in combined
            or "No connected accounts" in combined
            or "no channels" in combined.lower()
        ), f"unexpected `scheduler trigger` output: {combined!r}"
    finally:
        # Cancel only the tasks created by this trigger run, by id. The suite runs
        # sequentially (shared session strings) and these live CLI tests do not run
        # a worker, so within the test window `scheduler trigger` is the only
        # enqueuer — a before/after id diff is the set this run created.
        # If either snapshot read failed we do NOT fall back to an empty baseline
        # (that would diff to "all pending" and over-cancel); we skip and leak.
        after_ids, after_ok = snapshot_pending_collection_task_ids(cli_real_cli_env.db_path)
        if not (before_ok and after_ok):
            if sys.exc_info()[0] is None:
                leak_msg = (
                    "could not snapshot pending collection_tasks reliably; skipping cleanup to "
                    "avoid over-cancelling tasks this test did not create"
                )
        else:
            new_ids = after_ids - before_ids
            if new_ids:
                cancel_leak = cancel_collection_tasks(cli_real_cli_env, new_ids)
                if cancel_leak is not None:
                    leak_msg = (
                        f"cleanup could not cancel test-created collection tasks "
                        f"{sorted(new_ids)}: {cancel_leak}"
                    )

        # Only raise on cleanup if the try block didn't already fail.
        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
