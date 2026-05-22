"""`scheduler trigger` — one-shot enqueue of all eligible channels.

Writes collection_tasks rows (pending) for every non-filtered channel. This
is reversible via `scheduler clear-pending`, which the test runs in finally
to leave the queue as it was before.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


def test_proc_scheduler_trigger_enqueues(run_cli, assert_cli_ok):
    try:
        result = run_cli("scheduler", "trigger")
        assert_cli_ok(result)
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
        # Reverse the side effect: delete any pending tasks the trigger created.
        # If no accounts were connected the trigger printed and exited without
        # writing anything — clear-pending in that case is a no-op.
        cleanup = run_cli("scheduler", "clear-pending")
        if cleanup.returncode != 0:
            # Don't mask the primary assertion; surface via print.
            print(f"cleanup `scheduler clear-pending` failed: {cleanup.stderr!r}")
