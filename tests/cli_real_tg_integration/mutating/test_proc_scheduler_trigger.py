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
        # writing anything — clear-pending in that case is a no-op. A failure
        # here means pending collection_tasks rows stay in the user's real DB,
        # which is exactly the silent-leak pattern fixed in test_proc_restart.
        cleanup = run_cli("scheduler", "clear-pending")
        assert cleanup.returncode == 0, (
            f"cleanup `scheduler clear-pending` failed; pending collection "
            f"tasks may be leaked: stderr={cleanup.stderr!r}"
        )
