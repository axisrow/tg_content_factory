"""`python -m src.main collect` (no args) — enqueue all eligible channels.

The bare `collect` invocation (no `--channel-id`, no `sample` sub) calls
`TaskEnqueuer.enqueue_all_channels()` which writes pending rows into the
`collection_tasks` table for every non-filtered channel (see
src/cli/commands/collect.py:48-58). The actual TG-iter_messages work is
deferred until a worker drains the queue, so this test does NOT itself
fetch messages. Still placed in heavy/ because the queue write touches
every channel in the live DB.

Self-contained cleanup: every successful invocation is followed by
`scheduler clear-pending` in `finally` to drain the queue this test
created. The cleanup goes through `cli_run_direct` (not `run_cli`) so a
timeout in clear-pending raises TimeoutExpired (which we catch) instead
of pytest.skip() — that would replace any in-flight AssertionError from
the try block and silently leak the queued rows.
"""
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(360)
def test_collect_default_enqueues_all(run_cli, assert_cli_ok, cli_env):
    leak_msg: str | None = None
    try:
        result = run_cli("collect", timeout=300)
        assert_cli_ok(result, allow_error_text=("No connected accounts",))
        combined = result.stdout + result.stderr
        # Possible outcomes:
        # - "Enqueued N channels (skipped M, total K)..." — normal path.
        # - "No connected accounts..." — pool empty, the handler short-circuits
        #   before reaching the enqueue branch.
        assert (
            "Enqueued" in combined
            or "No connected accounts" in combined
        ), f"unexpected bare `collect` output: {combined!r}"
    finally:
        # Drain the queue this test created. Same self-contained pattern as
        # mutating/test_proc_scheduler_trigger.py:test_proc_scheduler_trigger_enqueues.
        try:
            cleanup = cli_run_direct(cli_env, "scheduler", "clear-pending", timeout=30)
        except subprocess.TimeoutExpired:
            leak_msg = (
                "cleanup `scheduler clear-pending` timed out; pending "
                "collection_tasks rows may be leaked — inspect the DB manually."
            )
        else:
            if cleanup.returncode != 0:
                leak_msg = (
                    f"cleanup `scheduler clear-pending` failed; pending "
                    f"collection tasks may be leaked: stderr={cleanup.stderr!r}"
                )

        # Only raise on cleanup if the try block didn't already raise.
        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
