"""`scheduler trigger` — one-shot enqueue of all eligible channels.

Writes collection_tasks rows (pending) for every non-filtered channel. This
is reversible via `scheduler clear-pending`, which the test runs in finally
to leave the queue as it was before.
"""
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_safe


def test_proc_scheduler_trigger_enqueues(run_cli, assert_cli_ok, cli_env):
    leak_msg: str | None = None
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
        # Cleanup uses cli_run_direct (not run_cli) so a TimeoutExpired here
        # raises explicitly instead of pytest.skip(), which would replace any
        # in-flight AssertionError from the try block.
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

        # Only raise on cleanup if the try block didn't already fail.
        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
