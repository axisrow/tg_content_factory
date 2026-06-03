"""`python -m src.main collect` (no args) — enqueue all eligible channels.

The bare `collect` invocation (no `--channel-id`, no `sample` sub) calls
`TaskEnqueuer.enqueue_all_channels()` which writes pending rows into the
`collection_tasks` table for every non-filtered channel (see
src/cli/commands/collect.py:48-58). The actual TG-iter_messages work is
deferred until a worker drains the queue, so this test does NOT itself
fetch messages. Still placed in heavy/ because the queue write touches
every channel in the live DB.

Self-contained cleanup: cancels ONLY the tasks this `collect` run created
(diff of a before/after snapshot of pending channel-collect task ids), by id.
It does NOT call the global `scheduler clear-pending`, which would cancel
every pending task in the queue — including ones another process enqueued.
"""
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    cancel_collection_tasks,
    snapshot_pending_collection_task_ids,
)

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(360)
def test_collect_default_enqueues_all(run_cli, assert_cli_ok, cli_env):
    leak_msg: str | None = None
    before_ids = snapshot_pending_collection_task_ids(cli_env.db_path)
    try:
        result = run_cli("collect", timeout=300)
        assert_cli_ok(result)
        combined = result.stdout + result.stderr
        # "No connected accounts" is not accepted here: the live fixture already
        # requires a configured account, so this smoke must prove enqueue works.
        assert "Enqueued" in combined, f"unexpected bare `collect` output: {combined!r}"
    finally:
        # Cancel only the tasks this collect run created, by id.
        after_ids = snapshot_pending_collection_task_ids(cli_env.db_path)
        new_ids = after_ids - before_ids
        if new_ids:
            cancel_leak = cancel_collection_tasks(cli_env, new_ids)
            if cancel_leak is not None:
                leak_msg = (
                    f"cleanup could not cancel test-created collection tasks "
                    f"{sorted(new_ids)}: {cancel_leak}"
                )

        # Only raise on cleanup if the try block didn't already raise.
        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
