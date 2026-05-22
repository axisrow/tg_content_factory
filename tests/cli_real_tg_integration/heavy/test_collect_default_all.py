"""`python -m src.main collect` (no args) — enqueue all eligible channels.

The bare `collect` invocation (no `--channel-id`, no `sample` sub) calls
`TaskEnqueuer.enqueue_all_channels()` which writes pending rows into the
`collection_tasks` table for every non-filtered channel (see
src/cli/commands/collect.py:48-58). The actual TG-iter_messages work is
deferred until a worker drains the queue, so this test does NOT itself
fetch messages. Still placed in heavy/ because the queue write touches
every channel in the live DB and aligns with how the issue groups
collection commands.

Per user instruction, this PR is paired with the matching
`scheduler clear-pending` cleanup invocation (already covered by the
existing mutating/test_proc_scheduler_trigger test). Operators who run
this test sequentially with that one will leave the queue clean.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


def test_collect_default_enqueues_all(run_cli, assert_cli_ok):
    result = run_cli("collect", timeout=300)
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # Possible outcomes:
    # - "Enqueued N channels (skipped M, total K)..." — normal path.
    # - "No connected accounts..." — pool empty, the handler short-circuits
    #   before reaching the enqueue branch.
    assert (
        "Enqueued" in combined
        or "No connected accounts" in combined
    ), f"unexpected bare `collect` output: {combined!r}"
