from __future__ import annotations

import re
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_result_failure_summary, cli_run_direct

pytestmark = pytest.mark.real_tg_mutation_safe

_PUBLISHED_MSG_RE = re.compile(r"published_message_id=(\d+)\s+phone=(\S+)\s+dialog_id=(\d+)")


def _pipeline_has_targets(run_cli, assert_cli_ok, pipeline_id: str) -> bool:
    """Return True if `pipeline show` lists at least one publish target.

    `pipeline show` prints a `targets:` header followed by one ` - <phone>:<id>`
    line per target. A pipeline without targets has nowhere to publish, so the
    publish run is a no-op rather than a product bug.
    """
    result = run_cli("pipeline", "show", pipeline_id)
    assert_cli_ok(result)
    in_targets = False
    for line in result.stdout.splitlines():
        if line.rstrip() == "targets:":
            in_targets = True
            continue
        if in_targets:
            if line.startswith(" - "):
                return True
            if line and not line.startswith(" "):
                break
    return False


@pytest.mark.timeout(180)
def test_pipeline_publish_sandbox(
    run_cli, assert_cli_ok, cli_real_cli_env, discover_first_pipeline_id, discover_first_run_id
):
    pipeline_id = discover_first_pipeline_id()
    if not _pipeline_has_targets(run_cli, assert_cli_ok, pipeline_id):
        pytest.skip(f"pipeline id={pipeline_id} has no publish targets; nothing to publish")
    run_id = discover_first_run_id()
    leak_msg: str | None = None
    published_entries: list[tuple[str, str, str]] = []  # (message_id, phone, dialog_id)

    try:
        result = run_cli("pipeline", "publish", run_id, timeout=120)
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert f"Published run id={run_id}" in combined

        for match in _PUBLISHED_MSG_RE.finditer(combined):
            published_entries.append((match.group(1), match.group(2), match.group(3)))

    finally:
        for message_id, phone, dialog_id in published_entries:
            try:
                cleanup = cli_run_direct(
                    cli_real_cli_env,
                    "dialogs",
                    "delete-message",
                    "--yes",
                    "--phone",
                    phone,
                    dialog_id,
                    message_id,
                    timeout=60,
                )
            except subprocess.TimeoutExpired:
                entry_msg = (
                    f"published message {message_id} in dialog {dialog_id} "
                    "may be left: cleanup timed out"
                )
                if sys.exc_info()[0] is None:
                    leak_msg = entry_msg
                else:
                    print(entry_msg, file=sys.stderr)
            else:
                cleanup_failure = cli_result_failure_summary(cleanup)
                if cleanup_failure is not None:
                    entry_msg = (
                        f"published message {message_id} in dialog {dialog_id} "
                        f"may be left: {cleanup_failure}"
                    )
                    if sys.exc_info()[0] is None:
                        leak_msg = entry_msg
                    else:
                        print(entry_msg, file=sys.stderr)

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
