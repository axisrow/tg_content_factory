from __future__ import annotations

import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_result_failure_summary, cli_run_direct

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(120)
def test_dialogs_unpin_message_scratch_group_message(
    run_cli,
    assert_cli_ok,
    cli_real_cli_env,
    live_scratch_group_message,
):
    chat_id = live_scratch_group_message.chat_ref
    message_id = live_scratch_group_message.message_id
    phone = live_scratch_group_message.phone
    leak_msg: str | None = None
    message_pinned = False

    try:
        setup = run_cli(
            "dialogs",
            "pin-message",
            "--yes",
            "--phone",
            phone,
            chat_id,
            message_id,
            timeout=60,
        )
        assert_cli_ok(setup)
        message_pinned = True

        result = run_cli(
            "dialogs",
            "unpin-message",
            "--message-id",
            message_id,
            "--yes",
            "--phone",
            phone,
            chat_id,
            timeout=60,
        )
        assert_cli_ok(result)
        assert "Message(s) unpinned." in result.stdout
        message_pinned = False
    finally:
        if message_pinned:
            try:
                cleanup = cli_run_direct(
                    cli_real_cli_env,
                    "dialogs",
                    "unpin-message",
                    "--message-id",
                    message_id,
                    "--yes",
                    "--phone",
                    phone,
                    chat_id,
                    timeout=60,
                )
            except subprocess.TimeoutExpired:
                leak_msg = f"message {message_id} in {chat_id} may be left pinned: cleanup timed out"
            else:
                cleanup_failure = cli_result_failure_summary(cleanup)
                if cleanup_failure is not None:
                    leak_msg = f"message {message_id} in {chat_id} may be left pinned: {cleanup_failure}"

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
