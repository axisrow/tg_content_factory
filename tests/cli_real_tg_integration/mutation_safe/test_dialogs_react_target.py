from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(90)
def test_dialogs_react_scratch_message(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message):
    chat_id = live_scratch_message.chat_ref
    message_id = live_scratch_message.message_id
    phone = live_scratch_message.phone
    emoji = os.environ.get("CLI_REAL_TG_REACT_EMOJI", "👍")
    leak_msg: str | None = None

    try:
        result = run_cli(
            "dialogs",
            "react",
            "--yes",
            "--phone",
            phone,
            chat_id,
            message_id,
            emoji,
            timeout=60,
        )

        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert "Reaction" in combined and "sent to message" in combined
    finally:
        try:
            cleanup = cli_run_direct(
                cli_real_cli_env,
                "dialogs",
                "react",
                "--clear",
                "--yes",
                "--phone",
                phone,
                chat_id,
                message_id,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            leak_msg = f"reaction {emoji!r} on message {message_id} in {chat_id} may be left set: cleanup timed out"
        else:
            if cleanup.returncode != 0:
                leak_msg = (
                    f"reaction {emoji!r} on message {message_id} in {chat_id} may be left set: "
                    f"cleanup stderr={cleanup.stderr!r}"
                )
            elif "Reaction cleared" not in f"{cleanup.stdout}\n{cleanup.stderr}":
                leak_msg = (
                    f"reaction {emoji!r} on message {message_id} in {chat_id} may be left set: "
                    f"unexpected cleanup output={cleanup.stdout!r} stderr={cleanup.stderr!r}"
                )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
