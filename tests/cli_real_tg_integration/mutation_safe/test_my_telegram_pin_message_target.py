from __future__ import annotations

import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(120)
def test_my_telegram_pin_message_owned_message(run_cli, assert_cli_ok, cli_real_cli_env, live_pin_mutation_message):
    chat_id = live_pin_mutation_message.chat_ref
    message_id = live_pin_mutation_message.message_id
    phone = live_pin_mutation_message.phone
    leak_msg: str | None = None

    try:
        result = run_cli(
            "my-telegram",
            "pin-message",
            "--yes",
            "--phone",
            phone,
            chat_id,
            message_id,
            timeout=60,
        )
        assert_cli_ok(result)
        assert f"Message #{message_id} pinned." in result.stdout
    finally:
        try:
            cleanup = cli_run_direct(
                cli_real_cli_env,
                "my-telegram",
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
            if cleanup.returncode != 0:
                leak_msg = (
                    f"message {message_id} in {chat_id} may be left pinned: "
                    f"cleanup stderr={cleanup.stderr!r}"
                )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
