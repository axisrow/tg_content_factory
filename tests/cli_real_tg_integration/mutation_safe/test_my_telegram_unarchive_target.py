from __future__ import annotations

import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(120)
def test_my_telegram_unarchive_scratch_group(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_group):
    chat_id = live_scratch_group.chat_ref
    phone = live_scratch_group.phone
    leak_msg: str | None = None

    try:
        setup = run_cli(
            "my-telegram",
            "archive",
            "--phone",
            phone,
            chat_id,
            timeout=60,
        )
        assert_cli_ok(setup)

        result = run_cli(
            "my-telegram",
            "unarchive",
            "--phone",
            phone,
            chat_id,
            timeout=60,
        )
        assert_cli_ok(result)
        assert f"{chat_id} unarchived." in result.stdout
    finally:
        try:
            cleanup = cli_run_direct(
                cli_real_cli_env,
                "my-telegram",
                "unarchive",
                "--phone",
                phone,
                chat_id,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            leak_msg = f"live dialog {chat_id} may be left archived: cleanup timed out"
        else:
            if cleanup.returncode != 0:
                leak_msg = (
                    f"live dialog {chat_id} may be left archived: "
                    f"cleanup stderr={cleanup.stderr!r}"
                )
            elif f"{chat_id} unarchived." not in cleanup.stdout:
                leak_msg = (
                    f"live dialog {chat_id} may be left archived: "
                    f"unexpected cleanup output={cleanup.stdout!r} stderr={cleanup.stderr!r}"
                )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
