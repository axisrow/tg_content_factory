from __future__ import annotations

import re
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    cli_run_direct,
    cli_verify_channel_title,
    make_cli_nonce,
)

pytestmark = pytest.mark.real_tg_manual

_GROUP_ID_RE = re.compile(r"\bCreated group id=(-?\d+)\b")


@pytest.mark.timeout(180)
def test_dialogs_create_group_and_cleanup(run_cli, assert_cli_ok, cli_real_cli_env, live_phone):
    """Create a scratch group and leave it as cleanup.

    Manual check after running: if the test fails or reports a cleanup leak,
    inspect Telegram and remove only the temporary group with this test title.

    Gate: RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1
    """
    title = f"sbx-tmp-group-{make_cli_nonce()}"
    group_id: str | None = None
    leak_msg: str | None = None

    try:
        result = run_cli(
            "dialogs",
            "create-group",
            "--title",
            title,
            "--phone",
            live_phone,
            timeout=90,
        )
        assert_cli_ok(result)
        match = _GROUP_ID_RE.search(result.stdout)
        assert match is not None, f"no group id in stdout: {result.stdout!r}"
        group_id = match.group(1)
    finally:
        if group_id is not None:
            verdict = cli_verify_channel_title(
                cli_real_cli_env,
                phone=live_phone,
                channel_id=group_id,
                expected_title=title,
            )
            if not verdict.ok:
                if sys.exc_info()[0] is None:
                    leak_msg = f"group {group_id} left in place: {verdict.reason}"
            else:
                try:
                    cleanup = cli_run_direct(
                        cli_real_cli_env,
                        "dialogs",
                        "leave",
                        group_id,
                        "--phone",
                        live_phone,
                        "--yes",
                        timeout=60,
                    )
                except subprocess.TimeoutExpired:
                    leak_msg = f"group {group_id} may still exist: cleanup leave timed out"
                else:
                    if cleanup.returncode != 0:
                        leak_msg = (
                            f"group {group_id} may still exist: "
                            f"cleanup leave stderr={cleanup.stderr!r}"
                        )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
