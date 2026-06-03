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

_CHANNEL_ID_RE = re.compile(r"\bCreated channel id=(-?\d+)\b")


@pytest.mark.timeout(180)
def test_dialogs_create_channel_and_cleanup(run_cli, assert_cli_ok, cli_real_cli_env, live_phone):
    """Create a scratch broadcast channel and leave (delete) it as cleanup.

    Gate: RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1
    """
    # Unique title so cleanup can prove the channel it is about to leave is the one
    # this test created (verified via `dialogs resolve`) — never an unrelated chat.
    title = f"sbx-tmp-test-{make_cli_nonce()}"
    channel_id: str | None = None
    leak_msg: str | None = None

    try:
        result = run_cli(
            "dialogs",
            "create-channel",
            "--title",
            title,
            "--phone",
            live_phone,
            timeout=60,
        )
        assert_cli_ok(result)
        m = _CHANNEL_ID_RE.search(result.stdout)
        assert m is not None, f"no channel id in stdout: {result.stdout!r}"
        channel_id = m.group(1)
    finally:
        if channel_id is not None:
            verdict = cli_verify_channel_title(
                cli_real_cli_env,
                phone=live_phone,
                channel_id=channel_id,
                expected_title=title,
            )
            if not verdict.ok:
                # Not provably the channel we created — do not leave/delete it.
                if sys.exc_info()[0] is None:
                    leak_msg = f"channel {channel_id} left in place: {verdict.reason}"
            else:
                try:
                    cleanup = cli_run_direct(
                        cli_real_cli_env,
                        "dialogs",
                        "leave",
                        channel_id,
                        "--phone",
                        live_phone,
                        "--yes",
                        timeout=60,
                    )
                except subprocess.TimeoutExpired:
                    leak_msg = f"channel {channel_id} may still exist: cleanup leave timed out"
                else:
                    if cleanup.returncode != 0:
                        leak_msg = (
                            f"channel {channel_id} may still exist: "
                            f"cleanup leave stderr={cleanup.stderr!r}"
                        )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
