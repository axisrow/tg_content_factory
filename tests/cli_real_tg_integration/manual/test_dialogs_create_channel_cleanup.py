from __future__ import annotations

import re
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import cli_run_direct

pytestmark = pytest.mark.real_tg_manual

_CHANNEL_ID_RE = re.compile(r"\bCreated channel id=(-?\d+)\b")


@pytest.mark.timeout(180)
def test_dialogs_create_channel_and_cleanup(run_cli, assert_cli_ok, cli_real_cli_env, live_phone):
    """Create a scratch broadcast channel and leave (delete) it as cleanup.

    Gate: RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1
    """
    channel_id: str | None = None
    leak_msg: str | None = None

    try:
        result = run_cli(
            "dialogs",
            "create-channel",
            "--title",
            "sbx-tmp-test",
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
