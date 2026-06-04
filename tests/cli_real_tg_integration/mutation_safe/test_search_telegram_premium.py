from __future__ import annotations

import re
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    _await_phone_flood_or_skip,
    _first_premium_phone,
    cli_run_direct,
)

pytestmark = pytest.mark.real_tg_mutation_safe

# Premium global search (`search --mode=telegram`) caches its hits into the shared
# `messages` table, tagging only newly inserted rows with `premium_search_query`
# (existing user messages are skipped by INSERT OR IGNORE and never tagged). The
# test searches for "тест", then deletes exactly those tagged rows via the
# `search --purge-cache` CLI — never touching pre-existing user data.
_FOUND_RE = re.compile(r"\bFound\s+(\d+)\s+results\b")
_PURGED_RE = re.compile(r"\bPurged\s+(\d+)\s+cached message")
_QUERY = "тест"


# Two sequential live CLI round-trips (the global search itself is slow, plus a
# cold-start purge process), so the per-test budget covers both 60s calls with
# headroom — 90s was not enough and timed out mid-cleanup.
@pytest.mark.timeout(180)
def test_search_telegram_premium_caches_and_purges(run_cli, assert_cli_ok, cli_real_cli_env):
    phone = _first_premium_phone(cli_real_cli_env.db_path, cli_real_cli_env.phones)
    if phone is None:
        pytest.skip("no connected Telegram Premium account; global search requires Telegram Premium")
    _await_phone_flood_or_skip(cli_real_cli_env, phone)

    leak_msg: str | None = None
    try:
        result = run_cli(
            "search",
            _QUERY,
            "--mode",
            "telegram",
            "--limit",
            "1",
            timeout=60,
        )
        assert_cli_ok(result)

        match = _FOUND_RE.search(result.stdout or "")
        assert match is not None, f"search did not print a 'Found N results' line: {result.stdout!r}"
        found = int(match.group(1))
        if found == 0:
            # 0 results is indistinguishable from an exhausted daily quota / flood
            # via stdout (the command prints total, not the error). Nothing was
            # cached, so nothing to verify or clean up — skip rather than fail.
            pytest.skip("Premium search returned 0 results (empty or daily quota exhausted)")
    finally:
        # Always purge our tagged rows, even if the assertions above failed after a
        # successful search. Deletes only rows tagged premium_search_query == _QUERY.
        try:
            cleanup = cli_run_direct(
                cli_real_cli_env,
                "search",
                _QUERY,
                "--purge-cache",
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            leak_msg = f"cached Premium-search rows for {_QUERY!r} may be left in DB: purge timed out"
        else:
            if cleanup.returncode != 0:
                leak_msg = (
                    f"cached Premium-search rows for {_QUERY!r} may be left in DB: "
                    f"purge stderr={cleanup.stderr!r}"
                )
            elif _PURGED_RE.search(f"{cleanup.stdout}\n{cleanup.stderr}") is None:
                leak_msg = (
                    f"cached Premium-search rows for {_QUERY!r} may be left in DB: "
                    f"unexpected purge output={cleanup.stdout!r} stderr={cleanup.stderr!r}"
                )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
