"""`python -m src.main test all` — runs read + write + telegram self-checks.

The write part copies the live DB into a tempfile and exercises mutating
operations on that copy (account_toggle, search_query CRUD, channel toggle,
filter apply/reset). The tempfile is removed at the end of the command, so
nothing persists in the real DB. The telegram part runs `Collector.sample`
on the first configured account; if no account is connected the check is
skipped, not failed.

Long-running (~30-60s on a populated DB) but bounded.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


def test_proc_test_all(run_cli, assert_cli_ok):
    result = run_cli("test", "all", timeout=300)
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # `test all` prints PASS/FAIL/SKIP lines for each check across the three
    # sections (read/write/telegram). At least one PASS is expected because
    # the read section always covers schema/version probes.
    assert "PASS" in combined, f"unexpected `test all` output: {combined!r}"
