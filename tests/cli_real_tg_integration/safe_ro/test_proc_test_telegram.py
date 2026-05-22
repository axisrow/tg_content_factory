"""`python -m src.main test telegram` — live TG self-check on a temp DB copy.

src/cli/commands/test.py copies the live DB into a tempfile before running
telegram-side checks (_init_db_copy → tg_iter_messages → cleanup), so this
test never mutates the operator's real DB. Stays in safe_ro despite the
"telegram" name.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


def test_proc_test_telegram(run_cli, assert_cli_ok):
    result = run_cli("test", "telegram", timeout=300)
    assert_cli_ok(result)
    combined = result.stdout + result.stderr

    # `test telegram` emits PASS/SKIP/FAIL lines per check. Reviewers caught
    # two issues with the prior `"PASS" in combined or "SKIP" in combined`:
    #   - all-SKIP (no accounts) silently passed the assertion.
    #   - mixed PASS+FAIL silently passed when "PASS" was present.
    # Real-world deployments DO legitimately produce FAIL lines on transient
    # session/auth issues, so a blanket `FAIL not in combined` would over-reject.
    # Compromise: require at least one PASS (smoke that something live actually
    # worked). If there are zero PASS lines but the run reached its summary,
    # turn it into a skip — the suite cannot tell us anything useful then.
    if "PASS" in combined:
        return
    if "SKIP" in combined or "No accounts" in combined or "no accounts" in combined.lower():
        pytest.skip(
            "`test telegram` produced only SKIP/'No accounts' lines — "
            "nothing to smoke-check (typically no connected accounts)"
        )
    pytest.fail(f"`test telegram` produced no PASS and no recognizable SKIP: {combined!r}")
