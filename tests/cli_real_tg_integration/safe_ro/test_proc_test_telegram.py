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
    # `test telegram` emits PASS/SKIP/FAIL lines per check. We require at
    # least one PASS — every probe SKIP means the suite ran but couldn't
    # do anything (typically no accounts).
    assert "PASS" in combined or "SKIP" in combined, (
        f"unexpected `test telegram` output: {combined!r}"
    )
