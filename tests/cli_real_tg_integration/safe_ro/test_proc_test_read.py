"""`python -m src.main test read` — runs the built-in read-only DB self-check.

The handler copies the live DB into a tempfile and exercises a fixed set of
SELECT/aggregation operations there, then removes the tempfile. Nothing is
written back to the real DB, so this stays in safe_ro.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


def test_proc_test_read(run_cli, assert_cli_ok):
    result = run_cli("test", "read")
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # `test read` prints a summary section. The exact format may evolve, but
    # the word "PASS" (or "FAIL") for each check is stable.
    assert "PASS" in combined or "FAIL" in combined, (
        f"unexpected `test read` output: {combined!r}"
    )
