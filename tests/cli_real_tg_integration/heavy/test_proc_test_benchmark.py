"""`python -m src.main test benchmark` — runs pytest 3 times back-to-back.

Compares serial vs parallel-safe vs aiosqlite-serial elapsed times. Each
sub-run is a real `pytest` subprocess (see src/cli/commands/test.py:122,
`_run_pytest_benchmark`). Total wall time depends on the test suite size;
keep the timeout generous (10 min) for a populated repo.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(660)
def test_proc_test_benchmark(run_cli, assert_cli_ok):
    result = run_cli("test", "benchmark", timeout=600)
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # The handler prints elapsed-time lines for each of the three pytest
    # invocations. "serial" appears in the first label.
    assert "serial" in combined.lower(), (
        f"`test benchmark` did not produce timing output: {combined!r}"
    )
