import pytest

pytestmark = pytest.mark.real_tg_safe


def test_proc_scheduler_clear_pending(run_cli, assert_cli_ok):
    result = run_cli("scheduler", "clear-pending")
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    assert "Cleared" in combined and "pending collection tasks" in combined
