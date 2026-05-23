import pytest

pytestmark = pytest.mark.real_tg_safe


def test_debug_logs(run_cli, assert_cli_ok):
    result = run_cli("debug", "logs", "--limit", "10")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
