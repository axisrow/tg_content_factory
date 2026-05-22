import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_calendar(run_cli, assert_cli_ok):
    result = run_cli("analytics", "calendar", "--limit", "5")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
