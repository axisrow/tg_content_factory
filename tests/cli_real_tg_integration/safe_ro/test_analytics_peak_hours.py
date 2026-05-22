import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_peak_hours(run_cli, assert_cli_ok):
    result = run_cli("analytics", "peak-hours")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
