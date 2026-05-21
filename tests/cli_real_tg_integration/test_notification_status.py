import pytest

pytestmark = pytest.mark.real_tg_safe


def test_notification_status(run_cli, assert_cli_ok):
    result = run_cli("notification", "status")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
