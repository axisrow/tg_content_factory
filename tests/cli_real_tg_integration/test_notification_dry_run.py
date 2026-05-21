import pytest

pytestmark = pytest.mark.real_tg_safe


def test_notification_dry_run(run_cli, assert_cli_ok):
    result = run_cli("notification", "dry-run", timeout=60)
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
