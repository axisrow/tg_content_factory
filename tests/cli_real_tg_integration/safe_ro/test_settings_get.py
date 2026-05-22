import pytest

pytestmark = pytest.mark.real_tg_safe


def test_settings_get_all(run_cli, assert_cli_ok):
    result = run_cli("settings", "get")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
