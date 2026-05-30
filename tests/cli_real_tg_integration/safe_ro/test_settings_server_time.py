import pytest

pytestmark = pytest.mark.real_tg_safe


def test_settings_server_time(run_cli, assert_cli_ok):
    result = run_cli("settings", "server-time")
    assert_cli_ok(result)
    assert "UTC" in result.stdout, "`settings server-time` should print a UTC time"
