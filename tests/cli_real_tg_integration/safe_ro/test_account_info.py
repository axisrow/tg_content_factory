import pytest

pytestmark = pytest.mark.real_tg_safe


def test_account_info(run_cli, assert_cli_ok, live_phone):
    result = run_cli("account", "info")
    assert_cli_ok(result)
    assert live_phone in result.stdout, f"sandbox phone {live_phone} missing from `account info`"
