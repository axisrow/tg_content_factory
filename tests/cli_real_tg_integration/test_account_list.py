import pytest

pytestmark = pytest.mark.real_tg_safe


def test_account_list(run_cli, assert_cli_ok, sandbox_phone):
    result = run_cli("account", "list")
    assert_cli_ok(result)
    assert sandbox_phone in result.stdout, f"sandbox phone {sandbox_phone} missing from `account list`"
