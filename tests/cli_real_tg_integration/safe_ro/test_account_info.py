import pytest

pytestmark = pytest.mark.real_tg_safe


def test_account_info(run_cli, assert_cli_ok):
    result = run_cli("account", "info")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`account info` produced empty stdout"
