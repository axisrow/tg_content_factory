import pytest

pytestmark = pytest.mark.real_tg_safe


def test_account_list(run_cli, assert_cli_ok):
    result = run_cli("account", "list")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`account list` produced empty stdout"
