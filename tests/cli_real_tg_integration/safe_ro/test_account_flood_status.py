import pytest

pytestmark = pytest.mark.real_tg_safe


def test_account_flood_status(run_cli, assert_cli_ok):
    result = run_cli("account", "flood-status")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`account flood-status` produced empty stdout"
