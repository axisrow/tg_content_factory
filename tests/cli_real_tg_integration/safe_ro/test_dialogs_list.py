import pytest

pytestmark = pytest.mark.real_tg_safe


def test_dialogs_list(run_cli, assert_cli_ok):
    result = run_cli("dialogs", "list")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs list` produced empty stdout"
