import pytest

pytestmark = pytest.mark.real_tg_safe


def test_dialogs_refresh(run_cli, assert_cli_ok):
    result = run_cli("dialogs", "refresh", timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs refresh` produced empty stdout"
