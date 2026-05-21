import pytest

pytestmark = pytest.mark.real_tg_safe


def test_dialogs_resolve_first(run_cli, assert_cli_ok, discover_first_dialog_username):
    username = discover_first_dialog_username()
    result = run_cli("dialogs", "resolve", username, timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs resolve` produced empty stdout"
