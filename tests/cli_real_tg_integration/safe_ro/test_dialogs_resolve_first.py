import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_dialogs_resolve_first(run_cli, assert_cli_ok, sandbox_channel_username):
    result = run_cli("dialogs", "resolve", sandbox_channel_username, timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs resolve` produced empty stdout"
