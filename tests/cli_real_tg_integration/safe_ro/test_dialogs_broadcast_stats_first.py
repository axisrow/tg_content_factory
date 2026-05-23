import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_dialogs_broadcast_stats_first(run_cli, assert_cli_ok, sandbox_channel_username):
    result = run_cli("dialogs", "broadcast-stats", sandbox_channel_username, timeout=120)
    assert_cli_ok(result)
    assert "Error fetching broadcast stats" not in result.stdout
    assert result.stdout.strip(), "`dialogs broadcast-stats` produced empty stdout"
