import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_dialogs_broadcast_stats_first(run_cli, assert_cli_ok, live_owned_broadcast_channel):
    # broadcast-stats requires channel admin rights, so target an own broadcast
    # channel via the account that administers it (not an arbitrary monitored one).
    result = run_cli(
        "dialogs",
        "broadcast-stats",
        "--phone",
        live_owned_broadcast_channel.phone,
        live_owned_broadcast_channel.chat_ref,
        timeout=120,
    )
    assert_cli_ok(result)
    assert "Error fetching broadcast stats" not in result.stdout
    assert result.stdout.strip(), "`dialogs broadcast-stats` produced empty stdout"
