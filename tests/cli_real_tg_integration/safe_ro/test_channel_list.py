import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_list(run_cli, assert_cli_ok, live_channel):
    result = run_cli("channel", "list")
    assert_cli_ok(result)
    _pk, channel_id = live_channel
    assert channel_id in result.stdout, f"sandbox channel {channel_id} missing from `channel list` output"
