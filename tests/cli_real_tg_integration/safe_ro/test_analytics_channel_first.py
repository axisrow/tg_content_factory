import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_channel_first(run_cli, assert_cli_ok, live_channel):
    _pk, channel_id = live_channel
    result = run_cli("analytics", "channel", channel_id, "--days", "7")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
