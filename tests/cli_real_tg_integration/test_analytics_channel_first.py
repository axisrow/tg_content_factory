import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_channel_first(run_cli, assert_cli_ok, discover_first_channel):
    _pk, channel_id = discover_first_channel()
    result = run_cli("analytics", "channel", channel_id, "--days", "7")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
