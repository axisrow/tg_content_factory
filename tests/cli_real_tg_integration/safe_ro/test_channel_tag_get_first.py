import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_tag_get_first(run_cli, assert_cli_ok, live_channel):
    pk, _channel_id = live_channel
    result = run_cli("channel", "tag", "get", pk)
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
