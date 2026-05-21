import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_stats_first(run_cli, assert_cli_ok, discover_first_channel):
    pk, _channel_id = discover_first_channel()
    result = run_cli("channel", "stats", pk, timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`channel stats` produced empty stdout"
