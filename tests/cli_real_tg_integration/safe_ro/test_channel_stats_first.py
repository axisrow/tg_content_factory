import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_channel_stats_first(run_cli, assert_cli_ok, sandbox_channel):
    pk, _channel_id = sandbox_channel
    result = run_cli("channel", "stats", pk, timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`channel stats` produced empty stdout"
