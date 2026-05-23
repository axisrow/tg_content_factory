import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(960)
def test_channel_stats_all(run_cli, assert_cli_ok):
    """N-channel stats — foreach activeChannels, риск FLOOD_WAIT."""
    result = run_cli("channel", "stats", "--all", timeout=900)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip()
