import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(660)
def test_channel_refresh_meta_all(run_cli, assert_cli_ok):
    """N-channel refresh-meta — foreach activeChannels, риск FLOOD_WAIT."""
    result = run_cli("channel", "refresh-meta", "--all", timeout=600)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip()
