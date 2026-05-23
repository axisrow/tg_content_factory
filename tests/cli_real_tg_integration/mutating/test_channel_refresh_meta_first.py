import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_channel_refresh_meta_first(run_cli, assert_cli_ok, live_channel):
    """Single-channel refresh-meta — 1-2 API calls, lightweight."""
    pk, _channel_id = live_channel
    result = run_cli("channel", "refresh-meta", pk, timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip(), (
        "`channel refresh-meta <pk>` produced no output at all"
    )
