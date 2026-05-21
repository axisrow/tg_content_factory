import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_refresh_meta_first(run_cli, assert_cli_ok, discover_first_channel):
    """Single-channel refresh-meta — 1-2 API calls, lightweight."""
    pk, _channel_id = discover_first_channel()
    result = run_cli("channel", "refresh-meta", pk, timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip(), (
        "`channel refresh-meta <pk>` produced no output at all"
    )
