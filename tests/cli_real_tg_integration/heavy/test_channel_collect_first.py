import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_collect_first(run_cli, assert_cli_ok, discover_first_channel):
    """Full collection одного канала — много API запросов (iter_messages)."""
    pk, _channel_id = discover_first_channel()
    result = run_cli("channel", "collect", pk, timeout=900)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip()
