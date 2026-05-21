import pytest

pytestmark = pytest.mark.real_tg_safe


def test_messages_read_db_first(run_cli, assert_cli_ok, discover_first_channel):
    """`messages read` without --live — pure DB read, no Telegram contact."""
    pk, _channel_id = discover_first_channel()
    result = run_cli("messages", "read", pk, "--limit", "3")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
