import pytest

pytestmark = pytest.mark.real_tg_safe


def test_messages_read_first(run_cli, assert_cli_ok, discover_first_channel):
    pk, _channel_id = discover_first_channel()
    result = run_cli(
        "messages", "read", pk, "--live", "--limit", "3", timeout=180
    )
    assert_cli_ok(result)
    assert result.stdout.strip(), "`messages read --live` produced empty stdout"
