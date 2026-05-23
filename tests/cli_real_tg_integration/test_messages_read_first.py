import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_messages_read_first(run_cli, assert_cli_ok, sandbox_channel):
    pk, _channel_id = sandbox_channel
    result = run_cli(
        "messages", "read", pk, "--live", "--limit", "3", timeout=180
    )
    assert_cli_ok(result)
    assert result.stdout.strip(), "`messages read --live` produced empty stdout"
