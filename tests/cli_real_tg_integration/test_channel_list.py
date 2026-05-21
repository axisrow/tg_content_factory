import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_list(run_cli, assert_cli_ok):
    result = run_cli("channel", "list")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`channel list` produced empty stdout"
