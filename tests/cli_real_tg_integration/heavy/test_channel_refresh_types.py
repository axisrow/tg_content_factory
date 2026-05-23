import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(360)
def test_channel_refresh_types(run_cli, assert_cli_ok):
    result = run_cli("channel", "refresh-types", timeout=300)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip(), (
        "`channel refresh-types` produced no output at all"
    )
