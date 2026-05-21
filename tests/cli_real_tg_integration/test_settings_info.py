import pytest

pytestmark = pytest.mark.real_tg_safe


def test_settings_info(run_cli, assert_cli_ok):
    result = run_cli("settings", "info")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`settings info` produced empty stdout"
