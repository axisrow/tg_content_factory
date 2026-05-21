import pytest

pytestmark = pytest.mark.real_tg_safe


def test_debug_timing(run_cli, assert_cli_ok):
    result = run_cli("debug", "timing")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`debug timing` produced empty stdout"
