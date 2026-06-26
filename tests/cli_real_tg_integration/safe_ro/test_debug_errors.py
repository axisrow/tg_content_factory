import pytest

pytestmark = pytest.mark.real_tg_safe


def test_debug_errors(run_cli, assert_cli_ok):
    result = run_cli("debug", "errors")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`debug errors` produced empty stdout"
    assert "recovery instances" in result.stdout.lower()
