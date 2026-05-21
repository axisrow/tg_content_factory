import pytest

pytestmark = pytest.mark.real_tg_safe


def test_debug_memory(run_cli, assert_cli_ok):
    result = run_cli("debug", "memory")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`debug memory` produced empty stdout"
