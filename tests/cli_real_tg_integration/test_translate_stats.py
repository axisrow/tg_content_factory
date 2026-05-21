import pytest

pytestmark = pytest.mark.real_tg_safe


def test_translate_stats(run_cli, assert_cli_ok):
    result = run_cli("translate", "stats")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`translate stats` produced empty stdout"
