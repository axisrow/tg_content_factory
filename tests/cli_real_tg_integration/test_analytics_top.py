import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_top(run_cli, assert_cli_ok):
    result = run_cli("analytics", "top", "--limit", "5", timeout=60)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`analytics top` produced empty stdout"
