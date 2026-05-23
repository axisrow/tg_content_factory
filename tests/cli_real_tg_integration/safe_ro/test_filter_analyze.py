import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_filter_analyze(run_cli, assert_cli_ok):
    result = run_cli("filter", "analyze", timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`filter analyze` produced empty stdout"
