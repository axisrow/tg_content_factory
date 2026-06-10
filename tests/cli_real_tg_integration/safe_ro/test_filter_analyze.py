import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_filter_analyze_quick(run_cli, assert_cli_ok):
    """Smoke: --quick skips the cross-dupe self-join so the command fits the live budget (#774)."""
    result = run_cli("filter", "analyze", "--quick", timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`filter analyze --quick` produced empty stdout"


@pytest.mark.timeout(660)
def test_filter_analyze_full(run_cli, assert_cli_ok):
    """Full analysis includes the heavy cross-dupe map — needs a wider budget on live DBs (#774)."""
    result = run_cli("filter", "analyze", timeout=600)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`filter analyze` produced empty stdout"
