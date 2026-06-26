import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_filter_analyze_quick(run_cli, assert_cli_ok):
    """Smoke: --quick samples the last N msgs/channel + skips cross-dupe (#774, #1138).

    Since #1138 quick also bypasses the full-history text scan (samples last 300),
    so it finishes in seconds and fits the live budget with room to spare.
    """
    result = run_cli("filter", "analyze", "--quick", timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`filter analyze --quick` produced empty stdout"


@pytest.mark.timeout(240)
def test_filter_analyze_quick_sample_size(run_cli, assert_cli_ok):
    """Smoke: --quick --sample-size overrides the sampled window size (#1138)."""
    result = run_cli("filter", "analyze", "--quick", "--sample-size", "100", timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`filter analyze --quick --sample-size 100` produced empty stdout"


@pytest.mark.timeout(660)
def test_filter_analyze_full(run_cli, assert_cli_ok):
    """Full analysis includes the heavy cross-dupe map — needs a wider budget on live DBs (#774)."""
    result = run_cli("filter", "analyze", timeout=600)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`filter analyze` produced empty stdout"
