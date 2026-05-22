import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_pipeline_stats(run_cli, assert_cli_ok):
    result = run_cli("analytics", "pipeline-stats")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
