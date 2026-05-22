import pytest

pytestmark = pytest.mark.real_tg_safe


def test_analytics_trending_topics(run_cli, assert_cli_ok):
    result = run_cli("analytics", "trending-topics", "--days", "7", "--limit", "5")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
