import pytest

pytestmark = pytest.mark.real_tg_safe


def test_search_query_stats_first(run_cli, assert_cli_ok, discover_first_search_query_id):
    sq_id = discover_first_search_query_id()
    result = run_cli("search-query", "stats", sq_id, "--days", "7")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
