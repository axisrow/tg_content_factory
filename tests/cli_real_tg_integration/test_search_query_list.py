import pytest

pytestmark = pytest.mark.real_tg_safe


def test_search_query_list(run_cli, assert_cli_ok):
    result = run_cli("search-query", "list")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
