import pytest

pytestmark = pytest.mark.real_tg_safe


def test_search_query_get_first(run_cli, assert_cli_ok, discover_first_search_query_id):
    sq_id = discover_first_search_query_id()
    result = run_cli("search-query", "get", sq_id)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`search-query get` produced empty stdout"
