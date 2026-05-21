import pytest

pytestmark = pytest.mark.real_tg_safe


def test_search_query_run_first(run_cli, assert_cli_ok, discover_first_search_query_id):
    sq_id = discover_first_search_query_id()
    # `search-query run` исполняет запрос против локальной БД (FTS5), без
    # сетевых вызовов. Может выдать «No matches» — это валидно.
    result = run_cli("search-query", "run", sq_id, timeout=60)
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
