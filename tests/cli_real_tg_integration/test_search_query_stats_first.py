import re

import pytest

pytestmark = pytest.mark.real_tg_safe


def test_search_query_stats_first(run_cli, assert_cli_ok, discover_first_search_query_id):
    sq_id = discover_first_search_query_id()
    result = run_cli("search-query", "stats", sq_id, "--days", "7")
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # Либо пусто, либо строки с датами YYYY-MM-DD (гистограмма по дням).
    assert (
        "No stats found" in combined
        or re.search(r"\d{4}-\d{2}-\d{2}", combined) is not None
    ), f"unexpected `search-query stats` output: {combined!r}"
