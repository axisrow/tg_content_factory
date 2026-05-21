import pytest

pytestmark = pytest.mark.real_tg_safe


def test_search_basic(run_cli, assert_cli_ok):
    # Local mode — пользуется FTS5, не дёргает Telegram, безопасно
    result = run_cli("search", "test", "--limit", "5", "--mode", "local", timeout=60)
    assert_cli_ok(result)
    # Может быть «No results» — это валидно, главное что команда не упала
    assert result.stdout.strip() or "no results" in (result.stderr or "").lower()
