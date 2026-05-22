import pytest

pytestmark = pytest.mark.real_tg_safe


def test_dialogs_cache_status(run_cli, assert_cli_ok):
    result = run_cli("dialogs", "cache-status")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
