import pytest

pytestmark = pytest.mark.real_tg_safe


def test_filter_precheck(run_cli, assert_cli_ok):
    result = run_cli("filter", "precheck", timeout=60)
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
