import pytest

pytestmark = pytest.mark.real_tg_safe


def test_export_csv(run_cli, assert_cli_ok):
    result = run_cli("export", "csv", "--limit", "5")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
