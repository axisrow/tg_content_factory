import pytest

pytestmark = pytest.mark.real_tg_safe


def test_agent_threads(run_cli, assert_cli_ok):
    result = run_cli("agent", "threads")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
