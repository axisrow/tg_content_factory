import pytest

pytestmark = pytest.mark.real_tg_safe


def test_agent_thread_delete_noop(run_cli, assert_cli_ok):
    result = run_cli("agent", "thread-delete", "0")
    assert_cli_ok(result)
    assert "Тред #0 удалён" in result.stdout
