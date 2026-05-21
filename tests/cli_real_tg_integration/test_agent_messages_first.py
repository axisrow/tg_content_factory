import pytest

pytestmark = pytest.mark.real_tg_safe


def test_agent_messages_first(run_cli, assert_cli_ok, discover_first_agent_thread_id):
    thread_id = discover_first_agent_thread_id()
    result = run_cli("agent", "messages", thread_id, "--limit", "5")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
