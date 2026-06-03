from __future__ import annotations

import pytest

from tests.cli_real_tg_integration.conftest import (
    _capture_cli,
    cli_result_failure_summary,
    make_cli_nonce,
)

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(120)
def test_notification_test_sends_bot_message(run_cli, assert_cli_ok, cli_real_cli_env):
    """Send a one-off notification only when the personal notification bot is configured."""
    status = _capture_cli(cli_real_cli_env, "notification", "status", timeout=60)
    status_failure = cli_result_failure_summary(status)
    if status_failure is not None:
        pytest.skip(f"notification target is unavailable: {status_failure}")
    if "Bot: @" not in (status.stdout or ""):
        pytest.skip("notification bot is not configured; skipping direct-message fallback")

    message = f"codex live cli notification bot test {make_cli_nonce()}"
    result = run_cli("notification", "test", "--message", message, timeout=60)
    assert_cli_ok(result)
    combined = f"{result.stdout}\n{result.stderr}"
    assert "Test notification sent." in combined
