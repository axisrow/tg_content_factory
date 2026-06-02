from __future__ import annotations

import pytest

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(90)
def test_notification_test_sends_message(run_cli, assert_cli_ok):
    """Verify that `notification test` sends without error.

    The notification is sent to the configured bot target. The operation is
    effectively idempotent: repeated test notifications are non-destructive and
    do not require a separate cleanup step.
    """
    result = run_cli("notification", "test", "--message", "codex live cli notification test", timeout=60)
    assert_cli_ok(result)
    combined = f"{result.stdout}\n{result.stderr}"
    assert "Test notification sent." in combined
