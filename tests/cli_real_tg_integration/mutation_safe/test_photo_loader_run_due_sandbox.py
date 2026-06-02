from __future__ import annotations

import pytest

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(150)
def test_photo_loader_run_due_sandbox(run_cli, assert_cli_ok, cli_real_cli_env):
    """Verify that `photo-loader run-due` completes successfully.

    The command processes all due photo_batch_items and auto-upload jobs.
    In a clean test environment there may be zero due items, which is
    acceptable — the command still validates the processing path.
    """
    result = run_cli("photo-loader", "run-due", timeout=120)
    assert_cli_ok(result)
    combined = f"{result.stdout}\n{result.stderr}"
    assert "Processed due photo items=" in combined
