import pytest

pytestmark = pytest.mark.real_tg_safe


def test_pipeline_runs_first(run_cli, assert_cli_ok, discover_first_pipeline_id):
    pipeline_id = discover_first_pipeline_id()
    result = run_cli("pipeline", "runs", pipeline_id, "--limit", "5")
    assert_cli_ok(result)
    assert result.stdout.strip() or "no" in (result.stdout + result.stderr).lower()
