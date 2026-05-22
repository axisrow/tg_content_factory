import pytest

pytestmark = pytest.mark.real_tg_safe


def test_pipeline_moderation_view_first(run_cli, assert_cli_ok, discover_first_run_id):
    run_id = discover_first_run_id()
    result = run_cli("pipeline", "moderation-view", run_id)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`pipeline moderation-view` produced empty stdout"
