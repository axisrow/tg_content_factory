import pytest

pytestmark = pytest.mark.real_tg_safe


def test_pipeline_export_first(run_cli, assert_cli_ok, discover_first_pipeline_id):
    pipeline_id = discover_first_pipeline_id()
    # без -o → JSON в stdout
    result = run_cli("pipeline", "export", pipeline_id)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`pipeline export` produced empty stdout"
