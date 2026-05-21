import pytest

pytestmark = pytest.mark.real_tg_safe


def test_pipeline_show_first(run_cli, assert_cli_ok, discover_first_pipeline_id):
    pipeline_id = discover_first_pipeline_id()
    result = run_cli("pipeline", "show", pipeline_id)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`pipeline show` produced empty stdout"
