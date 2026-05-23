import pytest

pytestmark = pytest.mark.real_tg_safe


def test_pipeline_templates(run_cli, assert_cli_ok):
    result = run_cli("pipeline", "templates")
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # `pipeline templates` либо «No templates found.», либо таблица с заголовком,
    # начинающимся с "ID".
    assert "No templates found" in combined or "ID" in combined, (
        f"unexpected `pipeline templates` output: {combined!r}"
    )
