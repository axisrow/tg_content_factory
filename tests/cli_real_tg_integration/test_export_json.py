import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(90)
def test_export_json(run_cli, assert_cli_ok):
    result = run_cli("export", "json", "--limit", "5", timeout=60)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`export json` produced empty stdout"
