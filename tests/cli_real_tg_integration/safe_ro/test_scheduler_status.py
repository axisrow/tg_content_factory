import pytest

pytestmark = pytest.mark.real_tg_safe


def test_scheduler_status(run_cli, assert_cli_ok):
    result = run_cli("scheduler", "status")
    assert_cli_ok(result)
    assert result.stdout.strip(), "`scheduler status` produced empty stdout"
