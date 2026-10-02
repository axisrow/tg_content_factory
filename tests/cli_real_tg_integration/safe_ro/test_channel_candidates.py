import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_candidates(run_cli, assert_cli_ok):
    result = run_cli("channel", "candidates")
    assert_cli_ok(result)
    assert result.stdout.strip()
