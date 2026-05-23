import pytest

pytestmark = pytest.mark.real_tg_safe


def test_pipeline_dry_run_count_first(run_cli, assert_cli_ok, live_channel):
    _pk, channel_id = live_channel
    result = run_cli(
        "pipeline",
        "dry-run-count",
        "--source",
        channel_id,
        "--since-value",
        "24",
        "--since-unit",
        "h",
    )
    assert_cli_ok(result)
    assert result.stdout.strip(), "`pipeline dry-run-count` produced empty stdout"
