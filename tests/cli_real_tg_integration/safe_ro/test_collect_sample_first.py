import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_collect_sample_first(run_cli, assert_cli_ok, live_channel):
    """`collect sample <channel_id> --limit 5` — read-only preview.

    Calls collector.sample_channel() against the real Telegram API without
    writing anything to the DB (see src/cli/commands/collect.py:64-91).
    """
    _pk, channel_id = live_channel
    result = run_cli("collect", "sample", channel_id, "--limit", "5", timeout=120)
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # The handler prints either a sampling header + per-message rows, or
    # "No messages found." when the channel is empty. Both are legitimate.
    assert "Sampling" in combined or "No messages found" in combined, (
        f"unexpected `collect sample` output: {combined!r}"
    )
