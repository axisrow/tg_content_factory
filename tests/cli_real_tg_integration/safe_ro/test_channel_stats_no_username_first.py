import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(300)
def test_channel_stats_no_username_first(run_cli, assert_cli_ok, live_channel_no_username):
    """Regression for the "Обновить фильтры" loop (#794).

    A channel without a username is resolved by numeric id via PeerChannel.
    When its entity is missing from the cold cache (StringSession loses it
    across restarts), the stats path must warm the dialog cache and retry —
    or deactivate the channel — but never crash with "Could not find the
    input entity" and leave the channel forever without stats, which makes
    the filter-refresh button loop.

    Against the real API this asserts the command exits cleanly (no traceback,
    no unresolved-entity error) for a real no-username channel.
    """
    pk, _channel_id = live_channel_no_username
    result = run_cli("channel", "stats", pk, timeout=240)
    assert_cli_ok(result)

    combined = f"{result.stdout}\n{result.stderr}"
    assert "Could not find the input entity" not in combined, (
        "stats for a no-username channel must not fail on a cold entity-cache miss"
    )
    assert "Traceback" not in combined, "`channel stats` crashed for a no-username channel"
    assert result.stdout.strip(), "`channel stats` produced empty stdout"
