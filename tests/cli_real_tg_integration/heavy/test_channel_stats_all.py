from datetime import datetime, timezone

import pytest

from src.telegram.flood_wait import is_blocking_flood_wait_until

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(960)
def test_channel_stats_all(run_cli, assert_cli_ok, cli_real_cli_env):
    """N-channel stats — foreach activeChannels, риск FLOOD_WAIT."""
    before = _blocking_flood_state(cli_real_cli_env.db_path)
    result = run_cli("channel", "stats", "--all", timeout=900)
    assert_cli_ok(result)
    assert result.stdout.strip() or result.stderr.strip()
    assert "remaining" in result.stdout
    after = _blocking_flood_state(cli_real_cli_env.db_path)
    assert set(after) <= set(before)
    for phone, flood_until in after.items():
        assert flood_until <= before[phone]


def _blocking_flood_state(db_path):
    import sqlite3

    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT phone, flood_wait_until FROM accounts WHERE flood_wait_until IS NOT NULL"
        ).fetchall()
    state = {}
    for phone, flood_until in rows:
        if not is_blocking_flood_wait_until(flood_until, now=now):
            continue
        parsed = datetime.fromisoformat(str(flood_until))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        state[str(phone)] = parsed
    return state
