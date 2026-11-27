"""`channel add-bulk --phone +X --dialog-ids N` — idempotent bulk INSERT.

The CLI handler matches each provided id against `pool.get_my_dialogs(phone)`
(via ChannelService.get_my_dialogs). If the channel_id already exists in the
DB the handler prints "SKIP: ... — already exists" and does NOT touch the
row — exactly the idempotency we need for a tier-safe_write smoke.

We reuse channel_id (numeric) from live_channel: the add-bulk handler's
`info_map` is keyed by channel_id (see
tg_agent/cli/commands/channel.py:391), not by Telethon dialog.id, so this is
the correct value to pass.

An id missing from the account's dialogs adds nothing and exits 1 (#1965).
That outcome is not a pass: the insert this test is about was never reached,
so the test checks the exit code and skips.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_channel_add_bulk_idempotent_first(run_cli, assert_cli_ok, live_phone, live_channel):
    phone = live_phone
    _pk, channel_id = live_channel
    result = run_cli(
        "channel",
        "add-bulk",
        "--phone",
        phone,
        "--dialog-ids",
        channel_id,
        timeout=180,
    )
    if "not found in dialogs" in result.stdout:
        assert result.returncode == 1, f"a miss must exit 1, got: {result.returncode}"
        assert "1 of 1 dialog id(s) are not in this account's dialogs." in result.stderr
        pytest.skip("the live channel is not in the primary account's dialogs")
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # First-time channel will say "Added"; already-known channel will say
    # "SKIP ... already exists". Either is a legitimate idempotent outcome.
    assert "Added" in combined or "already exists" in combined, (
        f"unexpected `channel add-bulk` output: {combined!r}"
    )
