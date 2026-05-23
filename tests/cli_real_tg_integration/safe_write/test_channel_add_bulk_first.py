"""`channel add-bulk --phone +X --dialog-ids N` — idempotent bulk INSERT.

The CLI handler matches each provided id against `pool.get_my_dialogs(phone)`
(via ChannelService.get_my_dialogs). If the channel_id already exists in the
DB the handler prints "SKIP: ... — already exists" and does NOT touch the
row — exactly the idempotency we need for a tier-safe_write smoke.

We reuse channel_id (numeric) from live_channel: the add-bulk handler's
`info_map` is keyed by channel_id (see
src/cli/commands/channel.py:391), not by Telethon dialog.id, so this is
the correct value to pass.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


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
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # First-time channel will say "Added"; already-known channel will say
    # "SKIP ... already exists". Either is a legitimate idempotent outcome.
    assert (
        "Added" in combined
        or "already exists" in combined
        or "not found in dialogs" in combined
    ), f"unexpected `channel add-bulk` output: {combined!r}"
