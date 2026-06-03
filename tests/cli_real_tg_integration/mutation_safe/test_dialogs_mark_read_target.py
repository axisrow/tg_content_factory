from __future__ import annotations

import pytest

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(90)
def test_dialogs_mark_read_scratch_message(run_cli, assert_cli_ok, live_scratch_message):
    chat_id = live_scratch_message.chat_ref
    max_id = live_scratch_message.message_id
    phone = live_scratch_message.phone

    result = run_cli(
        "dialogs",
        "mark-read",
        "--max-id",
        max_id,
        "--phone",
        phone,
        chat_id,
        timeout=60,
    )

    assert_cli_ok(result)
    assert f"Messages marked as read in {chat_id}." in result.stdout
