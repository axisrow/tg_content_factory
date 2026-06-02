from __future__ import annotations

import re
import uuid

import pytest

pytestmark = pytest.mark.real_tg_manual

_MESSAGE_ID_RE = re.compile(r"\bmessage_id=(\d+)\b")


@pytest.mark.timeout(180)
def test_dialogs_delete_scratch_message(run_cli, assert_cli_ok, live_scratch_message_dialog):
    """Send a scratch message to a self-owned dialog then immediately delete it.

    The deletion is the primary operation under test.  Sending is setup only.
    No external cleanup is required: the message is deleted as the final step,
    and if the test fails before the delete the message is left in the
    operator's own chat — an acceptable temporary artefact.

    Gate: RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1
    """
    chat_id = live_scratch_message_dialog.chat_ref
    phone = live_scratch_message_dialog.phone
    marker = uuid.uuid4().hex[:12]
    text = f"codex live cli delete test {marker}"

    sent = run_cli(
        "dialogs",
        "send",
        "--yes",
        "--phone",
        phone,
        chat_id,
        text,
        timeout=60,
    )
    assert_cli_ok(sent)
    assert f"Message sent to {chat_id}." in sent.stdout, f"unexpected send output: {sent.stdout!r}"
    m = _MESSAGE_ID_RE.search(sent.stdout)
    assert m is not None, f"send stdout did not include message_id: {sent.stdout!r}"
    message_id = m.group(1)

    result = run_cli(
        "dialogs",
        "delete-message",
        "--yes",
        "--phone",
        phone,
        chat_id,
        message_id,
        timeout=60,
    )
    assert_cli_ok(result)
    assert "Deleted 1 message(s)." in result.stdout, f"unexpected delete output: {result.stdout!r}"
