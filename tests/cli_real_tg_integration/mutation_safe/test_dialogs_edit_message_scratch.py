from __future__ import annotations

import re
import subprocess
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    cli_result_failure_summary,
    cli_run_direct,
    cli_verify_message_nonce,
    make_cli_nonce,
)

pytestmark = pytest.mark.real_tg_mutation_safe

_MESSAGE_ID_RE = re.compile(r"\bmessage_id=(\d+)\b")


@pytest.mark.timeout(150)
def test_dialogs_edit_scratch_message(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    chat_id = live_scratch_message_dialog.chat_ref
    phone = live_scratch_message_dialog.phone
    # Full nonce embedded in every revision of the text. The cleanup re-edit only
    # rewrites the message after confirming it still carries this nonce, so it can
    # never overwrite a message this test did not create.
    nonce = make_cli_nonce()
    initial_text = f"codex live cli edit test initial {nonce}"
    edited_text = f"codex live cli edit test edited {nonce}"
    final_text = f"codex live cli edit test completed {nonce}"
    message_id: str | None = None
    leak_msg: str | None = None

    try:
        sent = run_cli(
            "dialogs",
            "send",
            "--yes",
            "--phone",
            phone,
            chat_id,
            initial_text,
            timeout=60,
        )
        assert_cli_ok(sent)
        assert f"Message sent to {chat_id}." in sent.stdout
        match = _MESSAGE_ID_RE.search(sent.stdout)
        assert match is not None, f"send stdout did not include message_id: {sent.stdout!r}"
        message_id = match.group(1)

        result = run_cli(
            "dialogs",
            "edit-message",
            "--yes",
            "--phone",
            phone,
            chat_id,
            message_id,
            edited_text,
            timeout=60,
        )
        assert_cli_ok(result)
        assert f"Message #{message_id} edited." in result.stdout
    finally:
        if message_id is not None:
            verdict = cli_verify_message_nonce(
                cli_real_cli_env,
                phone=phone,
                chat_ref=chat_id,
                message_id=int(message_id),
                nonce=nonce,
            )
            if not verdict.ok:
                # Not provably our message — do not re-edit it.
                if sys.exc_info()[0] is None:
                    leak_msg = f"message {message_id} in {chat_id} left as-is: {verdict.reason}"
            else:
                try:
                    cleanup = cli_run_direct(
                        cli_real_cli_env,
                        "dialogs",
                        "edit-message",
                        "--yes",
                        "--phone",
                        phone,
                        chat_id,
                        message_id,
                        final_text,
                        timeout=60,
                    )
                except subprocess.TimeoutExpired:
                    leak_msg = f"message {message_id} in {chat_id} may be left with test text: cleanup timed out"
                else:
                    cleanup_failure = cli_result_failure_summary(cleanup)
                    if cleanup_failure is not None:
                        leak_msg = (
                            f"message {message_id} in {chat_id} may be left with test text: {cleanup_failure}"
                        )

        if leak_msg:
            print(leak_msg, file=sys.stderr)
            if sys.exc_info()[0] is None:
                pytest.fail(leak_msg)
