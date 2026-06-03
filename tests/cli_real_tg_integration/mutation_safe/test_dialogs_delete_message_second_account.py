from __future__ import annotations

import os
import re
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    CLI_REAL_TG_MUTATION_PHONE_ENV,
    _capture_cli,
    cleanup_verified_messages,
    cli_result_failure_summary,
    make_cli_nonce,
)

pytestmark = pytest.mark.real_tg_mutation_safe

_MESSAGE_ID_RE = re.compile(r"\bmessage_id=(\d+)\b")


@pytest.mark.timeout(180)
def test_dialogs_delete_second_account_message(run_cli, assert_cli_ok, cli_real_cli_env):
    """Create one DM message to another connected account, then delete only that message."""
    configured_sender = os.environ.get(CLI_REAL_TG_MUTATION_PHONE_ENV)
    if configured_sender and configured_sender not in cli_real_cli_env.phones:
        pytest.skip(f"{CLI_REAL_TG_MUTATION_PHONE_ENV} is not a connected live CLI account")

    sender_phone = configured_sender or cli_real_cli_env.primary_phone
    recipient_phone = next(
        (phone for phone in cli_real_cli_env.phones if phone != sender_phone),
        None,
    )
    if recipient_phone is None:
        pytest.skip("`dialogs delete-message` live test requires a second connected account")

    resolve = _capture_cli(
        cli_real_cli_env,
        "dialogs",
        "resolve",
        recipient_phone,
        "--phone",
        sender_phone,
        timeout=60,
    )
    resolve_failure = cli_result_failure_summary(resolve)
    if resolve_failure is not None or "Type: dm" not in (resolve.stdout or ""):
        pytest.skip(
            "second connected account is not resolvable from sender account; "
            f"resolve failure={resolve_failure!r}"
        )

    nonce = make_cli_nonce()
    text = f"codex live cli delete second-account test {nonce}"
    message_id: str | None = None
    deleted = False
    leak_msg: str | None = None

    try:
        sent = run_cli(
            "dialogs",
            "send",
            "--yes",
            "--phone",
            sender_phone,
            recipient_phone,
            text,
            timeout=60,
        )
        assert_cli_ok(sent)
        assert f"Message sent to {recipient_phone}." in sent.stdout, f"unexpected send output: {sent.stdout!r}"
        match = _MESSAGE_ID_RE.search(sent.stdout)
        assert match is not None, f"send stdout did not include message_id: {sent.stdout!r}"
        message_id = match.group(1)

        result = run_cli(
            "dialogs",
            "delete-message",
            "--yes",
            "--phone",
            sender_phone,
            recipient_phone,
            message_id,
            timeout=60,
        )
        assert_cli_ok(result)
        assert "Deleted 1 message(s)." in result.stdout, f"unexpected delete output: {result.stdout!r}"
        deleted = True
    finally:
        if message_id is not None and not deleted:
            leak_msg = cleanup_verified_messages(
                cli_real_cli_env,
                phone=sender_phone,
                chat_ref=recipient_phone,
                candidates=[int(message_id)],
                nonce=nonce,
            )

        if leak_msg:
            print(leak_msg, file=sys.stderr)
            if sys.exc_info()[0] is None:
                pytest.fail(leak_msg)
