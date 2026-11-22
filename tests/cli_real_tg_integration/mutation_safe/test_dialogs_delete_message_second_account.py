from __future__ import annotations

import os
import re
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    CLI_REAL_TG_MUTATION_PHONE_ENV,
    CliRealCliEnv,
    _capture_cli,
    _parse_live_message_blocks,
    cleanup_verified_messages,
    cli_result_failure_summary,
    cli_verify_message_nonce,
    make_cli_nonce,
)

pytestmark = pytest.mark.real_tg_mutation_safe

_MESSAGE_ID_RE = re.compile(r"\bmessage_id=(\d+)\b")
_RESOLVED_DM_RE = re.compile(
    r"^Type: dm\r?\nID: ([1-9]\d*)(?:\r?\nUsername: (@[a-zA-Z0-9_]+))?\s*\Z",
    re.MULTILINE,
)
_STOP_LOOKUP_RE = re.compile(
    r"\b(?:HandledFloodWaitError|FloodWaitError|FLOOD[ _-]?WAIT(?:_\d+)?|rate-limited|TelegramRateLimitedError|"
    r"PEER_FLOOD|PeerFloodError|AUTH_KEY_DUPLICATED|AuthKeyDuplicatedError|AuthKeyUnregisteredError|UnauthorizedError)\b",
    re.IGNORECASE,
)


def _resolve_dm(cli_env: CliRealCliEnv, phone: str, ref: str) -> tuple[int, str | None] | None:
    result = _capture_cli(cli_env, "dialogs", "resolve", ref, "--phone", phone, timeout=30)
    if cli_result_failure_summary(result) is not None:
        # A missing peer can include indented frames from flood_wait.py.
        diagnostic_lines = (line for line in (result.stderr or "").splitlines() if line == line.lstrip())
        if any(_STOP_LOOKUP_RE.search(line) for line in diagnostic_lines):
            pytest.skip("Telegram limited a profile lookup before the test send")
        return None
    match = _RESOLVED_DM_RE.search(result.stdout or "")
    return (int(match[1]), match[2]) if match else None


def _verified_pair(cli_env: CliRealCliEnv, configured_sender: str | None) -> tuple[str, str]:
    if len(cli_env.phones) < 2:
        pytest.skip("`dialogs delete-message` live test requires a second connected account")

    profiles = {phone: _resolve_dm(cli_env, phone, "me") for phone in cli_env.phones}
    senders = (configured_sender,) if configured_sender else cli_env.phones
    for sender_phone in senders:
        sender = profiles[sender_phone]
        if sender is None:
            continue
        for phone, recipient in profiles.items():
            if recipient is None or recipient[0] == sender[0]:
                continue
            uid, username = recipient
            for ref in (username, phone):
                if ref is None:
                    continue
                peer = _resolve_dm(cli_env, sender_phone, ref)
                if peer is not None and peer[0] == uid:
                    return sender_phone, ref

    pytest.skip("No other connected test account resolves to a verified DM peer")


@pytest.mark.timeout(240)
def test_dialogs_delete_second_account_message(run_cli, assert_cli_ok, cli_real_cli_env):
    """Create one DM message to another connected account, then delete only that message."""
    configured_sender = os.environ.get(CLI_REAL_TG_MUTATION_PHONE_ENV)
    if configured_sender and configured_sender not in cli_real_cli_env.phones:
        pytest.skip(f"{CLI_REAL_TG_MUTATION_PHONE_ENV} is not a connected live CLI account")

    sender_phone, recipient = _verified_pair(cli_real_cli_env, configured_sender)

    nonce = make_cli_nonce()
    text = f"codex live cli delete second-account test {nonce}"
    message_id: str | None = None
    verified = False
    deleted = False
    leak_msg: str | None = None

    try:
        sent = run_cli(
            "dialogs",
            "send",
            "--yes",
            "--phone",
            sender_phone,
            recipient,
            text,
            timeout=60,
        )
        assert_cli_ok(sent)
        assert f"Message sent to {recipient}." in sent.stdout, f"unexpected send output: {sent.stdout!r}"
        match = _MESSAGE_ID_RE.search(sent.stdout)
        assert match is not None, f"send stdout did not include message_id: {sent.stdout!r}"
        message_id = match.group(1)

        verdict = cli_verify_message_nonce(
            cli_real_cli_env, phone=sender_phone, chat_ref=recipient, message_id=int(message_id), nonce=nonce
        )
        assert verdict.ok, verdict.reason
        verified = True

        result = run_cli(
            "dialogs",
            "delete-message",
            "--yes",
            "--phone",
            sender_phone,
            recipient,
            message_id,
            timeout=60,
        )
        assert_cli_ok(result)
        assert "Deleted 1 message(s)." in result.stdout, f"unexpected delete output: {result.stdout!r}"
        deleted = True

        after = _capture_cli(
            cli_real_cli_env,
            "messages",
            "read",
            recipient,
            "--live",
            "--phone",
            sender_phone,
            "--offset-id",
            str(int(message_id) + 1),
            "--limit",
            "1",
            timeout=30,
        )
        assert_cli_ok(after)
        remaining = _parse_live_message_blocks(after.stdout)
        assert remaining or "No messages found." in after.stdout.splitlines(), "No readable delete confirmation"
        assert int(message_id) not in remaining, f"Test message #{message_id} is still present after deletion"
    finally:
        if verified and message_id is not None and not deleted:
            leak_msg = cleanup_verified_messages(
                cli_real_cli_env,
                phone=sender_phone,
                chat_ref=recipient,
                candidates=[int(message_id)],
                nonce=nonce,
            )

        if leak_msg:
            print(leak_msg, file=sys.stderr)
            if sys.exc_info()[0] is None:
                pytest.fail(leak_msg)
