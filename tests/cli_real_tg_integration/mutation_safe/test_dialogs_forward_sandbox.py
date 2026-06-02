from __future__ import annotations

import re
import subprocess
import sys
import uuid

import pytest

from tests.cli_real_tg_integration.conftest import cli_result_failure_summary, cli_run_direct

pytestmark = pytest.mark.real_tg_mutation_safe

_MESSAGE_ID_RE = re.compile(r"\bmessage_id=(\d+)\b")
_FORWARDED_IDS_RE = re.compile(r"\bforwarded_ids=([\d,]+)\b")


@pytest.mark.timeout(180)
def test_dialogs_forward_sandbox(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    chat_ref = live_scratch_message_dialog.chat_ref
    phone = live_scratch_message_dialog.phone
    marker = uuid.uuid4().hex[:12]
    send_text = f"codex live cli forward test {marker}"
    source_message_id: str | None = None
    forwarded_message_ids: list[str] = []
    leak_msg: str | None = None

    try:
        sent = run_cli(
            "dialogs",
            "send",
            "--yes",
            "--phone",
            phone,
            chat_ref,
            send_text,
            timeout=60,
        )
        assert_cli_ok(sent)
        assert f"Message sent to {chat_ref}." in sent.stdout
        match = _MESSAGE_ID_RE.search(sent.stdout)
        assert match is not None, f"send stdout did not include message_id: {sent.stdout!r}"
        source_message_id = match.group(1)

        result = run_cli(
            "dialogs",
            "forward",
            "--yes",
            "--phone",
            phone,
            chat_ref,
            chat_ref,
            source_message_id,
            timeout=60,
        )
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert "Forwarded 1 message(s)" in combined

        fwd_match = _FORWARDED_IDS_RE.search(combined)
        if fwd_match:
            forwarded_message_ids = [mid for mid in fwd_match.group(1).split(",") if mid.strip()]

    finally:
        ids_to_delete = list(forwarded_message_ids)
        if source_message_id is not None:
            ids_to_delete.append(source_message_id)

        if ids_to_delete:
            try:
                cleanup = cli_run_direct(
                    cli_real_cli_env,
                    "dialogs",
                    "delete-message",
                    "--yes",
                    "--phone",
                    phone,
                    chat_ref,
                    *ids_to_delete,
                    timeout=60,
                )
            except subprocess.TimeoutExpired:
                leak_msg = (
                    f"message(s) {ids_to_delete} in {chat_ref} may be left: cleanup timed out"
                )
            else:
                cleanup_failure = cli_result_failure_summary(cleanup)
                if cleanup_failure is not None:
                    leak_msg = (
                        f"message(s) {ids_to_delete} in {chat_ref} may be left: {cleanup_failure}"
                    )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
