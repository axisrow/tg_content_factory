from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.cli_real_tg_integration.conftest import (
    assert_safe_to_delete,
    cli_result_failure_summary,
    cli_run_direct,
    make_cli_nonce,
)
from tests.cli_real_tg_integration.mutation_safe.conftest import make_minimal_png

pytestmark = pytest.mark.real_tg_mutation_safe

_ITEM_ID_RE = re.compile(r"Sent photo item #(\d+)")


def _fetch_telegram_message_ids(db_path: Path, item_id: str) -> list[str]:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT telegram_message_ids FROM photo_batch_items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
    except sqlite3.Error:
        return []
    if row is None or not row[0]:
        return []
    try:
        parsed = json.loads(row[0])
        return [str(mid) for mid in parsed if mid]
    except (json.JSONDecodeError, TypeError):
        return []


@pytest.mark.timeout(150)
def test_photo_loader_send_sandbox(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    phone = live_scratch_message_dialog.phone
    target = live_scratch_message_dialog.chat_ref
    # The caption becomes the media message's text, so a nonce in it lets cleanup
    # verify ownership of each telegram_message_id (looked up from the DB) before
    # deleting — so a stale/wrong item->id mapping can never delete another message.
    nonce = make_cli_nonce()
    caption = f"codex live cli photo send test {nonce}"
    item_id: str | None = None
    telegram_message_ids: list[str] = []
    leak_msg: str | None = None
    tmpdir_obj = tempfile.TemporaryDirectory()

    try:
        png_path = Path(tmpdir_obj.name) / "test_photo.png"
        make_minimal_png(png_path)

        result = run_cli(
            "photo-loader",
            "send",
            "--phone",
            phone,
            "--target",
            target,
            "--files",
            str(png_path),
            "--caption",
            caption,
            timeout=90,
        )
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert "Sent photo item #" in combined

        match = _ITEM_ID_RE.search(combined)
        assert match is not None, f"photo-loader send stdout did not include item id: {combined!r}"
        item_id = match.group(1)

        telegram_message_ids = _fetch_telegram_message_ids(cli_real_cli_env.db_path, item_id)

    finally:
        tmpdir_obj.cleanup()

        if telegram_message_ids:
            verified_ids, verify_leak = assert_safe_to_delete(
                cli_real_cli_env,
                phone=phone,
                chat_ref=target,
                candidates=[int(mid) for mid in telegram_message_ids],
                nonce=nonce,
            )
            if verify_leak and sys.exc_info()[0] is None:
                leak_msg = verify_leak

            if verified_ids:
                try:
                    cleanup = cli_run_direct(
                        cli_real_cli_env,
                        "dialogs",
                        "delete-message",
                        "--yes",
                        "--phone",
                        phone,
                        target,
                        *[str(mid) for mid in verified_ids],
                        timeout=60,
                    )
                except subprocess.TimeoutExpired:
                    if sys.exc_info()[0] is None:
                        leak_msg = (
                            f"photo message(s) {verified_ids} in {target} may be left: cleanup timed out"
                        )
                else:
                    cleanup_failure = cli_result_failure_summary(cleanup)
                    if cleanup_failure is not None and sys.exc_info()[0] is None:
                        leak_msg = (
                            f"photo message(s) {verified_ids} in {target} may be left: {cleanup_failure}"
                        )
        elif item_id is not None and sys.exc_info()[0] is None:
            leak_msg = (
                f"photo item #{item_id} was created but telegram_message_ids not found in DB; "
                "sent photo may not have been cleaned up"
            )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
