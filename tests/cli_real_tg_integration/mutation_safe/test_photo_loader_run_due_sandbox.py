from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.cli_real_tg_integration.conftest import (
    cleanup_verified_messages,
    make_cli_nonce,
    resolve_saved_messages_dialog_id,
)
from tests.cli_real_tg_integration.mutation_safe.conftest import make_minimal_png

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(150)
def test_photo_loader_run_due_sandbox(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    phone = live_scratch_message_dialog.phone
    saved_dialog_id = resolve_saved_messages_dialog_id(cli_real_cli_env, phone=phone)
    if saved_dialog_id is None:
        pytest.skip("could not resolve Saved Messages dialog id for photo-loader run-due sandbox")

    nonce = make_cli_nonce()
    caption = f"codex live cli photo run-due test {nonce}"
    batch_id: int | None = None
    item_id: int | None = None
    telegram_message_ids: list[int] = []
    leak_msg: str | None = None
    tmpdir_obj = tempfile.TemporaryDirectory()

    try:
        png_path = Path(tmpdir_obj.name) / "due_photo.png"
        make_minimal_png(png_path)
        batch_id, item_id = _create_due_photo_item(
            cli_real_cli_env.db_path,
            phone=phone,
            dialog_id=saved_dialog_id,
            png_path=png_path,
            caption=caption,
        )

        result = run_cli("photo-loader", "run-due", "--item-id", str(item_id), timeout=120)
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert "Processed due photo items=1 auto_jobs=0" in combined

        telegram_message_ids = _fetch_telegram_message_ids(cli_real_cli_env.db_path, item_id)
        assert telegram_message_ids, f"due photo item #{item_id} did not record telegram_message_ids"
    finally:
        if item_id is not None and not telegram_message_ids:
            telegram_message_ids = _fetch_telegram_message_ids(cli_real_cli_env.db_path, item_id)

        if telegram_message_ids:
            current_leak = cleanup_verified_messages(
                cli_real_cli_env,
                phone=phone,
                chat_ref="me",
                candidates=telegram_message_ids,
                nonce=nonce,
            )
            if current_leak and sys.exc_info()[0] is None:
                leak_msg = current_leak
        elif item_id is not None and sys.exc_info()[0] is None:
            leak_msg = (
                f"photo item #{item_id} was run but telegram_message_ids not found in DB; "
                "sent photo may not have been cleaned up"
            )

        _delete_due_photo_rows(cli_real_cli_env.db_path, batch_id=batch_id, item_id=item_id)
        tmpdir_obj.cleanup()

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg, pytrace=False)


def _create_due_photo_item(
    db_path: Path,
    *,
    phone: str,
    dialog_id: int,
    png_path: Path,
    caption: str,
) -> tuple[int, int]:
    due_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.execute(
            """
            INSERT INTO photo_batches (
                phone, target_dialog_id, target_title, target_type,
                send_mode, caption, status
            )
            VALUES (?, ?, 'Saved Messages', 'saved', 'separate', ?, 'pending')
            """,
            (phone, int(dialog_id), caption),
        )
        batch_id = int(cur.lastrowid or 0)
        cur = conn.execute(
            """
            INSERT INTO photo_batch_items (
                batch_id, phone, target_dialog_id, target_title, target_type,
                file_paths, send_mode, caption, schedule_at, status, telegram_message_ids
            )
            VALUES (?, ?, ?, 'Saved Messages', 'saved', ?, 'separate', ?, ?, 'pending', ?)
            """,
            (
                batch_id,
                phone,
                int(dialog_id),
                json.dumps([str(png_path)]),
                caption,
                due_at,
                json.dumps([]),
            ),
        )
        item_id = int(cur.lastrowid or 0)
    return batch_id, item_id


def _fetch_telegram_message_ids(db_path: Path, item_id: int) -> list[int]:
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
    except (json.JSONDecodeError, TypeError):
        return []
    return [int(mid) for mid in parsed if mid]


def _delete_due_photo_rows(
    db_path: Path,
    *,
    batch_id: int | None,
    item_id: int | None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        if item_id is not None:
            conn.execute("DELETE FROM photo_batch_items WHERE id = ?", (int(item_id),))
        if batch_id is not None:
            conn.execute(
                "DELETE FROM photo_batches WHERE id = ? AND target_type = 'saved'",
                (int(batch_id),),
            )
