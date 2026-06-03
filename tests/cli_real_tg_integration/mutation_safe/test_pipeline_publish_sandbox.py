from __future__ import annotations

import re
import sqlite3
import sys

import pytest

from tests.cli_real_tg_integration.conftest import (
    cleanup_verified_messages,
    make_cli_nonce,
    resolve_saved_messages_dialog_id,
)

pytestmark = pytest.mark.real_tg_mutation_safe

_PUBLISHED_MSG_RE = re.compile(r"published_message_id=(\d+)\s+phone=(\S+)\s+dialog_id=(-?\d+)")


def _create_saved_messages_publish_run(
    db_path,
    *,
    phone: str,
    dialog_id: int,
    nonce: str,
) -> tuple[int, int, str]:
    generated_text = f"codex live cli pipeline publish test {nonce}"
    pipeline_name = f"codex-live-publish-sandbox-{nonce}"
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.execute(
            """
            INSERT INTO content_pipelines (
                name, prompt_template, publish_mode, generation_backend,
                is_active, generate_interval_minutes, account_phone
            )
            VALUES (?, ?, 'moderated', 'chain', 0, 60, ?)
            """,
            (pipeline_name, "sandbox", phone),
        )
        pipeline_id = int(cur.lastrowid or 0)
        conn.execute(
            """
            INSERT INTO pipeline_targets (
                pipeline_id, phone, target_dialog_id, target_title, target_type
            )
            VALUES (?, ?, ?, 'Saved Messages', 'saved')
            """,
            (pipeline_id, phone, int(dialog_id)),
        )
        cur = conn.execute(
            """
            INSERT INTO generation_runs (
                pipeline_id, status, prompt, generated_text, metadata,
                moderation_status, created_at, updated_at
            )
            VALUES (?, 'completed', 'sandbox', ?, '{}', 'approved', datetime('now'), datetime('now'))
            """,
            (pipeline_id, generated_text),
        )
        run_id = int(cur.lastrowid or 0)
    return pipeline_id, run_id, generated_text


def _delete_saved_messages_publish_run(db_path, *, pipeline_id: int, run_id: int) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM generation_runs WHERE id = ?", (int(run_id),))
        conn.execute("DELETE FROM pipeline_targets WHERE pipeline_id = ?", (int(pipeline_id),))
        conn.execute(
            "DELETE FROM content_pipelines WHERE id = ? AND name LIKE 'codex-live-publish-sandbox-%'",
            (int(pipeline_id),),
        )


@pytest.mark.timeout(180)
def test_pipeline_publish_sandbox(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    phone = live_scratch_message_dialog.phone
    saved_dialog_id = resolve_saved_messages_dialog_id(cli_real_cli_env, phone=phone)
    if saved_dialog_id is None:
        pytest.skip("could not resolve Saved Messages dialog id for pipeline publish sandbox")

    nonce = make_cli_nonce()
    pipeline_id, run_id, generated_text = _create_saved_messages_publish_run(
        cli_real_cli_env.db_path,
        phone=phone,
        dialog_id=saved_dialog_id,
        nonce=nonce,
    )
    published_entries: list[tuple[str, str, str]] = []
    leak_msg: str | None = None

    try:
        result = run_cli("pipeline", "publish", str(run_id), timeout=120)
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert f"Published run id={run_id}" in combined

        for match in _PUBLISHED_MSG_RE.finditer(combined):
            published_entries.append((match.group(1), match.group(2), match.group(3)))
        assert published_entries, f"publish stdout did not include published message ids: {combined!r}"
    finally:
        for message_id, published_phone, _dialog_id in published_entries:
            current_leak = cleanup_verified_messages(
                cli_real_cli_env,
                phone=published_phone,
                chat_ref="me",
                candidates=[int(message_id)],
                nonce=generated_text,
            )
            if current_leak and sys.exc_info()[0] is None:
                leak_msg = current_leak

        _delete_saved_messages_publish_run(
            cli_real_cli_env.db_path,
            pipeline_id=pipeline_id,
            run_id=run_id,
        )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg, pytrace=False)
