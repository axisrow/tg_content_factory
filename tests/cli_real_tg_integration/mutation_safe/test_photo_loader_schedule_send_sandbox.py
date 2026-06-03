from __future__ import annotations

import re
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.cli_real_tg_integration.conftest import cli_result_failure_summary, cli_run_direct
from tests.cli_real_tg_integration.mutation_safe.conftest import make_minimal_png

pytestmark = pytest.mark.real_tg_mutation_safe

_ITEM_ID_RE = re.compile(r"Scheduled photo item #(\d+)")


def _fetch_item_status(db_path: Path, item_id: str) -> str | None:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT status FROM photo_batch_items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
    except sqlite3.Error:
        return None
    return str(row[0]) if row else None


@pytest.mark.timeout(150)
def test_photo_loader_schedule_send_sandbox(run_cli, assert_cli_ok, cli_real_cli_env, live_scratch_message_dialog):
    phone = live_scratch_message_dialog.phone
    target = live_scratch_message_dialog.chat_ref
    # Schedule far enough in the future so run-due does not fire during the test.
    future_at = (datetime.now(timezone.utc) + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    item_id: str | None = None
    leak_msg: str | None = None
    tmpdir_obj = tempfile.TemporaryDirectory()

    try:
        png_path = Path(tmpdir_obj.name) / "test_sched.png"
        make_minimal_png(png_path)

        result = run_cli(
            "photo-loader",
            "schedule-send",
            "--phone",
            phone,
            "--target",
            target,
            "--files",
            str(png_path),
            "--at",
            future_at,
            "--caption",
            "codex live cli schedule-send test",
            timeout=90,
        )
        assert_cli_ok(result)
        combined = f"{result.stdout}\n{result.stderr}"
        assert "Scheduled photo item #" in combined

        match = _ITEM_ID_RE.search(combined)
        assert match is not None, f"schedule-send stdout did not include item id: {combined!r}"
        item_id = match.group(1)

        # Verify the DB row was created with the scheduled status (schedule_send
        # creates the batch with PhotoBatchStatus.SCHEDULED; run-due flips it to
        # running/completed later, which we avoid by scheduling 24h out).
        status = _fetch_item_status(cli_real_cli_env.db_path, item_id)
        assert status == "scheduled", f"expected scheduled status for scheduled item #{item_id}, got {status!r}"

    finally:
        tmpdir_obj.cleanup()

        if item_id is not None:
            try:
                cleanup = cli_run_direct(
                    cli_real_cli_env,
                    "photo-loader",
                    "batch-cancel",
                    item_id,
                    timeout=30,
                )
            except subprocess.TimeoutExpired:
                leak_msg = f"scheduled photo item #{item_id} may be left pending: cleanup timed out"
            else:
                cleanup_failure = cli_result_failure_summary(cleanup)
                if cleanup_failure is not None:
                    leak_msg = (
                        f"scheduled photo item #{item_id} may be left pending: {cleanup_failure}"
                    )

        if leak_msg and sys.exc_info()[0] is None:
            pytest.fail(leak_msg)
