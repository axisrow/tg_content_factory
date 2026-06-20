"""Integration tests for the offline export orchestration (issue #834)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import Channel, Message
from src.services.export_service import (
    default_export_dir,
    gather_channel_messages,
    resolve_html_page_size,
    resolve_max_file_size_mb,
    run_offline_export,
)


async def _seed(db, channel_id=555, message_ids=(1, 2, 3)):
    await db.repos.channels.add_channel(
        Channel(channel_id=channel_id, title="My Chan", username="mychan", channel_type="channel")
    )
    for mid in message_ids:
        await db.repos.messages.insert_message(
            Message(
                channel_id=channel_id,
                message_id=mid,
                text=f"msg {mid}",
                date=datetime(2026, 6, 12, 15, 0, 0, tzinfo=timezone.utc),
            )
        )


async def test_run_offline_export_writes_full_tree(db, tmp_path):
    await _seed(db)
    summary = await run_offline_export(db, 555, fmt="both", out_dir=tmp_path)
    assert summary is not None
    assert summary.message_count == 3
    names = {p.name for p in tmp_path.iterdir()}
    assert {"result.json", "messages.html", "export_manifest.json"} <= names


async def test_run_offline_export_default_json_only(db, tmp_path):
    await _seed(db)
    await run_offline_export(db, 555, fmt="json", out_dir=tmp_path)
    names = {p.name for p in tmp_path.iterdir()}
    assert "result.json" in names
    assert "messages.html" not in names


async def test_run_offline_export_unknown_channel(db, tmp_path):
    assert await run_offline_export(db, 999, out_dir=tmp_path) is None


async def test_run_offline_export_channel_without_messages(db, tmp_path):
    await db.repos.channels.add_channel(Channel(channel_id=777, title="Empty"))
    assert await run_offline_export(db, 777, out_dir=tmp_path) is None


async def test_gather_messages_sorted_by_message_id(db):
    await _seed(db, channel_id=42, message_ids=(5, 1, 3, 2, 4))
    msgs = await gather_channel_messages(db, 42, limit=10)
    assert [m.message_id for m in msgs] == [1, 2, 3, 4, 5]


async def test_gather_messages_respects_limit(db):
    await _seed(db, channel_id=43, message_ids=tuple(range(1, 20)))
    msgs = await gather_channel_messages(db, 43, limit=5)
    assert len(msgs) == 5


async def test_resolve_max_file_size_mb_override_and_setting(db):
    assert await resolve_max_file_size_mb(db, 7) == 7  # explicit override wins
    assert await resolve_max_file_size_mb(db, None) == 3  # default when unset
    await db.set_setting("export_max_file_size_mb", "12")
    assert await resolve_max_file_size_mb(db, None) == 12
    await db.set_setting("export_max_file_size_mb", "garbage")
    assert await resolve_max_file_size_mb(db, None) == 3  # bad value falls back


async def test_resolve_html_page_size_setting(db):
    assert await resolve_html_page_size(db) == 1000
    await db.set_setting("export_html_page_size", "50")
    assert await resolve_html_page_size(db) == 50


@pytest.mark.parametrize("channel_id", [100, -1001234567890])
def test_default_export_dir_naming(channel_id):
    now = datetime(2026, 6, 12, tzinfo=timezone.utc)
    path = default_export_dir(channel_id, now=now)
    assert path.name == f"ChatExport_2026-06-12_{channel_id}"
    assert path.parent.name == "exports"
