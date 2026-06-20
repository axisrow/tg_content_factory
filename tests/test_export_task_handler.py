"""Worker EXPORT task handler + payload wiring (issue #834, PR-3)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from src.models import (
    Channel,
    CollectionTaskStatus,
    CollectionTaskType,
    ExportTaskPayload,
    Message,
)
from src.services.task_handlers.base import TaskHandlerContext
from src.services.task_handlers.export import ExportTaskHandler


async def _seed(db, channel_id=900, *, with_media_msg=False, **channel_kw):
    await db.repos.channels.add_channel(
        Channel(channel_id=channel_id, title="Chan", username="chan", channel_type="channel", **channel_kw)
    )
    for mid in (1, 2):
        await db.repos.messages.insert_message(
            Message(channel_id=channel_id, message_id=mid, text=f"m{mid}",
                    date=datetime(2026, 6, 12, 12, mid, tzinfo=timezone.utc))
        )
    if with_media_msg:
        await db.repos.messages.insert_message(
            Message(channel_id=channel_id, message_id=3, text="pic", media_type="photo",
                    date=datetime(2026, 6, 12, 12, 3, tzinfo=timezone.utc))
        )


def _context(db, *, client_pool=None) -> TaskHandlerContext:
    return TaskHandlerContext(
        collector=MagicMock(),
        channel_bundle=MagicMock(),
        tasks=db.repos.tasks,
        stop_event=asyncio.Event(),
        db=db,
        client_pool=client_pool,
    )


async def _make_task(db, payload: ExportTaskPayload):
    task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.EXPORT, title="export", payload=payload
    )
    return await db.repos.tasks.get_collection_task(task_id)


# ── payload wiring ─────────────────────────────────────────────────────────


async def test_export_payload_roundtrips_through_repo(db):
    payload = ExportTaskPayload(channel_id=42, fmt="both", with_media=True, max_file_size_mb=7, limit=99)
    task = await _make_task(db, payload)
    assert isinstance(task.payload, ExportTaskPayload)
    assert task.payload.channel_id == 42
    assert task.payload.fmt == "both"
    assert task.payload.with_media is True
    assert task.payload.max_file_size_mb == 7
    assert task.payload.limit == 99


# ── handler ────────────────────────────────────────────────────────────────


async def test_handle_offline_writes_tree_and_completes(db, tmp_path):
    await _seed(db, 900)
    task = await _make_task(db, ExportTaskPayload(channel_id=900, fmt="both", out_dir=str(tmp_path)))
    await ExportTaskHandler(_context(db)).handle(task)

    refreshed = await db.repos.tasks.get_collection_task(task.id)
    assert refreshed.status == CollectionTaskStatus.COMPLETED
    names = {p.name for p in tmp_path.iterdir()}
    assert {"result.json", "messages.html", "export_manifest.json"} <= names


async def test_handle_invalid_payload_fails(db):
    # A non-export payload (plain dict) under an EXPORT task → FAILED.
    task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.EXPORT, title="bad", payload={"task_kind": "nope"}
    )
    task = await db.repos.tasks.get_collection_task(task_id)
    await ExportTaskHandler(_context(db)).handle(task)
    refreshed = await db.repos.tasks.get_collection_task(task_id)
    assert refreshed.status == CollectionTaskStatus.FAILED


async def test_handle_unknown_channel_completes_with_note(db, tmp_path):
    task = await _make_task(db, ExportTaskPayload(channel_id=12345, out_dir=str(tmp_path)))
    await ExportTaskHandler(_context(db)).handle(task)
    refreshed = await db.repos.tasks.get_collection_task(task.id)
    assert refreshed.status == CollectionTaskStatus.COMPLETED
    assert "not found" in (refreshed.note or "")


async def test_handle_with_media_no_pool_falls_back_offline(db, tmp_path):
    await _seed(db, 901, with_media_msg=True)
    task = await _make_task(db, ExportTaskPayload(channel_id=901, with_media=True, out_dir=str(tmp_path)))
    await ExportTaskHandler(_context(db, client_pool=None)).handle(task)
    refreshed = await db.repos.tasks.get_collection_task(task.id)
    assert refreshed.status == CollectionTaskStatus.COMPLETED
    data = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))
    media_msg = next(m for m in data["messages"] if m["id"] == 3)
    # No account → media represented as "not included", not a broken link.
    assert "not included" in media_msg["photo"].lower()


async def test_handle_with_media_downloads_via_action_service(db, tmp_path, monkeypatch):
    await _seed(db, 902, with_media_msg=True)

    from src.services.telegram_actions import MediaDownloadOutcome

    class FakeActionService:
        def __init__(self, pool):
            self._pool = pool

        async def download_media_sized(self, *, phone, chat_id, message_id, output_dir, max_size_bytes):
            return MediaDownloadOutcome(
                phone=phone, kind="photo", subdir="photos",
                path=str(Path(output_dir) / "photos" / f"{message_id}.jpg"),
                rel_path=f"photos/{message_id}.jpg", size_bytes=100,
            )

    monkeypatch.setattr("src.services.telegram_actions.TelegramActionService", FakeActionService)
    pool = MagicMock()
    pool.clients = {"+15550001111": object()}

    task = await _make_task(db, ExportTaskPayload(channel_id=902, with_media=True, out_dir=str(tmp_path)))
    await ExportTaskHandler(_context(db, client_pool=pool)).handle(task)

    refreshed = await db.repos.tasks.get_collection_task(task.id)
    assert refreshed.status == CollectionTaskStatus.COMPLETED
    data = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))
    media_msg = next(m for m in data["messages"] if m["id"] == 3)
    assert media_msg["photo"] == "photos/3.jpg"  # downloaded, linked relative path


async def test_handle_with_media_skips_oversized_and_records(db, tmp_path, monkeypatch):
    await _seed(db, 903, with_media_msg=True)

    from src.services.telegram_actions import MediaDownloadOutcome

    class FakeActionService:
        def __init__(self, pool):
            pass

        async def download_media_sized(self, *, phone, chat_id, message_id, output_dir, max_size_bytes):
            return MediaDownloadOutcome(
                phone=phone, kind="photo", subdir="photos",
                size_bytes=9_000_000, skipped=True, reason="exceeds_max_size",
            )

    monkeypatch.setattr("src.services.telegram_actions.TelegramActionService", FakeActionService)
    pool = MagicMock()
    pool.clients = {"+15550001111": object()}

    task = await _make_task(
        db, ExportTaskPayload(channel_id=903, with_media=True, max_file_size_mb=3, out_dir=str(tmp_path))
    )
    await ExportTaskHandler(_context(db, client_pool=pool)).handle(task)

    manifest = json.loads((tmp_path / "export_manifest.json").read_text(encoding="utf-8"))
    assert manifest["media_skipped"] == 1
    entry = manifest["skipped_files"][0]
    assert entry["reason"] == "exceeds_max_size"
    assert entry["original_size_bytes"] == 9_000_000
    data = json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))
    media_msg = next(m for m in data["messages"] if m["id"] == 3)
    assert "exceeds maximum size" in media_msg["photo"].lower()
