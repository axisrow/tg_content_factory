"""Регресс-гард на naive/aware границу row→model в кэше диалогов (#1291).

`dialog_cache.cached_at` имеет схемный DEFAULT `datetime('now')`, дающий
naive-строку без офсета. `get_cached_at` раньше отдавал её как есть, и
`pool_dialogs._get_db_cached_dialogs` падал на
`datetime.now(timezone.utc) - cached_at` с TypeError.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.database import Database


@pytest.mark.anyio
async def test_get_cached_at_is_utc_aware_for_schema_default_row(tmp_path):
    """Строка, вставленная без явного cached_at, читается как UTC-aware (#1291)."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.execute_write(
            """
            INSERT INTO dialog_cache (phone, dialog_id, title, channel_type)
            VALUES (?, ?, ?, ?)
            """,
            ("+70001", 1001, "Default cached_at", "channel"),
        )

        cached_at = await db.repos.dialog_cache.get_cached_at("+70001")

        assert cached_at is not None
        assert cached_at.tzinfo is not None
        # Арифметика из pool_dialogs._get_db_cached_dialogs не должна падать.
        age_sec = (datetime.now(timezone.utc) - cached_at).total_seconds()
        assert age_sec >= 0
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_cached_at_preserves_aware_value_from_normal_write_path(tmp_path):
    """Обычный путь записи (aware isoformat) читается без сдвига времени (#1291)."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.repos.dialog_cache.replace_dialogs(
            "+70002",
            [{"channel_id": 2002, "title": "Aware", "channel_type": "channel"}],
        )

        cached_at = await db.repos.dialog_cache.get_cached_at("+70002")

        assert cached_at is not None
        assert cached_at.tzinfo is not None
        assert abs((datetime.now(timezone.utc) - cached_at).total_seconds()) < 60
    finally:
        await db.close()


@pytest.mark.anyio
async def test_upsert_dialogs_preserves_previous_snapshot_rows(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.repos.dialog_cache.replace_dialogs(
            "+70003",
            [{"channel_id": 1, "title": "Existing", "channel_type": "channel"}],
        )
        await db.repos.dialog_cache.upsert_dialogs(
            "+70003",
            [{"channel_id": 2, "title": "Reached before timeout", "channel_type": "group"}],
        )

        cached = await db.repos.dialog_cache.list_dialogs("+70003")
        assert {dialog["channel_id"] for dialog in cached} == {1, 2}
    finally:
        await db.close()


@pytest.mark.anyio
async def test_upsert_dialogs_keeps_the_snapshot_stale(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.execute_write(
            """
            INSERT INTO dialog_cache (phone, dialog_id, title, channel_type, cached_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("+70004", 1, "Old", "channel", "2020-01-01T00:00:00+00:00"),
        )
        await db.repos.dialog_cache.upsert_dialogs(
            "+70004",
            [{"channel_id": 2, "title": "Reached", "channel_type": "group"}],
        )

        # A partial walk leaves the snapshot INCOMPLETE, so it must not read as
        # fresh. Previously the prior timestamp was preserved, which was enough
        # only while that predecessor was itself stale; a partial walk over a
        # still-fresh snapshot then inherited its freshness (#1359). The whole
        # phone is now aged unconditionally.
        cached_at = await db.repos.dialog_cache.get_cached_at("+70004")
        assert cached_at is not None
        assert cached_at.year <= 2020
    finally:
        await db.close()


@pytest.mark.anyio
async def test_first_partial_upsert_is_marked_stale(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.repos.dialog_cache.upsert_dialogs(
            "+70005",
            [{"channel_id": 1, "title": "Partial", "channel_type": "channel"}],
        )

        cached_at = await db.repos.dialog_cache.get_cached_at("+70005")
        assert cached_at is not None
        assert cached_at.year == 1970
    finally:
        await db.close()
