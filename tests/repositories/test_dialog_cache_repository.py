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
