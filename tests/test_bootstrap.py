"""Tests for start_container bootstrap behavior."""
from __future__ import annotations

import pytest

from tg_agent.database import Database
from tg_agent.search.engine import SearchEngine


@pytest.mark.anyio
async def test_search_engine_accepts_none_pool(tmp_path):
    """SearchEngine built with pool=None (web-mode) must not raise on check_search_quota."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        engine = SearchEngine(db, pool=None)
        assert engine._telegram._pool is None
        quota = await engine.check_search_quota("test")
        assert quota is None
    finally:
        await db.close()


@pytest.mark.anyio
async def test_persist_incoming_dm_records_journal_entry(db):
    """#1427: персист-колбэк слушателя пишет DM в журнал."""
    from datetime import datetime, timezone

    from tg_agent.runtime.bootstrap import _persist_incoming_dm
    from tg_agent.telegram.dm_listener import IncomingDmEvent

    received = datetime.now(timezone.utc)
    await _persist_incoming_dm(
        db,
        IncomingDmEvent(
            phone="+111",
            chat_id=42,
            message_id=7,
            text="hi",
            message_date=received,
            received_at=received,
        ),
    )

    count = await db.repos.incoming_dms.count_unprocessed()
    assert count == 1


@pytest.mark.anyio
async def test_persist_outgoing_dm_writes_archive_not_journal(db):
    """#1453: исходящее событие — в архив (обе стороны), журнал не трогаем:
    у исходящих нет черновика, processed=0 им не положен по смыслу."""
    from datetime import datetime, timezone

    from tg_agent.runtime.bootstrap import _persist_incoming_dm
    from tg_agent.telegram.dm_listener import IncomingDmEvent

    received = datetime.now(timezone.utc)
    await _persist_incoming_dm(
        db,
        IncomingDmEvent(
            phone="+111",
            chat_id=42,
            message_id=7,
            text="мой ответ",
            message_date=received,
            received_at=received,
            out=True,
        ),
    )

    assert await db.repos.dm_messages.count("+111") == 1
    incoming, outgoing = await db.repos.dm_messages.count_by_direction("+111")
    assert (incoming, outgoing) == (0, 1)
    assert await db.repos.incoming_dms.count_unprocessed() == 0
    cur = await db.db.execute("SELECT COUNT(*) AS n FROM incoming_dms")
    assert (await cur.fetchone())["n"] == 0


@pytest.mark.anyio
async def test_persist_incoming_dm_skips_event_without_ids(db, caplog):
    """Без (chat_id, message_id) запись неидемпотентна — пропуск с предупреждением."""
    from datetime import datetime, timezone

    from tg_agent.runtime.bootstrap import _persist_incoming_dm
    from tg_agent.telegram.dm_listener import IncomingDmEvent

    with caplog.at_level("WARNING", logger="tg_agent.runtime.bootstrap"):
        await _persist_incoming_dm(
            db,
            IncomingDmEvent(
                phone="+111",
                chat_id=None,
                message_id=None,
                text="hi",
                message_date=None,
                received_at=datetime.now(timezone.utc),
            ),
        )

    assert await db.repos.incoming_dms.count_unprocessed() == 0
    assert any("without chat_id" in r.message for r in caplog.records)
