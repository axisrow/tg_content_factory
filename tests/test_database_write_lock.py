"""Tests for Database._write_lock — connection-wide write serialization
that closes the same-connection race window between BEGIN IMMEDIATE
transactions and autocommit writes on a shared aiosqlite connection
(issue #569).
"""

from __future__ import annotations

import asyncio
import time

import pytest

from src.database import Database
from src.models import (
    Channel,
    ContentPipeline,
    PipelineGenerationBackend,
    PipelinePublishMode,
)


def _channel(channel_id: int, title: str = "t") -> Channel:
    return Channel(
        channel_id=channel_id,
        title=title,
        username=f"ch{channel_id}",
        channel_type="channel",
        is_active=True,
    )


async def test_execute_write_acquires_lock(db: Database):
    """Two concurrent execute_write calls must serialize on
    Database._write_lock — their critical sections do not overlap."""
    intervals: list[tuple[float, float]] = []
    original = db._db.execute  # type: ignore[union-attr]

    async def slow_execute(sql, params=()):
        t0 = time.perf_counter()
        await asyncio.sleep(0.02)
        result = await original(sql, params)
        intervals.append((t0, time.perf_counter()))
        return result

    db._db.execute = slow_execute  # type: ignore[union-attr,assignment]
    try:
        await asyncio.gather(
            db.execute_write(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("a", "1"),
            ),
            db.execute_write(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("b", "2"),
            ),
        )
    finally:
        db._db.execute = original  # type: ignore[union-attr,assignment]

    # Two execute calls per execute_write (DML + commit). We care
    # only that the two write critical sections do not overlap.
    # Each "interval" group of 2 belongs to one execute_write.
    assert len(intervals) >= 2
    # The first write must finish before the second starts.
    intervals.sort()
    first_end = intervals[0][1]
    later_starts = [t0 for (t0, _) in intervals[1:]]
    assert all(start >= first_end - 0.001 for start in later_starts), (
        f"execute_write critical sections overlapped: {intervals}"
    )


async def test_transaction_serializes_against_execute_write(db: Database):
    """A transaction() block and a concurrent execute_write must not
    interleave on the same connection. Both writes succeed; neither
    observes the other half-committed."""
    order: list[str] = []

    async def tx_writer():
        async with db.transaction() as conn:
            order.append("tx:start")
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("tx_a", "1"),
            )
            await asyncio.sleep(0.01)
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("tx_b", "2"),
            )
            order.append("tx:end")

    async def autocommit_writer():
        await asyncio.sleep(0.005)  # give tx a head start
        order.append("ac:start")
        await db.execute_write(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("ac", "3"),
        )
        order.append("ac:end")

    await asyncio.gather(tx_writer(), autocommit_writer())

    # transaction must close before autocommit's critical section runs.
    tx_end = order.index("tx:end")
    ac_end = order.index("ac:end")
    # Either tx finished entirely before ac started, or ac started but
    # blocked on the lock — but ac:end must come after tx:end either
    # way, since ac cannot acquire the lock while tx holds it.
    assert ac_end > tx_end, f"autocommit raced inside transaction: {order}"

    # All three rows must exist.
    rows = await db.execute_fetchall(
        "SELECT key FROM settings WHERE key IN ('tx_a', 'tx_b', 'ac')"
    )
    keys = sorted(r["key"] for r in rows)
    assert keys == ["ac", "tx_a", "tx_b"]


async def test_transaction_rollback_releases_lock(db: Database):
    """If a transaction body raises, the lock must be released and
    the connection must not be left in a dirty transaction."""

    class _BoomError(Exception):
        pass

    with pytest.raises(_BoomError):
        async with db.transaction() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("never", "1"),
            )
            raise _BoomError

    assert not db._write_lock.locked(), "write lock leaked after rollback"

    # The aborted insert must be rolled back.
    rows = await db.execute_fetchall(
        "SELECT key FROM settings WHERE key = 'never'"
    )
    assert rows == []

    # A follow-up transaction must succeed cleanly.
    async with db.transaction() as conn:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("after", "1"),
        )
    rows = await db.execute_fetchall(
        "SELECT key FROM settings WHERE key = 'after'"
    )
    assert len(rows) == 1


async def test_delete_channel_serialized_with_pipeline_add(db: Database):
    """Issue #569 stress test: 30 iterations of concurrent
    delete_channel + ContentPipelinesRepository.add referencing the
    same channel. The forbidden state — channel survives while child
    rows are gone — must never appear, regardless of which side wins.
    """
    for i in range(30):
        ch_id = 8000 + i
        pk = await db.repos.channels.add_channel(_channel(channel_id=ch_id))
        # Seed child rows.
        await db._db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (?, ?, ?, ?)",
            (ch_id, 1, "x", "2025-01-01T00:00:00"),
        )
        await db._db.execute(
            "INSERT INTO channel_stats (channel_id, subscriber_count) VALUES (?, ?)",
            (ch_id, 1),
        )
        await db._db.commit()

        pipeline = ContentPipeline(
            name=f"p-{i}",
            prompt_template="t",
            publish_mode=PipelinePublishMode.MODERATED,
            generation_backend=PipelineGenerationBackend.CHAIN,
        )

        async def do_add():
            try:
                await db.repos.content_pipelines.add(
                    pipeline,
                    source_channel_ids=[ch_id],
                    targets=[],
                )
            except Exception as e:
                return e
            return None

        await asyncio.gather(
            db.repos.channels.delete_channel(pk),
            do_add(),
            return_exceptions=True,
        )

        # Check forbidden state.
        ch = await db.repos.channels.get_channel_by_pk(pk)
        channel_alive = ch is not None
        cur = await db._db.execute(
            "SELECT COUNT(*) AS n FROM messages WHERE channel_id = ?",
            (ch_id,),
        )
        msgs = (await cur.fetchone())["n"]
        cur = await db._db.execute(
            "SELECT COUNT(*) AS n FROM channel_stats WHERE channel_id = ?",
            (ch_id,),
        )
        stats = (await cur.fetchone())["n"]
        forbidden = channel_alive and (msgs == 0 or stats == 0)
        assert not forbidden, (
            f"iter={i}: channel survived but children gone: "
            f"alive={channel_alive} msgs={msgs} stats={stats}"
        )


async def test_read_does_not_acquire_lock(db: Database):
    """SELECT must not block while a transaction holds the write
    lock — reads are lock-free."""
    proceed = asyncio.Event()
    inside = asyncio.Event()

    async def long_tx():
        async with db.transaction() as conn:
            inside.set()
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ("lock_check", "1"),
            )
            await proceed.wait()

    tx_task = asyncio.create_task(long_tx())
    try:
        await inside.wait()
        # While the tx holds _write_lock, a SELECT must complete
        # without timing out.
        t0 = time.perf_counter()
        rows = await db.execute_fetchall("SELECT 1 AS v")
        elapsed = time.perf_counter() - t0
        assert rows[0]["v"] == 1
        assert elapsed < 0.5, (
            f"SELECT blocked on write lock for {elapsed:.3f}s — "
            "reads must be lock-free"
        )
    finally:
        proceed.set()
        await tx_task
