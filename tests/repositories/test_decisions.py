"""Tests for the provenance decision journal (#1309)."""

from __future__ import annotations

import pytest


@pytest.mark.anyio
async def test_record_history_and_last_human_decision(db):
    repo = db.repos.decisions

    await repo.record(
        entity="channel",
        entity_key=123,
        entity_name="Example",
        field="is_filtered",
        old_value="0",
        new_value="1",
        origin="auto",
        actor="analyzer",
        reason="low uniqueness",
    )
    human_id = await repo.record(
        entity="channel",
        entity_key=123,
        entity_name="Example",
        field="is_filtered",
        old_value="1",
        new_value="0",
        origin="human",
        actor="web",
        reason="reviewed manually",
    )

    history = await repo.history("channel", 123, field="is_filtered")
    assert [decision.id for decision in history] == [human_id, human_id - 1]
    assert history[0].origin == "human"
    assert history[0].created_at is not None
    last_human = await repo.last_human_decision("channel", 123, "is_filtered")
    assert last_human is not None
    assert last_human.id == human_id
    assert last_human.reason == "reviewed manually"


@pytest.mark.anyio
async def test_record_can_join_callers_transaction(db):
    repo = db.repos.decisions

    async with db.transaction() as conn:
        decision_id = await repo.record(
            entity="channel",
            entity_key=456,
            field="is_active",
            new_value="enabled",
            origin="human",
            actor="cli",
            commit=False,
        )
        cur = await conn.execute("SELECT id FROM decisions WHERE id = ?", (decision_id,))
        assert await cur.fetchone() is not None

    history = await repo.history("channel", 456)
    assert len(history) == 1
    assert history[0].id == decision_id
    cur = await db.execute_fetchall("SELECT id FROM decisions WHERE id = ?", (decision_id,))
    assert len(cur) == 1


@pytest.mark.anyio
async def test_provenance_columns_have_noop_defaults(db):
    await db.execute_write(
        "INSERT INTO channels (channel_id, title) VALUES (?, ?)",
        (123, "Example"),
    )
    cur = await db.execute_fetchall(
        "SELECT active_origin, filtered_origin, approval_state FROM channels WHERE channel_id = ?",
        (123,),
    )
    assert [tuple(row) for row in cur] == [("auto", "auto", "approved")]
