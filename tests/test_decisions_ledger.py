"""Every human provenance write leaves exactly one decisions journal row (#1316).

The decisions table is the audit trail the provenance principle (#1307)
relies on: an operator action (``origin='human'``) must record what changed —
old and new value — so that later suppression of automation is explainable.
If a human write stops journaling, these tests go red.
"""

from __future__ import annotations

from tg_agent.models import Channel


async def test_decisions_ledger_records_human_write(db):
    # -- channels.is_active ---------------------------------------------------
    pk = await db.add_channel(Channel(channel_id=-100416, title="Ledger Active"))
    assert await db.set_channel_active(pk, False, origin="human", actor="cli") == 1

    rows = await db.repos.decisions.history("channel", -100416, field="is_active")
    assert len(rows) == 1
    assert (rows[0].old_value, rows[0].new_value) == ("1", "0")
    assert (rows[0].origin, rows[0].actor) == ("human", "cli")

    # -- channels.is_filtered -------------------------------------------------
    assert await db.repos.channels.set_channel_filtered(pk, True, origin="human", actor="cli") == 1

    rows = await db.repos.decisions.history("channel", -100416, field="is_filtered")
    assert len(rows) == 1
    assert (rows[0].old_value, rows[0].new_value) == ("0", "1")
    assert rows[0].origin == "human"

    # -- generation_runs.moderation_status ------------------------------------
    run_id = await db.repos.generation_runs.create_run(None, "prompt")
    assert (
        await db.repos.generation_runs.set_moderation_status(
            run_id, "rejected", origin="human", actor="cli"
        )
        == 1
    )

    rows = await db.repos.decisions.history("generation_run", run_id, field="moderation_status")
    assert len(rows) == 1
    assert (rows[0].old_value, rows[0].new_value) == ("pending", "rejected")
    assert (rows[0].origin, rows[0].actor) == ("human", "cli")
