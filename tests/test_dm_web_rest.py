"""Web/REST surfaces for the incoming-DM journal and draft approvals (#1579).

Issue scope: repo `list`/`get` read paths for `incoming_dms` (the 24h TTL
journal — the `dm_messages` archive stays out of scope), a web page modeled on
/moderation, and REST read + approve endpoints. The read paths must expose the
ids the `dm drafts` flow needs (incoming_dm_id, phone, chat_id).

All app tests build the FastAPI app over the same `db` (:memory:) instance the
test seeds through (`app.state.db is db`).
"""

from __future__ import annotations

import pytest

from tests.helpers import _record_dm

# === Repo read paths ========================================================


@pytest.mark.anyio
async def test_repo_list_returns_journal_fresh_first_with_draft_flow_ids(db):
    await _record_dm(db, chat_id=42, message_id=7)
    await _record_dm(db, chat_id=43, message_id=8)

    rows = await db.repos.incoming_dms.list()

    assert [r.chat_id for r in rows] == [43, 42]
    fresh = rows[0]
    assert (fresh.phone, fresh.message_id, fresh.processed) == ("+10000000001", 8, False)
    assert all(r.id is not None for r in rows)


@pytest.mark.anyio
async def test_repo_list_respects_ttl_window(db):
    """Журнал = окно TTL 24ч: протухшая запись для читателя не существует."""
    await _record_dm(db, minutes_ago=25 * 60)

    assert await db.repos.incoming_dms.list() == []
