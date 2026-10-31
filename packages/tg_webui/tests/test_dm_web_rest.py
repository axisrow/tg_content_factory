"""Web/REST surfaces for the incoming-DM journal and draft approvals (#1579).

Issue scope: repo `list`/`get` read paths for `incoming_dms` (the 24h TTL
journal — the `dm_messages` archive stays out of scope), a web page modeled on
/moderation, and REST read + approve endpoints. The read paths must expose the
ids the `dm drafts` flow needs (incoming_dm_id, phone, chat_id).

All app tests build the FastAPI app over the same `db` (:memory:) instance the
test seeds through (`app.state.db is db`).
"""
# Web-panel tests split out of tests/test_dm_web_rest.py (#1720 wave 3).


from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from webui_helpers import build_web_app, make_auth_client

from tests.helpers import make_test_config
from tg_agent.models import IncomingDm


@pytest.fixture
async def client(db, real_pool_harness_factory, tmp_path):
    harness = real_pool_harness_factory()
    app, _ = await build_web_app(make_test_config(tmp_path), harness, db=db)
    async with make_auth_client(app) as c:
        c._transport_app = app
        yield c


async def _record_dm(
    db, *, chat_id: int = 42, message_id: int = 7, text: str = "привет", minutes_ago: int = 0
) -> None:
    received = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    await db.repos.incoming_dms.record(
        IncomingDm(
            phone="+10000000001",
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            message_date=received,
            received_at=received,
        )
    )


async def _make_draft(db, incoming_dm_id: int) -> int:
    draft_id, _created = await db.repos.dm_reply_drafts.create_draft(
        incoming_dm_id=incoming_dm_id,
        phone="+10000000001",
        chat_id=42,
        reply_text="черновик ответа",
    )
    return draft_id


# === REST read ==============================================================


@pytest.mark.anyio
async def test_api_lists_journal_and_single_entry(client, db):
    await _record_dm(db, chat_id=42, message_id=7, text="первое")
    await _record_dm(db, chat_id=43, message_id=8, text="второе")

    resp = await client.get("/dms/api/incoming")
    assert resp.status_code == 200
    items = resp.json()
    assert [i["chat_id"] for i in items] == [43, 42]
    assert set(items[0]) >= {"id", "phone", "chat_id", "message_id", "text", "processed"}

    dm_id = items[0]["id"]
    one = await client.get(f"/dms/api/incoming/{dm_id}")
    assert one.status_code == 200
    assert one.json()["message_id"] == 8

    missing = await client.get("/dms/api/incoming/99999")
    assert missing.status_code == 404
    assert missing.json() == {"error": "dm_not_found"}


# === REST approve / reject ==================================================


@pytest.mark.anyio
async def test_api_approve_and_reject_roundtrip(client, db):
    await _record_dm(db)
    draft_id = await _make_draft(db, (await db.repos.incoming_dms.list())[0].id)

    rejected = await client.post(f"/dms/api/drafts/{draft_id}/reject")
    assert rejected.status_code == 200
    assert rejected.json() == {"id": draft_id, "status": "rejected", "changed": True}

    # rejected → approved остаётся возможен (владелец передумал), как в decide().
    approved = await client.post(f"/dms/api/drafts/{draft_id}/approve")
    assert approved.status_code == 200
    assert approved.json() == {"id": draft_id, "status": "approved", "changed": True}

    missing = await client.post("/dms/api/drafts/99999/approve")
    assert missing.status_code == 404
    assert missing.json() == {"error": "draft_not_found"}


@pytest.mark.anyio
async def test_api_approve_never_touches_sent_draft(client, db):
    """Апрув после отправки — no-op (двойная отправка исключена)."""
    await _record_dm(db)
    draft_id = await _make_draft(db, (await db.repos.incoming_dms.list())[0].id)
    assert await db.repos.dm_reply_drafts.decide(draft_id, "approved")
    assert await db.repos.dm_reply_drafts.claim_for_send(draft_id)
    assert await db.repos.dm_reply_drafts.mark_sent(draft_id, 555)

    resp = await client.post(f"/dms/api/drafts/{draft_id}/approve")

    assert resp.status_code == 200
    assert resp.json() == {"id": draft_id, "status": "sent", "changed": False}


# === Web page + approve flow ================================================


@pytest.mark.anyio
async def test_page_renders_journal_and_draft_actions(client, db):
    await _record_dm(db, text="привет из журнала")
    draft_id = await _make_draft(db, (await db.repos.incoming_dms.list())[0].id)

    resp = await client.get("/dms")
    assert resp.status_code == 200
    assert "привет из журнала" in resp.text
    assert f'/dms/drafts/{draft_id}/approve' in resp.text
    assert f'/dms/drafts/{draft_id}/reject' in resp.text


@pytest.mark.anyio
async def test_page_empty_state(client, db):
    resp = await client.get("/dms")
    assert resp.status_code == 200
    assert "Журнал пуст" in resp.text
    assert "Черновиков нет" in resp.text


@pytest.mark.anyio
async def test_web_approve_and_reject_update_db(client, db):
    await _record_dm(db)
    draft_id = await _make_draft(db, (await db.repos.incoming_dms.list())[0].id)

    approve = await client.post(
        f"/dms/drafts/{draft_id}/approve", follow_redirects=False
    )
    assert approve.status_code == 303
    assert "msg=draft_approved" in approve.headers["location"]
    assert (await db.repos.dm_reply_drafts.get(draft_id)).status == "approved"

    reject = await client.post(f"/dms/drafts/{draft_id}/reject", follow_redirects=False)
    assert reject.status_code == 303
    assert "msg=draft_rejected" in reject.headers["location"]
    assert (await db.repos.dm_reply_drafts.get(draft_id)).status == "rejected"


@pytest.mark.anyio
async def test_web_approve_unknown_draft_redirects_with_error(client, db):
    resp = await client.post("/dms/drafts/99999/approve", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=draft_not_found" in resp.headers["location"]
