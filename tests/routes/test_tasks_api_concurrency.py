"""Interop task API claim-race + dispatcher-gating tests (#962).

Complements tests/routes/test_tasks_api.py (lifecycle/gating) with the
concurrency invariant: a task is claimed by exactly one caller, and the
factory's own dispatcher never claims an external interop task.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.services.unified_dispatcher import HANDLED_TYPES

pytestmark = pytest.mark.anyio


async def test_concurrent_creates_with_one_key_store_one_task(route_client):
    responses = await asyncio.gather(
        *[
            route_client.post("/api/tasks", json={"type": "dm_reply", "idempotency_key": "send-1"})
            for _ in range(8)
        ]
    )
    assert all(response.status_code == 201 for response in responses)
    assert len({response.json()["id"] for response in responses}) == 1
    db = route_client._transport_app.state.db
    assert await db.repos.tasks.count_collection_tasks() == 1


@pytest.mark.parametrize(
    "endpoint,body", [("complete", {"result_payload": {"id": 1}}), ("fail", {"error": "boom"})]
)
async def test_concurrent_identical_reports_all_succeed(route_client, endpoint, body):
    created = await route_client.post("/api/tasks", json={"type": "dm_reply"})
    task_id = created.json()["id"]
    await route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]})
    responses = await asyncio.gather(
        *[route_client.post(f"/api/tasks/{task_id}/{endpoint}", json=body) for _ in range(8)]
    )
    assert all(response.status_code == 200 for response in responses)
    assert all(response.json() == {"ok": True} for response in responses)


async def test_concurrent_complete_and_fail_accept_only_one_outcome(route_client):
    created = await route_client.post("/api/tasks", json={"type": "dm_reply"})
    task_id = created.json()["id"]
    await route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]})
    reports = [("complete", {"result_payload": {"id": 1}}), ("fail", {"error": "boom"})]
    responses = await asyncio.gather(
        *[route_client.post(f"/api/tasks/{task_id}/{endpoint}", json=body) for endpoint, body in reports]
    )
    assert sorted(response.status_code for response in responses) == [200, 409]
    original = (await route_client.get(f"/api/tasks/{task_id}")).json()
    for (endpoint, body), response in zip(reports, responses, strict=True):
        replay = await route_client.post(f"/api/tasks/{task_id}/{endpoint}", json=body)
        assert replay.status_code == response.status_code
    assert (await route_client.get(f"/api/tasks/{task_id}")).json() == original


async def test_concurrent_claims_yield_the_task_to_exactly_one(route_client):
    created = await route_client.post(
        "/api/tasks", json={"type": "dm_reply", "payload": {"peer": "@bob", "text": "hi"}}
    )
    task_id = created.json()["id"]

    # Fire many claims at once; only one may win the single pending task.
    responses = await asyncio.gather(
        *[route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]}) for _ in range(8)]
    )

    unexpected = [r for r in responses if r.status_code not in (200, 204)]
    assert not unexpected, f"unexpected statuses: {[r.status_code for r in unexpected]}"
    won = [r for r in responses if r.status_code == 200]
    empty = [r for r in responses if r.status_code == 204]
    assert len(won) == 1, f"expected exactly one winner, got {len(won)}"
    assert len(empty) == 7
    assert won[0].json()["id"] == task_id
    assert won[0].json()["status"] == "running"


async def test_claim_distributes_distinct_tasks(route_client):
    # Two pending tasks, many concurrent claimers → two distinct winners, no
    # task handed out twice.
    ids = set()
    for _ in range(2):
        r = await route_client.post("/api/tasks", json={"type": "fetch_dialogs", "payload": {}})
        ids.add(r.json()["id"])

    responses = await asyncio.gather(
        *[route_client.post("/api/tasks/claim", json={"types": ["fetch_dialogs"]}) for _ in range(6)]
    )
    unexpected = [r for r in responses if r.status_code not in (200, 204)]
    assert not unexpected, f"unexpected statuses: {[r.status_code for r in unexpected]}"
    won_ids = [r.json()["id"] for r in responses if r.status_code == 200]
    assert sorted(won_ids) == sorted(ids)  # each task claimed exactly once
    assert len(won_ids) == len(set(won_ids))


async def test_internal_dispatcher_never_claims_external_task(route_client):
    # The invariant: interop types are absent from the dispatcher's HANDLED_TYPES,
    # so the factory's own claim never picks them up.
    assert "chat_answer" not in HANDLED_TYPES

    db = route_client._transport_app.state.db
    # An internal task IS claimable over HANDLED_TYPES — proves the assertion below
    # is non-vacuous (the claim path works, it just excludes interop types).
    internal_id = await db.repos.tasks.create_generic_task(
        "stats_all", payload={"task_kind": "stats_all", "channel_ids": [1]}
    )
    created = await route_client.post(
        "/api/tasks", json={"type": "chat_answer", "payload": {"chat_id": 1, "text": "x"}}
    )
    external_id = created.json()["id"]

    now = datetime.now(timezone.utc)
    claimed_internal = await db.repos.tasks.claim_next_due_generic_task(now, HANDLED_TYPES)
    assert claimed_internal is not None and claimed_internal.id == internal_id
    # No external task is ever returned, even after the internal one is claimed.
    assert await db.repos.tasks.claim_next_due_generic_task(now, HANDLED_TYPES) is None

    still = await route_client.get(f"/api/tasks/{external_id}")
    assert still.json()["status"] == "pending"
