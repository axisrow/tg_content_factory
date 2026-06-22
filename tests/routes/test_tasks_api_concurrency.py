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


async def test_concurrent_claims_yield_the_task_to_exactly_one(route_client):
    created = await route_client.post(
        "/api/tasks", json={"type": "dm_reply", "payload": {"peer": "@bob", "text": "hi"}}
    )
    task_id = created.json()["id"]

    # Fire many claims at once; only one may win the single pending task.
    responses = await asyncio.gather(
        *[route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]}) for _ in range(8)]
    )

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
    won_ids = [r.json()["id"] for r in responses if r.status_code == 200]
    assert sorted(won_ids) == sorted(ids)  # each task claimed exactly once
    assert len(won_ids) == len(set(won_ids))


async def test_internal_dispatcher_never_claims_external_task(route_client):
    # An external task created via the API must stay PENDING when the factory's
    # own dispatcher runs its claim over HANDLED_TYPES.
    created = await route_client.post(
        "/api/tasks", json={"type": "chat_answer", "payload": {"chat_id": 1, "text": "x"}}
    )
    task_id = created.json()["id"]

    db = route_client._transport_app.state.db
    claimed = await db.repos.tasks.claim_next_due_generic_task(
        datetime.now(timezone.utc), HANDLED_TYPES
    )
    assert claimed is None

    still = await route_client.get(f"/api/tasks/{task_id}")
    assert still.json()["status"] == "pending"
