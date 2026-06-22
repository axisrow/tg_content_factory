"""Interop task REST API route tests (#961). Core happy-path + type gating;
the full claim-race / concurrency suite lives in #962."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.anyio


async def test_create_returns_id_and_is_fetchable(route_client):
    resp = await route_client.post(
        "/api/tasks", json={"type": "dm_reply", "payload": {"peer": "@bob", "text": "hi"}}
    )
    assert resp.status_code == 201
    task_id = resp.json()["id"]
    assert task_id > 0

    got = await route_client.get(f"/api/tasks/{task_id}")
    assert got.status_code == 200
    body = got.json()
    assert body["task_type"] == "dm_reply"
    assert body["status"] == "pending"
    assert body["payload"]["text"] == "hi"


async def test_create_rejects_internal_type(route_client):
    resp = await route_client.post("/api/tasks", json={"type": "channel_collect", "payload": {}})
    assert resp.status_code == 403


async def test_get_missing_returns_404(route_client):
    resp = await route_client.get("/api/tasks/999999")
    assert resp.status_code == 404


async def test_claim_returns_204_when_empty(route_client):
    resp = await route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]})
    assert resp.status_code == 204


async def test_claim_rejects_internal_type(route_client):
    resp = await route_client.post("/api/tasks/claim", json={"types": ["stats_all"]})
    assert resp.status_code == 403


async def test_full_lifecycle_complete(route_client):
    created = await route_client.post(
        "/api/tasks", json={"type": "fetch_dialogs", "payload": {"limit": 10}}
    )
    task_id = created.json()["id"]

    claimed = await route_client.post("/api/tasks/claim", json={"types": ["fetch_dialogs"]})
    assert claimed.status_code == 200
    assert claimed.json()["id"] == task_id
    assert claimed.json()["status"] == "running"

    done = await route_client.post(
        f"/api/tasks/{task_id}/complete", json={"result_payload": {"dialogs": [1, 2, 3]}}
    )
    assert done.status_code == 200

    final = await route_client.get(f"/api/tasks/{task_id}")
    assert final.json()["status"] == "completed"
    assert final.json()["result_payload"] == {"dialogs": [1, 2, 3]}


async def test_full_lifecycle_fail(route_client):
    created = await route_client.post(
        "/api/tasks", json={"type": "chat_answer", "payload": {"chat_id": 1, "text": "x"}}
    )
    task_id = created.json()["id"]

    # Must be claimed (→ RUNNING) before it can be failed (#961 review).
    await route_client.post("/api/tasks/claim", json={"types": ["chat_answer"]})
    failed = await route_client.post(f"/api/tasks/{task_id}/fail", json={"error": "boom"})
    assert failed.status_code == 200

    final = await route_client.get(f"/api/tasks/{task_id}")
    assert final.json()["status"] == "failed"
    assert final.json()["error"] == "boom"


async def test_complete_missing_returns_404(route_client):
    resp = await route_client.post("/api/tasks/999999/complete", json={"result_payload": {}})
    assert resp.status_code == 404


async def test_complete_requires_running_status(route_client):
    # A freshly-created (PENDING, unclaimed) task cannot be completed — guards
    # against skipping the atomic claim / replay-completing (#961 review).
    created = await route_client.post(
        "/api/tasks", json={"type": "fetch_dialogs", "payload": {}}
    )
    task_id = created.json()["id"]
    resp = await route_client.post(f"/api/tasks/{task_id}/complete", json={"result_payload": {}})
    assert resp.status_code == 409


async def test_internal_task_not_accessible_via_interop_api(route_client):
    # An internal task (created directly) must be invisible to the external API:
    # get/complete/fail all 403, so a worker with WEB_PASS can't read or poison it.
    db = route_client._transport_app.state.db
    internal_id = await db.repos.tasks.create_collection_task(12345, "Internal")
    assert (await route_client.get(f"/api/tasks/{internal_id}")).status_code == 403
    assert (
        await route_client.post(f"/api/tasks/{internal_id}/complete", json={"result_payload": {}})
    ).status_code == 403
    assert (
        await route_client.post(f"/api/tasks/{internal_id}/fail", json={"error": "x"})
    ).status_code == 403
