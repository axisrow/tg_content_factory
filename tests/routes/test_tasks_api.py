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


async def test_create_with_key_returns_existing_task_without_mutation(route_client):
    body = {"type": "dm_reply", "payload": {"text": "hi"}, "idempotency_key": "messenger:send-1"}
    created = await route_client.post("/api/tasks", json=body)
    task_id = created.json()["id"]
    original = (await route_client.get(f"/api/tasks/{task_id}")).json()

    replay = await route_client.post("/api/tasks", json=body)
    assert created.status_code == replay.status_code == 201
    assert replay.json() == created.json()
    # A key identifies the original operation across types; the first body wins.
    changed = await route_client.post("/api/tasks", json={**body, "type": "chat_answer", "payload": {}})
    assert changed.status_code == 201
    assert changed.json() == created.json()
    assert (await route_client.get(f"/api/tasks/{task_id}")).json() == original
    db = route_client._transport_app.state.db
    assert await db.repos.tasks.count_collection_tasks() == 1

    await route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]})
    await route_client.post(f"/api/tasks/{task_id}/complete", json={})
    terminal = (await route_client.get(f"/api/tasks/{task_id}")).json()
    assert terminal["status"] == "completed"
    assert (await route_client.post("/api/tasks", json=body)).json() == created.json()
    assert (await route_client.get(f"/api/tasks/{task_id}")).json() == terminal
    assert await db.repos.tasks.count_collection_tasks() == 1


@pytest.mark.parametrize(
    "keys",
    [
        ({}, {}),
        ({"idempotency_key": None}, {"idempotency_key": None}),
        ({"idempotency_key": "first"}, {"idempotency_key": "second"}),
    ],
)
async def test_create_without_shared_key_keeps_tasks_distinct(route_client, keys):
    responses = [
        await route_client.post("/api/tasks", json={"type": "fetch_dialogs", **key}) for key in keys
    ]
    assert all(response.status_code == 201 for response in responses)
    assert responses[0].json()["id"] != responses[1].json()["id"]


@pytest.mark.parametrize("key", ["", "x" * 256, 123])
async def test_create_rejects_invalid_idempotency_key(route_client, key):
    response = await route_client.post("/api/tasks", json={"type": "dm_reply", "idempotency_key": key})
    assert response.status_code == 422


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


@pytest.mark.parametrize(
    "endpoint,body,replay_body",
    [
        (
            "complete",
            {"result_payload": {"sent": True, "ids": [1, 2]}},
            {"result_payload": {"ids": [1, 2], "sent": True}},
        ),
        ("complete", {}, {"result_payload": {}}),
        ("fail", {"error": "boom"}, {"error": "boom"}),
    ],
)
async def test_terminal_report_replay_succeeds_without_mutation(route_client, endpoint, body, replay_body):
    created = await route_client.post("/api/tasks", json={"type": "dm_reply"})
    task_id = created.json()["id"]
    await route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]})
    accepted = await route_client.post(f"/api/tasks/{task_id}/{endpoint}", json=body)
    original = (await route_client.get(f"/api/tasks/{task_id}")).json()

    replay = await route_client.post(f"/api/tasks/{task_id}/{endpoint}", json=replay_body)
    assert accepted.status_code == replay.status_code == 200
    assert replay.json() == accepted.json() == {"ok": True}
    assert (await route_client.get(f"/api/tasks/{task_id}")).json() == original


@pytest.mark.parametrize(
    "first_endpoint,first_body,second_endpoint,second_body",
    [
        ("complete", {}, "fail", {"error": "boom"}),
        ("fail", {"error": "boom"}, "complete", {}),
        ("complete", {"result_payload": {"id": 1}}, "complete", {"result_payload": {"id": 2}}),
        ("complete", {"result_payload": {"sent": True}}, "complete", {"result_payload": {"sent": 1}}),
        ("fail", {"error": "boom"}, "fail", {"error": "different error"}),
    ],
)
async def test_conflicting_terminal_reports_are_rejected(
    route_client, first_endpoint, first_body, second_endpoint, second_body
):
    created = await route_client.post("/api/tasks", json={"type": "dm_reply"})
    task_id = created.json()["id"]
    await route_client.post("/api/tasks/claim", json={"types": ["dm_reply"]})
    accepted = await route_client.post(f"/api/tasks/{task_id}/{first_endpoint}", json=first_body)
    assert accepted.status_code == 200
    original = (await route_client.get(f"/api/tasks/{task_id}")).json()

    conflict = await route_client.post(f"/api/tasks/{task_id}/{second_endpoint}", json=second_body)
    assert conflict.status_code == 409
    assert (await route_client.get(f"/api/tasks/{task_id}")).json() == original


@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("endpoint,body", [("complete", {}), ("fail", {"error": "boom"})])
async def test_reports_cannot_skip_claim_or_overwrite_cancelled_tasks(route_client, cancelled, endpoint, body):
    created = await route_client.post("/api/tasks", json={"type": "dm_reply"})
    task_id = created.json()["id"]
    if cancelled:
        db = route_client._transport_app.state.db
        await db.repos.tasks.cancel_collection_task(task_id)
    original = (await route_client.get(f"/api/tasks/{task_id}")).json()
    response = await route_client.post(f"/api/tasks/{task_id}/{endpoint}", json=body)
    assert response.status_code == 409
    assert (await route_client.get(f"/api/tasks/{task_id}")).json() == original


async def test_complete_missing_returns_404(route_client):
    resp = await route_client.post("/api/tasks/999999/complete", json={"result_payload": {}})
    assert resp.status_code == 404


async def test_complete_requires_running_status(route_client):
    # A freshly-created (PENDING, unclaimed) task cannot be completed — guards
    # against skipping the atomic claim (#961 review).
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


async def test_create_rejects_oversized_payload(route_client):
    big = {"text": "x" * (70 * 1024)}
    resp = await route_client.post("/api/tasks", json={"type": "dm_reply", "payload": big})
    assert resp.status_code == 422
