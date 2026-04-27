"""End-to-end test of the collection flow (the main feature of this project).

Context (#457 round 4): previous rounds only checked HTTP status codes via
curl and asserted that a redirect happened. They did NOT check that clicking
"Собрать все каналы" actually causes messages to be written to the DB. That
gap is why the same "I pressed the button, nothing collected" bug kept
coming back: the user's report was about the FULL pipeline, but my tests
only covered the very first step (web enqueues a DB row) and the very last
step (redirect URL).

This test closes the gap. It runs `serve` as an ASGI app with the embedded
worker enabled (the new default), stubs `Collector.collect_single_channel`
to persist a couple of fake messages directly into `messages`, POSTs
`/scheduler/trigger`, then waits for the DB to see those rows. If any link
in the chain breaks — web doesn't enqueue, worker doesn't pick up, collector
isn't called — the test fails.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig, DatabaseConfig
from src.database import Database
from src.models import Account, Channel, CollectionTaskStatus, Message, RuntimeSnapshot
from src.web.app import create_app

_PASS = "testpass"


async def _fake_collect_single_channel(
    self, channel: Channel, *, full: bool = False, progress_callback=None, force: bool = False,
) -> int:
    """Stand-in for `Collector.collect_single_channel` used by e2e tests.

    Writes two deterministic messages into the `messages` table as if they
    were fetched from Telegram. Advances `last_collected_id` so subsequent
    incremental collections behave like the real thing.
    """
    base_id = (channel.last_collected_id or 0) + 1
    messages_to_write = [
        Message(
            channel_id=channel.channel_id,
            message_id=base_id,
            text=f"fake message {base_id} for channel {channel.channel_id}",
            date=datetime.now(timezone.utc),
        ),
        Message(
            channel_id=channel.channel_id,
            message_id=base_id + 1,
            text=f"fake message {base_id + 1} for channel {channel.channel_id}",
            date=datetime.now(timezone.utc),
        ),
    ]
    await self._db.repos.messages.insert_messages_batch(messages_to_write)
    await self._db.repos.channels.update_channel_last_id(channel.channel_id, base_id + 1)
    return len(messages_to_write)


@asynccontextmanager
async def _serve_app(tmp_path, *, embed_worker: bool):
    """Boot the real `serve` app lifespan (web container + optional embedded worker).

    Note: `ASGITransport` does NOT drive ASGI lifespan events by itself (httpx
    removed that in 0.27+). We drive them manually via `app.router.lifespan_context`
    so `configure_app(...)` runs and `app.state.db` / container are populated,
    just like when uvicorn boots the real process.
    """
    config = AppConfig(database=DatabaseConfig(path=str(tmp_path / "e2e.db")))
    config.web.password = _PASS
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"

    # Seed one active channel so trigger_collection finds something to enqueue.
    # We also pre-create an account row so the worker's ClientPool.initialize()
    # skips fast (it sees no usable session and doesn't try to connect).
    seed_db = Database(str(tmp_path / "e2e.db"))
    await seed_db.initialize()
    try:
        await seed_db.add_account(Account(phone="+7999", session_string=""))
        await seed_db.add_channel(
            Channel(channel_id=-1001, title="E2E Channel", is_active=True)
        )
    finally:
        await seed_db.close()

    app = create_app(config)
    app.state.embed_worker = embed_worker
    transport = ASGITransport(app=app)
    auth = base64.b64encode(f":{_PASS}".encode()).decode()

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            follow_redirects=False,
            headers={"Authorization": f"Basic {auth}", "Origin": "http://test"},
        ) as client:
            yield client, config


async def _wait_for_messages(db_path: str, channel_id: int, n: int, timeout: float = 15.0) -> int:
    """Poll the DB until at least `n` messages for `channel_id` are present."""
    deadline = asyncio.get_running_loop().time() + timeout
    db = Database(db_path)
    await db.initialize()
    try:
        while asyncio.get_running_loop().time() < deadline:
            rows = await db.execute_fetchall(
                "SELECT COUNT(*) AS c FROM messages WHERE channel_id = ?",
                (channel_id,),
            )
            count = rows[0][0] if rows else 0
            if count >= n:
                return count
            await asyncio.sleep(0.25)
        return count
    finally:
        await db.close()


async def _task_status(db_path: str, channel_id: int) -> str | None:
    db = Database(db_path)
    await db.initialize()
    try:
        rows = await db.execute_fetchall(
            "SELECT status FROM collection_tasks WHERE channel_id = ? ORDER BY id DESC LIMIT 1",
            (channel_id,),
        )
        return rows[0][0] if rows else None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_trigger_collection_persists_messages_via_embedded_worker(tmp_path):
    """`serve` with the embedded worker must complete the full collect tract.

    Click `Собрать все каналы` → task enqueued in DB → embedded worker picks
    it up → Collector.collect_single_channel (stubbed) writes messages → DB
    sees them → task status becomes COMPLETED. If any link breaks, the test
    hangs on the message-count assertion rather than passing silently.
    """
    with patch(
        "src.telegram.collector.Collector.collect_single_channel",
        new=_fake_collect_single_channel,
    ):
        async with _serve_app(tmp_path, embed_worker=True) as (client, config):
            resp = await client.post("/scheduler/trigger")
            assert resp.status_code == 303
            assert "collect_all_queued" in resp.headers.get("location", "")

            count = await _wait_for_messages(config.database.path, -1001, 2, timeout=15.0)
            assert count >= 2, f"expected >=2 messages for channel -1001, got {count}"

            status = await _task_status(config.database.path, -1001)
            assert status == CollectionTaskStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_trigger_collection_stuck_without_worker(tmp_path):
    """Regression guard: if `serve` is started with `--no-worker`, the same
    click leaves the task in PENDING. This proves the happy-path test above
    isn't passing by coincidence (e.g. DB writes happening on the web side).
    """
    with patch(
        "src.telegram.collector.Collector.collect_single_channel",
        new=_fake_collect_single_channel,
    ):
        async with _serve_app(tmp_path, embed_worker=False) as (client, config):
            resp = await client.post("/scheduler/trigger")
            assert resp.status_code == 303

            # Poll briefly; nothing should happen.
            await asyncio.sleep(2.0)
            count = await _wait_for_messages(config.database.path, -1001, 1, timeout=1.0)
            assert count == 0, (
                f"without the embedded worker, no messages should be collected, got {count}"
            )
            status = await _task_status(config.database.path, -1001)
            assert status == CollectionTaskStatus.PENDING.value


@pytest.mark.asyncio
async def test_embedded_worker_publishes_heartbeat(tmp_path):
    """Sanity: the embedded worker writes a fresh `worker_heartbeat` snapshot.

    Without this, round 1's UI banner can't detect a live worker and would
    always flip to `worker_down`.
    """
    with patch(
        "src.telegram.collector.Collector.collect_single_channel",
        new=_fake_collect_single_channel,
    ):
        async with _serve_app(tmp_path, embed_worker=True) as (_, config):
            # Wait for the first heartbeat — the worker publishes every ~5s.
            db = Database(config.database.path)
            await db.initialize()
            try:
                deadline = asyncio.get_running_loop().time() + 10.0
                snapshot = None
                while asyncio.get_running_loop().time() < deadline:
                    snapshot = await db.repos.runtime_snapshots.get_snapshot(
                        "worker_heartbeat"
                    )
                    if snapshot is not None and snapshot.updated_at is not None:
                        break
                    await asyncio.sleep(0.25)
                assert snapshot is not None, "embedded worker didn't publish a heartbeat"
                assert snapshot.payload.get("status") == "alive"
            finally:
                await db.close()


@pytest.mark.asyncio
async def test_health_reflects_accounts_after_snapshot_publish(tmp_path):
    """Regression guard for the `/scheduler/` shows 0/0 bug.

    Web container's read-only shims used to refresh exactly once at startup,
    so `/health` and `/scheduler/` reported `accounts_connected = 0` forever
    even when the worker kept publishing fresh `accounts_status` snapshots.

    Here we boot `serve` without an embedded worker (so nothing internally
    overwrites the snapshot), seed an `accounts_status` row directly, and
    expect the web container's `SnapshotRefresher` to pick it up within a
    couple of refresh ticks.
    """
    async with _serve_app(tmp_path, embed_worker=False) as (client, config):
        resp = await client.get("/health")
        assert resp.json()["accounts_connected"] == 0

        db = Database(config.database.path)
        await db.initialize()
        try:
            await db.repos.runtime_snapshots.upsert_snapshot(
                RuntimeSnapshot(
                    snapshot_type="accounts_status",
                    payload={"connected_phones": ["+7999"], "connected_count": 1},
                )
            )
        finally:
            await db.close()

        # SnapshotRefresher cadence is 3s; allow up to ~5s of jitter.
        deadline = asyncio.get_running_loop().time() + 6.0
        connected = 0
        while asyncio.get_running_loop().time() < deadline:
            resp = await client.get("/health")
            connected = resp.json()["accounts_connected"]
            if connected >= 1:
                break
            await asyncio.sleep(0.25)
        assert connected == 1, f"expected refresher to pick up snapshot, got accounts_connected={connected}"
