"""Deterministic concurrency guards for the pool lease lifecycle (#1228)."""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest

_LEASE_OWNERSHIP_ALLOWLIST = {
    # These helpers deliberately return the borrowed result to a caller that
    # owns the finally/release boundary.
    "src/telegram/pool_dialogs.py:_get_client",
    "src/services/telegram_command_dispatcher.py:_get_client",
    "src/services/telegram_actions.py:_client",
    "src/agent/tools/_registry.py:resolve_entity",
    "src/cli/commands/messages.py:messages_read_impl",
}


def test_pool_acquisition_call_sites_have_an_explicit_release_owner():
    """New acquisition call sites must show their lease ownership in source."""
    root = Path(__file__).parents[1] / "src"
    violations: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative = path.relative_to(root.parent).as_posix()
        for node in ast.walk(tree):
            if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            has_acquire = any(
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in {
                    "get_available_client",
                    "get_client_by_phone",
                    "get_native_client_by_phone",
                }
                for call in ast.walk(node)
            )
            if not has_acquire:
                continue
            has_release = any(
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "release_client"
                for call in ast.walk(node)
            )
            owner = f"{relative}:{node.name}"
            if not has_release and owner not in _LEASE_OWNERSHIP_ALLOWLIST:
                violations.append(owner)
    assert violations == [], "lease ownership is undocumented: " + ", ".join(violations)


@pytest.mark.anyio
async def test_phone_lease_barrier_never_mixes_exclusive_and_shared(real_pool_harness_factory):
    """Many simultaneous phone acquisitions have one exclusive owner at most."""
    harness = real_pool_harness_factory()
    phone = "+71230000001"
    await harness.add_account(phone, session_string="lease-stress")
    lease_pool = harness.pool._lease_pool
    barrier = asyncio.Barrier(24)

    async def acquire():
        await barrier.wait()
        return await lease_pool.acquire_by_phone(phone, {phone})

    leases = await asyncio.gather(*(acquire() for _ in range(24)))
    assert all(lease is not None for lease in leases)
    assert sum(not lease.shared for lease in leases if lease is not None) == 1
    assert sum(lease.shared for lease in leases if lease is not None) == 23
    await lease_pool.release(phone)


@pytest.mark.anyio
async def test_failed_backend_acquire_rolls_back_exclusive_reservation(real_pool_harness_factory):
    """An exception after reservation cannot leak _in_use or an active lease."""
    harness = real_pool_harness_factory()
    phone = "+71230000002"
    await harness.add_account(phone, session_string="lease-failure")
    lease = await harness.pool._lease_pool.acquire_by_phone(phone, {phone})
    assert lease is not None and not lease.shared

    async def fail_acquire(*_args, **_kwargs):
        raise RuntimeError("backend unavailable")

    harness.pool._backend_router.acquire_client = fail_acquire
    result = await harness.pool._acquire_from_lease(lease)

    assert result is None
    assert phone not in harness.pool._in_use
    assert phone not in harness.pool._active_leases


@pytest.mark.anyio
async def test_disconnect_all_clears_live_lease_state(real_pool_harness_factory):
    """Teardown removes clients and all lease ownership before returning."""
    harness = real_pool_harness_factory()
    phone = "+71230000003"
    client = harness.queue_cli_client(phone=phone)
    await harness.add_account(phone, session_string="lease-disconnect")
    await harness.initialize_connected_accounts()
    assert await harness.pool.get_client_by_phone(phone) is not None

    await harness.pool.disconnect_all()

    assert harness.pool.clients == {}
    assert harness.pool._in_use == set()
    assert dict(harness.pool._active_leases) == {}
    assert client.disconnect.await_count == 1


@pytest.mark.anyio
async def test_acquisition_started_during_teardown_is_rejected(real_pool_harness_factory):
    """A lease that reaches the pool after teardown starts cannot be returned."""
    harness = real_pool_harness_factory()
    phone = "+71230000004"
    harness.queue_cli_client(phone=phone)
    await harness.add_account(phone, session_string="lease-race")
    await harness.initialize_connected_accounts()

    acquire_entered = asyncio.Event()
    allow_acquire = asyncio.Event()
    teardown_started = asyncio.Event()
    original_acquire = harness.pool._lease_pool.acquire_by_phone

    async def blocked_acquire(*args, **kwargs):
        acquire_entered.set()
        await allow_acquire.wait()
        return await original_acquire(*args, **kwargs)

    harness.pool._lease_pool.acquire_by_phone = blocked_acquire

    original_disconnect = harness.pool.disconnect_all

    async def tracked_disconnect():
        teardown_started.set()
        await original_disconnect()

    harness.pool.disconnect_all = tracked_disconnect
    acquire_task = asyncio.create_task(harness.pool.get_client_by_phone(phone))
    await acquire_entered.wait()
    teardown_task = asyncio.create_task(harness.pool.disconnect_all())
    await teardown_started.wait()
    allow_acquire.set()
    result = await acquire_task
    await teardown_task

    assert result is None
    assert harness.pool._in_use == set()
    assert harness.pool._active_leases == {}


@pytest.mark.anyio
async def test_write_lock_barrier_blocks_autocommit_until_transaction_commits(db):
    """A transaction and execute_write cannot interleave at an await point."""
    transaction_ready = asyncio.Event()
    allow_commit = asyncio.Event()
    autocommit_started = asyncio.Event()
    autocommit_done = asyncio.Event()

    async def transaction_writer():
        async with db.transaction() as conn:
            await conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ("tx", "1"))
            transaction_ready.set()
            await allow_commit.wait()

    async def autocommit_writer():
        await transaction_ready.wait()
        autocommit_started.set()
        await db.execute_write("INSERT INTO settings (key, value) VALUES (?, ?)", ("ac", "1"))
        autocommit_done.set()

    tx_task = asyncio.create_task(transaction_writer())
    await transaction_ready.wait()
    ac_task = asyncio.create_task(autocommit_writer())
    await autocommit_started.wait()
    assert not autocommit_done.is_set()
    allow_commit.set()
    await asyncio.gather(tx_task, ac_task)
    rows = await db.execute_fetchall("SELECT key FROM settings WHERE key IN ('tx', 'ac')")
    assert sorted(row["key"] for row in rows) == ["ac", "tx"]
