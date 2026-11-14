"""Startup warm on the gateway pool must not force full sweeps (#1819).

Bootstrap runs ``warm_all_dialogs(skip_already_swept=True)`` on every worker
restart. The native pool honours the flag; ``GatewayPool`` swallowed it and
forced ``refresh=True`` full GetDialogs sweeps on every account, so restart
storms (4+ restarts within an hour on 2026-11-14) multiplied server-side
sweeps into multi-hour flood-waits. Startup warm must serve from the
gateway's persistent dialog cache (refresh only when stale), skip accounts
already in flood-wait, and never crash the startup task.
"""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from tg_agent.config import AppConfig
from tg_agent.gateway.pool import GatewayPool


def _account_row(phone: str, *, flood_until: float = 0.0, primary: bool = False) -> dict:
    return {
        "id": str(uuid4()),
        "phone": phone,
        "active": True,
        "flood_until": flood_until,
        "is_primary": primary,
        "connected": True,
    }


async def _make_pool(db, rows: list[dict]) -> tuple[GatewayPool, list]:
    pool = GatewayPool(AppConfig(), db)
    submitted: list = []

    async def submit(request, operation):
        submitted.append(operation)

    async def noop_initialize(**kwargs):
        pool.clients = {row["phone"]: row for row in rows}

    pool.initialize = noop_initialize
    pool.client = SimpleNamespace(submit=submit, wait=AsyncMock(
        return_value={"state": "succeeded", "result": {"dialogs": []}}))
    await pool.outbox.initialize()
    return pool, submitted


@pytest.mark.aiosqlite_serial
async def test_startup_warm_does_not_force_refresh_sweeps(db):
    rows = [_account_row("+100", primary=True), _account_row("+200")]
    pool, submitted = await _make_pool(db, rows)

    await pool.warm_all_dialogs(skip_already_swept=True)

    assert submitted, "startup warm should still touch every usable account"
    assert [op.refresh for op in submitted] == [False, False], (
        "startup warm must serve from the gateway's dialog cache, not force "
        "a full server-side sweep per account on every worker restart (#1819)"
    )


@pytest.mark.aiosqlite_serial
async def test_periodic_warm_keeps_refresh_sweeps(db):
    rows = [_account_row("+100", primary=True)]
    pool, submitted = await _make_pool(db, rows)

    await pool.warm_all_dialogs()

    assert [op.refresh for op in submitted] == [True], (
        "the periodic refresh job exists to re-sweep; it must keep refresh=True (#1043)"
    )


@pytest.mark.aiosqlite_serial
async def test_startup_warm_skips_flood_waited_accounts(db):
    now = time.time()
    flooded = _account_row("+100", flood_until=now + 3600, primary=True)
    usable = _account_row("+200")
    pool, submitted = await _make_pool(db, [flooded, usable])

    await pool.warm_all_dialogs(skip_already_swept=True)

    assert [op.account_id for op in submitted] == [UUID(usable["id"])], (
        "an account with a stored future flood-wait must not receive warm traffic"
    )


@pytest.mark.aiosqlite_serial
async def test_startup_warm_survives_all_accounts_flooded(db):
    now = time.time()
    rows = [
        _account_row("+100", flood_until=now + 8502, primary=True),
        _account_row("+200", flood_until=now + 52231),
    ]
    pool, submitted = await _make_pool(db, rows)

    # 2026-11-14: the startup task crashed with AllCollectionClientsFloodedError
    # because the restarted worker inherited persisted flood-waits; the warm
    # task must skip and complete instead of dying with a traceback.
    await pool.warm_all_dialogs(skip_already_swept=True)

    assert submitted == []


@pytest.mark.aiosqlite_serial
async def test_startup_warm_isolates_one_failing_phone(db):
    """A failed receipt on one phone must not abort the pass (#1821 review).

    A cache-miss sweep on a never-swept account can die (schema drift,
    network) and come back as a failed receipt; the remaining phones still
    need their warm, and the startup task must stay alive.
    """
    rows = [_account_row("+100", primary=True), _account_row("+200")]
    pool = GatewayPool(AppConfig(), db)
    submitted: list = []

    async def submit(request, operation):
        if str(operation.account_id) == rows[0]["id"]:
            raise ValueError("boom")
        submitted.append(operation)

    async def noop_initialize(**kwargs):
        pool.clients = {row["phone"]: row for row in rows}

    pool.initialize = noop_initialize
    pool.client = SimpleNamespace(submit=submit, wait=AsyncMock(
        return_value={"state": "succeeded", "result": {"dialogs": []}}))
    await pool.outbox.initialize()

    await pool.warm_all_dialogs(skip_already_swept=True)

    assert [op.account_id for op in submitted] == [UUID(rows[1]["id"])], (
        "the pass must continue past a failing phone and warm the rest"
    )
