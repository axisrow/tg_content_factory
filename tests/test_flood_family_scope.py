"""Family-scoped read-side FloodWait reporting (#1832).

A read-side FloodWait (resolve/dialogs/history) records a per-(phone, family)
deadline and must NOT write ``accounts.flood_wait_until``, so publishing from
the same account stays available. Send-family floods keep blocking the account
account-wide; collection rotation keeps skipping family-flooded accounts; the
per-family state survives a restart (DB rows) and shows in
``account flood-status`` / clears via ``account flood-clear``.

All floods here are injected at the real transport dispatch boundary
(``TelegramTransportSession._run``) with deterministic Telethon errors — no
live Telegram.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon.errors import FloodWaitError
from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitedError

from tests.helpers import FakeCliTelethonClient

PHONE_A = "+70000000001"
PHONE_B = "+70000000002"


def _flood(seconds: int) -> FloodWaitError:
    err = FloodWaitError(request=None, capture=0)
    err.seconds = seconds
    return err


def _flooded_session(harness, phone: str, *, resolve=None, send=None, edit_admin=None):
    """Transport session over a fake client injecting FloodWait at the boundary."""
    client = FakeCliTelethonClient(
        entity_resolver=resolve,
        send_message_side_effect=send,
        edit_admin_side_effect=edit_admin,
    )
    from tg_agent.telegram.backends import TelegramTransportSession

    return TelegramTransportSession(client, phone=phone, pool=harness.pool)


async def _connect(harness, phone: str, *, primary: bool = False) -> None:
    harness.queue_cli_client(phone=phone, client=FakeCliTelethonClient())
    await harness.add_account(phone, session_string=f"s_{phone}", is_primary=primary)
    await harness.initialize_connected_accounts()


@pytest.mark.anyio
async def test_read_flood_scopes_to_family_and_leaves_account_writable(
    real_pool_harness_factory,
):
    """ResolveUsername flood (3600s) records the family, not the account."""
    harness = real_pool_harness_factory()
    await harness.add_account(PHONE_A, session_string="s1", is_primary=True)

    session = _flooded_session(harness, PHONE_A, resolve=lambda _a: _flood(3600))
    with pytest.raises(HandledFloodWaitError):
        await session.get_entity("@some_channel")

    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is None, (
        "read-side flood must not freeze the whole account"
    )
    floods = await harness.db.repos.accounts.list_active_family_floods(phone=PHONE_A)
    assert PHONE_A in floods and "resolve" in floods[PHONE_A]
    assert floods[PHONE_A]["resolve"] > datetime.now(timezone.utc)


@pytest.mark.anyio
async def test_send_flood_still_blocks_account_wide(real_pool_harness_factory):
    """SendMessage flood keeps the old account-wide behavior (#1832 AC)."""
    harness = real_pool_harness_factory()
    await harness.add_account(PHONE_A, session_string="s1", is_primary=True)

    session = _flooded_session(harness, PHONE_A, send=lambda *_a, **_k: _flood(3600))
    with pytest.raises(HandledFloodWaitError):
        await session.send_message("@some_channel", "hi")

    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is not None
    floods = await harness.db.repos.accounts.list_active_family_floods(phone=PHONE_A)
    assert floods == {}, "send-family flood is account-wide, not a family row"


@pytest.mark.anyio
async def test_admin_action_flood_blocks_account_wide(real_pool_harness_factory):
    """admin_action is a mutating bucket (edit_admin/...): account-wide, not family."""
    harness = real_pool_harness_factory()
    await harness.add_account(PHONE_A, session_string="s1", is_primary=True)

    def _raise_flood(*_a, **_k):
        raise _flood(3600)

    session = _flooded_session(harness, PHONE_A, edit_admin=_raise_flood)
    with pytest.raises(HandledFloodWaitError):
        await session.edit_admin("@chat", "@user", is_admin=True)

    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is not None, (
        "admin_action flood must keep the account-wide report"
    )
    floods = await harness.db.repos.accounts.list_active_family_floods(phone=PHONE_A)
    assert floods == {}


@pytest.mark.anyio
async def test_pool_without_family_slice_falls_back_account_wide():
    """A pool that reports account-wide only keeps that behavior (no dropped flood)."""
    from unittest.mock import AsyncMock

    from telethon_floodgate import FloodCircuitBreaker, TelegramRateLimitGate

    from tg_agent.telegram.backends import TelegramTransportSession

    pool = SimpleNamespace(
        _rate_limit_gate=TelegramRateLimitGate(),
        _outgoing_ceiling=None,
        _flood_breaker=FloodCircuitBreaker(),
        report_flood=AsyncMock(),
        # No report_family_flood / family_flood_deadline: the legacy shape.
    )
    client = FakeCliTelethonClient(entity_resolver=lambda _a: _flood(3600))
    session = TelegramTransportSession(client, phone=PHONE_A, pool=pool)

    with pytest.raises(HandledFloodWaitError):
        await session.get_entity("@some_channel")

    pool.report_flood.assert_awaited_once_with(PHONE_A, 3600)


@pytest.mark.anyio
async def test_flooded_read_family_refused_before_dispatch(real_pool_harness_factory):
    """After a resolve flood, further resolve requests are refused locally."""
    harness = real_pool_harness_factory()
    await harness.add_account(PHONE_A, session_string="s1", is_primary=True)

    calls = {"n": 0}

    def _counting_resolve(_a):
        calls["n"] += 1
        return _flood(3600)

    session = _flooded_session(harness, PHONE_A, resolve=_counting_resolve)
    with pytest.raises(HandledFloodWaitError):
        await session.get_entity("@some_channel")
    assert calls["n"] == 1

    fresh = _flooded_session(harness, PHONE_A, resolve=_counting_resolve)
    with pytest.raises(TelegramRateLimitedError):
        await fresh.get_entity("@some_channel")
    assert calls["n"] == 1, "refused call still reached Telegram"


@pytest.mark.anyio
async def test_send_dispatch_proceeds_while_resolve_flooded(real_pool_harness_factory):
    """The whole point (#1832): a read flood must not stop publishing."""
    harness = real_pool_harness_factory()
    await harness.add_account(PHONE_A, session_string="s1", is_primary=True)

    session = _flooded_session(harness, PHONE_A, resolve=lambda _a: _flood(3600))
    with pytest.raises(HandledFloodWaitError):
        await session.get_entity("@some_channel")

    send_calls = {"n": 0}

    def _ok_send(*_a, **_k):
        send_calls["n"] += 1
        return SimpleNamespace(id=1)

    sender = _flooded_session(harness, PHONE_A, send=_ok_send)
    await sender.send_message("@known_peer", "publish me")
    assert send_calls["n"] == 1, "send never dispatched while account was read-flooded"


@pytest.mark.anyio
async def test_collection_rotation_skips_family_flooded_account(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    await _connect(harness, PHONE_A, primary=True)
    await _connect(harness, PHONE_B)

    await harness.pool.report_family_flood(PHONE_A, 3600, "history")

    result = await harness.pool.get_available_client()
    assert result is not None
    assert result[1] == PHONE_B


@pytest.mark.anyio
async def test_family_flood_survives_restart(real_pool_harness_factory):
    """Per-family state lives in the DB, like accounts.flood_wait_until (#1832 AC)."""
    harness = real_pool_harness_factory()
    await _connect(harness, PHONE_A, primary=True)
    await _connect(harness, PHONE_B)
    await harness.pool.report_family_flood(PHONE_A, 3600, "resolve")

    restarted = real_pool_harness_factory()  # fresh pool, same DB
    await _connect(restarted, PHONE_A, primary=True)
    await _connect(restarted, PHONE_B)

    result = await restarted.pool.get_available_client()
    assert result is not None
    assert result[1] == PHONE_B

    # Pre-dispatch check also sees the restored state without any fresh report.
    calls = {"n": 0}

    def _counting_resolve(_a):
        calls["n"] += 1
        return _flood(3600)

    session = _flooded_session(restarted, PHONE_A, resolve=_counting_resolve)
    with pytest.raises(TelegramRateLimitedError):
        await session.get_entity("@some_channel")
    assert calls["n"] == 0


@pytest.mark.anyio
async def test_next_resolve_capable_at_honors_family_row(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect(harness, PHONE_A, primary=True)

    assert await harness.pool.next_resolve_capable_at() is None

    await harness.pool.report_family_flood(PHONE_A, 3600, "resolve")
    capable_at = await harness.pool.next_resolve_capable_at()
    assert capable_at is not None
    assert capable_at > datetime.now(timezone.utc)


def _account(phone: str):
    from tg_agent.models import Account

    return Account(phone=phone, session_string="s1")


@pytest.mark.anyio
async def test_flood_status_lists_family_rows(db, monkeypatch, capsys):
    await db.repos.accounts.set_family_flood(
        PHONE_A,
        "resolve",
        datetime.now(timezone.utc) + timedelta(hours=2),
    )
    await db.add_account(_account(PHONE_A))

    async def _fake_init(_config_path):
        return None, db

    monkeypatch.setattr(
        "tg_agent.cli.commands.account.runtime.init_db", _fake_init
    )
    from tg_agent.cli.commands.account import flood_status_impl

    await flood_status_impl("config.yaml")
    out = capsys.readouterr().out
    assert "resolve" in out


@pytest.mark.anyio
async def test_flood_clear_clears_family_rows(db, monkeypatch, capsys):
    await db.update_account_flood(PHONE_A, datetime.now(timezone.utc) + timedelta(hours=1))
    await db.repos.accounts.set_family_flood(
        PHONE_A,
        "history",
        datetime.now(timezone.utc) + timedelta(hours=2),
    )
    await db.add_account(_account(PHONE_A))
    assert "history" in (await db.repos.accounts.list_active_family_floods(phone=PHONE_A))[PHONE_A]

    async def _fake_init(_config_path):
        return None, db

    monkeypatch.setattr(
        "tg_agent.cli.commands.account.runtime.init_db", _fake_init
    )
    from tg_agent.cli.commands.account import flood_clear_impl

    # The impl closes the shared in-memory DB on exit — assert state before,
    # and rely on its output (plus the repo test below) for the after-picture.
    await flood_clear_impl("config.yaml", phone=PHONE_A)
    out = capsys.readouterr().out
    assert "Flood wait cleared for" in out
    assert "cleared family flood: history" in out


@pytest.mark.anyio
async def test_clear_family_floods_removes_rows(db):
    until = datetime.now(timezone.utc) + timedelta(hours=2)
    await db.repos.accounts.set_family_flood(PHONE_A, "history", until)
    await db.repos.accounts.set_family_flood(PHONE_A, "resolve", until)
    await db.repos.accounts.set_family_flood(PHONE_B, "resolve", until)

    await db.repos.accounts.clear_family_floods(PHONE_A, family="history")
    floods = await db.repos.accounts.list_active_family_floods()
    assert floods[PHONE_A] == {"resolve": floods[PHONE_A]["resolve"]}
    assert PHONE_B in floods

    await db.repos.accounts.clear_family_floods(PHONE_A)
    floods = await db.repos.accounts.list_active_family_floods()
    assert PHONE_A not in floods and PHONE_B in floods
