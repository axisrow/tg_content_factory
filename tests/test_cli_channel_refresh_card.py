"""`channel refresh-card` — refresh all open channel metadata from Telegram (#1764).

Integration tests over the real :memory: DB. Pool-contract coverage follows the
#1765 review: the direct ClientPool contract is exercised through a real
ClientPool (``real_pool_harness_factory``) proving the group→PeerChat peer
rule, and the GatewayPool contract through a real GatewayPool with a stub HTTP
client proving preferred-account routing and the group peer_type. CLI-level
fakes cover the command flow over both contracts.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
import typer
from telethon.tl.types import PeerChannel, PeerChat

from tests.helpers import FakeCliTelethonClient
from tg_agent.cli.commands.channel import refresh_card_impl
from tg_agent.cli.commands.messages import messages_read_impl
from tg_agent.config import AppConfig
from tg_agent.gateway.pool import GatewayPool
from tg_agent.gateway.protocol import InspectPeer
from tg_agent.models import Channel, Message


def _reopen(db):
    """impl закрывает БД в finally — переоткрыть fixture-инстанс (паттерн test_cli_ping)."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


def _add_stale_channel(
    db,
    *,
    channel_id: int,
    title: str,
    username: str | None,
    channel_type: str = "channel",
) -> None:
    _reopen(db)
    asyncio.run(
        db.add_channel(
            Channel(
                channel_id=channel_id,
                title=title,
                username=username,
                channel_type=channel_type,
                about="old about",
                linked_chat_id=None,
                has_comments=False,
            )
        )
    )


def _seed_messages(db, *, channel_id: int, n: int = 2) -> None:
    _reopen(db)
    msgs = [
        Message(
            channel_id=channel_id,
            message_id=i + 1,
            text=f"post {i + 1}",
            date=datetime(2026, 10, 1, 12, tzinfo=timezone.utc),
        )
        for i in range(n)
    ]
    inserted = asyncio.run(db.repos.messages.insert_messages_batch(msgs))
    assert inserted == n


class FakeDirectPool:
    """ClientPool contract: resolve_channel (entity fields) + fetch_channel_meta.

    ``resolve_by_id[channel_id]`` is the resolve_channel return value for that
    channel — a dict, None (transient/unavailable) or an Exception instance.
    """

    clients = {"+10000000001": object()}

    def __init__(self, resolve_by_id, meta_by_id):
        self._resolve_by_id = resolve_by_id
        self._meta_by_id = meta_by_id
        self.resolve_calls: list[tuple[str, dict]] = []
        self.meta_calls: list[tuple[int, str | None]] = []

    async def resolve_channel(self, identifier, **kwargs):
        self.resolve_calls.append((str(identifier), kwargs))
        result = self._resolve_by_id[int(identifier)]
        if isinstance(result, Exception):
            raise result
        return result

    async def fetch_channel_meta(self, channel_id, channel_type=None, **kwargs):
        self.meta_calls.append((channel_id, channel_type))
        result = self._meta_by_id[channel_id]
        if isinstance(result, Exception):
            raise result
        return result

    async def disconnect_all(self):
        pass


class StubGatewayClient:
    """Captures submitted InspectPeer operations; replays one fixed receipt."""

    def __init__(self, receipt: dict):
        self.receipt = receipt
        self.submitted: list = []

    async def submit(self, request, operation):
        self.submitted.append(operation)

    async def wait(self, request, timeout=None):
        return self.receipt

    async def close(self):
        pass


def _make_gateway_pool(db, receipt: dict) -> tuple[GatewayPool, StubGatewayClient]:
    """Real GatewayPool over a stub HTTP client; two shared project accounts.

    Account rows mirror the gateway status JSON payload (id is a string).
    """
    pool = GatewayPool(AppConfig(), db, client=StubGatewayClient(receipt))
    primary = {"id": str(uuid4()), "phone": "+70000000001", "active": True,
               "flood_until": 0.0, "is_primary": True}
    owner = {"id": str(uuid4()), "phone": "+70000000002", "active": True,
             "flood_until": 0.0, "is_primary": False}
    pool.clients = {primary["phone"]: primary, owner["phone"]: owner}
    asyncio.run(pool.outbox.initialize())
    stub = pool.client
    assert isinstance(stub, StubGatewayClient)
    return pool, stub


_GATEWAY_CARD = {
    "channel_id": 821,
    "title": "GW Title",
    "username": "gwname",
    "channel_type": "channel",
    "deactivate": False,
    "about": "gw about",
    "linked_chat_id": 999,
    "has_comments": True,
    "created_at": "2020-01-01T00:00:00+00:00",
}


def _run_refresh_card(db, pool, *, identifier=None, all_channels=False):
    _reopen(db)

    async def fake_init_db(_):
        return AppConfig(), db

    async def fake_init_pool(_, __):
        return MagicMock(), pool

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), patch(
        "tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool
    ):
        asyncio.run(
            refresh_card_impl(
                "config.yaml", all_channels=all_channels, identifier=identifier
            )
        )
    return pool


def _run_messages_read(db, identifier):
    _reopen(db)

    async def fake_init_db(_):
        return AppConfig(), db

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db):
        asyncio.run(messages_read_impl("config.yaml", identifier=identifier, limit=10))


def _get_channel(db, channel_id: int) -> Channel:
    _reopen(db)
    return asyncio.run(db.repos.channels.get_channel_by_channel_id(channel_id))


# ---------------------------------------------------------------------------
# Single channel by stable ID: stale title/username refreshed, messages findable
# under the new username (#1764 acceptance)
# ---------------------------------------------------------------------------


def test_refresh_card_updates_stale_fields_and_messages_follow_new_username(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=700, title="Old Title", username="oldname")
    _seed_messages(db, channel_id=700)

    pool = FakeDirectPool(
        resolve_by_id={
            700: {
                "channel_id": 700,
                "title": "Fresh Title",
                "username": "newname",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            }
        },
        meta_by_id={700: {"about": "fresh about", "linked_chat_id": 555, "has_comments": True}},
    )
    _run_refresh_card(db, pool, identifier="700")

    # Telegram side was queried by the STABLE numeric id with the stored type
    # hint, never the stale username.
    assert pool.resolve_calls[0][0] == "700"
    assert pool.resolve_calls[0][1]["channel_type"] == "channel"
    assert pool.meta_calls == [(700, "channel")]

    ch = _get_channel(db, 700)
    assert ch.title == "Fresh Title"
    assert ch.username == "newname"
    assert ch.about == "fresh about"
    assert ch.linked_chat_id == 555
    assert ch.has_comments is True

    # Acceptance: messages read by the NEW username finds the collected messages,
    # and the old username no longer resolves.
    _run_messages_read(db, "@newname")
    out = capsys.readouterr().out
    assert "post 1" in out
    with pytest.raises(typer.Exit):
        _run_messages_read(db, "@oldname")
    assert "not found" in capsys.readouterr().err


def test_refresh_card_preserves_user_settings(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=701, title="Old", username="u701")
    ch = _get_channel(db, 701)
    asyncio.run(db.repos.channels.set_channel_active(ch.id, False, origin="human"))
    asyncio.run(db.set_channel_filtered(ch.id, True, origin="human"))

    pool = FakeDirectPool(
        resolve_by_id={
            701: {
                "channel_id": 701,
                "title": "New",
                "username": "u701-new",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            }
        },
        meta_by_id={701: {"about": "a", "linked_chat_id": None, "has_comments": False}},
    )
    _run_refresh_card(db, pool, identifier="701")

    fresh = _get_channel(db, 701)
    assert fresh.title == "New"
    assert fresh.is_active is False  # operator decision preserved
    assert fresh.active_origin == "human"
    assert fresh.is_filtered is True  # operator decision preserved
    assert fresh.filtered_origin == "human"
    assert "OK" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Legacy basic groups: known channel_type must reach the pool so the entity is
# resolved as PeerChat, not PeerChannel (#1765 review point 1)
# ---------------------------------------------------------------------------


def test_refresh_card_group_channel_passes_type_hint(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=-850, title="Legacy Group", username=None,
                       channel_type="group")

    def _card(cid: int) -> dict:
        return {
            "channel_id": cid,
            "title": "Renamed Group",
            "username": None,
            "channel_type": "group",
            "deactivate": False,
            "created_at": None,
        }

    pool = FakeDirectPool(
        resolve_by_id={-850: _card(-850)},
        meta_by_id={-850: None},  # direct pool has no full-channel part for groups
    )
    _run_refresh_card(db, pool, identifier="-850")

    assert pool.resolve_calls[0][0] == "-850"
    assert pool.resolve_calls[0][1]["channel_type"] == "group"
    # Group + no full-channel part is NOT a partial outage: entity fields applied.
    assert _get_channel(db, -850).title == "Renamed Group"
    assert "OK" in capsys.readouterr().out


async def test_resolve_channel_group_hint_builds_peerchat(real_pool_harness_factory, db):
    """Real ClientPool contract: a stored group row resolves via PeerChat.

    Resolving a legacy group's numeric id as PeerChannel raises even on a
    member account (#1765 review point 1) — the stored channel_type must pick
    the peer kind.
    """
    harness = real_pool_harness_factory()
    seen_peers: list = []

    def resolver(peer):
        seen_peers.append(peer)
        return SimpleNamespace(id=850, title="Legacy Group", username=None, date=None)

    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient(entity_resolver=resolver))
    await harness.add_account("+70000000001", session_string="s1", is_primary=True)
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel(
        "-850", signal_gone=True, numeric_fallback="-850", channel_type="group"
    )

    assert result is not None
    assert result["channel_type"] == "group"
    assert result["title"] == "Legacy Group"
    assert any(isinstance(p, PeerChat) and p.chat_id == 850 for p in seen_peers)


async def test_resolve_channel_without_hint_keeps_peerchannel(real_pool_harness_factory, db):
    """Default (no type hint) still resolves channels/supergroups via PeerChannel."""
    harness = real_pool_harness_factory()
    seen_peers: list = []

    def resolver(peer):
        seen_peers.append(peer)
        return SimpleNamespace(id=1970788983, title="Chan", username="chan", date=None)

    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient(entity_resolver=resolver))
    await harness.add_account("+70000000001", session_string="s1", is_primary=True)
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel(
        "1970788983", signal_gone=True, numeric_fallback="1970788983"
    )

    assert result is not None
    assert any(isinstance(p, PeerChannel) for p in seen_peers)


# ---------------------------------------------------------------------------
# Gateway path (real GatewayPool over a stub HTTP client, #1765 review point 4):
# full card updates + preferred-account routing + group peer_type
# ---------------------------------------------------------------------------


def test_refresh_card_gateway_path_updates_full_card(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=821, title="Old GW", username="oldgw")
    _seed_messages(db, channel_id=821)

    pool, _stub = _make_gateway_pool(
        db, {"state": "succeeded", "result": dict(_GATEWAY_CARD)}
    )
    _run_refresh_card(db, pool, identifier="821")

    ch = _get_channel(db, 821)
    assert ch.title == "GW Title"
    assert ch.username == "gwname"
    assert ch.about == "gw about"
    assert ch.linked_chat_id == 999
    assert ch.has_comments is True

    _run_messages_read(db, "@gwname")
    assert "post 1" in capsys.readouterr().out


def test_refresh_card_gateway_error_is_a_failure_not_a_wipe(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=822, title="GW Keep", username="gwkeep")

    pool, _stub = _make_gateway_pool(
        db, {"state": "failed", "result": {"error": "ChannelPrivateError"}}
    )
    _run_refresh_card(db, pool, identifier="822")

    ch = _get_channel(db, 822)
    assert ch.title == "GW Keep"
    assert ch.username == "gwkeep"
    assert "FAILED" in capsys.readouterr().out


async def test_gateway_pool_routes_owner_account_and_group_peer_type(db):
    """Real GatewayPool contract (#1765 review points 1+2).

    resolve_channel must forward the channel's preferred (owner) account —
    a private chat visible only to that account would otherwise fail on the
    project primary — and map a group row to peer_type "chat" so the server
    side builds PeerChat, not PeerChannel.
    """
    await db.add_channel(
        Channel(channel_id=444, title="Old", username="g444", channel_type="group")
    )
    await db.repos.channels.update_channel_preferred_phone(444, "+70000000002")

    pool = GatewayPool(AppConfig(), db)
    primary = {"id": str(uuid4()), "phone": "+70000000001", "active": True,
               "flood_until": 0.0, "is_primary": True}
    owner = {"id": str(uuid4()), "phone": "+70000000002", "active": True,
             "flood_until": 0.0, "is_primary": False}
    pool.clients = {primary["phone"]: primary, owner["phone"]: owner}
    stub = StubGatewayClient({"state": "succeeded",
                              "result": {"channel_id": 444, "title": "New Group",
                                         "username": "g444", "channel_type": "group",
                                         "deactivate": False}})
    pool.client = stub
    await pool.outbox.initialize()

    info = await pool.resolve_channel("444", channel_type="group", phone="+70000000002")

    assert info["title"] == "New Group"
    assert len(stub.submitted) == 1
    op = stub.submitted[0]
    assert isinstance(op, InspectPeer)
    assert op.account_id == UUID(owner["id"])  # owner account, NOT the primary
    assert op.peer_type == "chat"  # group → PeerChat on the server side

    # Non-group rows keep the channel peer type.
    await pool.resolve_channel("444", channel_type="channel", phone=None)
    assert stub.submitted[-1].peer_type == "channel"
    assert stub.submitted[-1].account_id == UUID(primary["id"])


# ---------------------------------------------------------------------------
# Confirmed absence vs inability to obtain
# ---------------------------------------------------------------------------


def test_refresh_card_confirmed_username_removal_writes_null(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=702, title="T", username="dropped")

    pool = FakeDirectPool(
        resolve_by_id={
            702: {
                "channel_id": 702,
                "title": "T",
                "username": None,  # channel dropped its @username — confirmed absence
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            }
        },
        meta_by_id={702: {"about": "a", "linked_chat_id": None, "has_comments": False}},
    )
    _run_refresh_card(db, pool, identifier="702")

    ch = _get_channel(db, 702)
    assert ch.username is None
    assert ch.username_state == "known"
    assert "OK" in capsys.readouterr().out


def test_refresh_card_partial_meta_keeps_existing_about(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=703, title="Old", username="u703")

    pool = FakeDirectPool(
        resolve_by_id={
            703: {
                "channel_id": 703,
                "title": "New",
                "username": "u703",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            }
        },
        meta_by_id={703: None},  # full-channel part unavailable (transient)
    )
    _run_refresh_card(db, pool, identifier="703")

    ch = _get_channel(db, 703)
    # Confirmed entity fields were written...
    assert ch.title == "New"
    # ...but the unavailable about/linked/comments kept their stored values.
    assert ch.about == "old about"
    assert ch.linked_chat_id is None
    assert ch.has_comments is False
    out = capsys.readouterr().out
    assert "partial" in out.lower()


def test_refresh_card_gone_and_error_leave_rows_untouched(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=704, title="KeepMe", username="keepme")
    _add_stale_channel(db, channel_id=705, title="KeepMe2", username="keepme2")

    pool = FakeDirectPool(
        resolve_by_id={704: {"gone": True}, 705: RuntimeError("no_client")},
        meta_by_id={},
    )
    _run_refresh_card(db, pool, identifier="704")

    ch = _get_channel(db, 704)
    assert ch.title == "KeepMe"
    assert ch.username == "keepme"
    assert ch.is_active is True  # refresh-card never deactivates

    _run_refresh_card(db, pool, identifier="705")
    ch2 = _get_channel(db, 705)
    assert ch2.title == "KeepMe2"
    assert ch2.username == "keepme2"
    out = capsys.readouterr().out
    assert "SKIP" in out
    assert "FAILED" in out


# ---------------------------------------------------------------------------
# Bulk mode: per-channel result + counters, one failure hides nothing
# ---------------------------------------------------------------------------


def test_refresh_card_bulk_reports_counters_and_isolates_errors(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=801, title="Ok Ch", username="okch")
    _add_stale_channel(db, channel_id=802, title="Gone Ch", username="gonech")
    _add_stale_channel(db, channel_id=803, title="Err Ch", username="errch")

    pool = FakeDirectPool(
        resolve_by_id={
            801: {
                "channel_id": 801,
                "title": "Ok Ch Renamed",
                "username": "okch",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            },
            802: {"gone": True},
            803: ValueError("Could not find the input entity"),
        },
        meta_by_id={801: {"about": "a", "linked_chat_id": None, "has_comments": False}},
    )
    _run_refresh_card(db, pool, all_channels=True)

    out = capsys.readouterr().out
    # Per-channel result for every channel, then the totals.
    assert "801" in out and "802" in out and "803" in out
    assert "Updated: 1" in out
    assert "Skipped: 1" in out
    assert "Failed: 1" in out

    assert _get_channel(db, 801).title == "Ok Ch Renamed"
    assert _get_channel(db, 802).title == "Gone Ch"  # untouched
    assert _get_channel(db, 803).title == "Err Ch"  # untouched


def test_refresh_card_bulk_covers_only_active_channels(cli_env):
    db = cli_env
    _add_stale_channel(db, channel_id=811, title="Active", username="act")
    _add_stale_channel(db, channel_id=812, title="Inactive", username="inact")
    ch = _get_channel(db, 812)
    asyncio.run(db.repos.channels.set_channel_active(ch.id, False, origin="human"))

    pool = FakeDirectPool(
        resolve_by_id={
            811: {
                "channel_id": 811,
                "title": "Active",
                "username": "act",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            }
        },
        meta_by_id={811: {"about": "a", "linked_chat_id": None, "has_comments": False}},
    )
    _run_refresh_card(db, pool, all_channels=True)
    assert [call[0] for call in pool.resolve_calls] == ["811"]  # inactive never queried


def test_refresh_card_single_channel_not_found(cli_env, capsys):
    db = cli_env
    pool = FakeDirectPool(resolve_by_id={}, meta_by_id={})
    _run_refresh_card(db, pool, identifier="@ghost")
    assert "not found" in capsys.readouterr().out


def test_refresh_card_backfills_created_at_only_when_missing(cli_env):
    db = cli_env
    _add_stale_channel(db, channel_id=831, title="B1", username="b1")
    _add_stale_channel(db, channel_id=832, title="B2", username="b2")
    existing = datetime(2021, 5, 5, tzinfo=timezone.utc)
    asyncio.run(db.repos.channels.update_channel_created_at(832, existing))

    created = datetime(2020, 3, 3, tzinfo=timezone.utc)

    def _card(cid: int) -> dict:
        return {
            "channel_id": cid,
            "title": "B",
            "username": "b",
            "channel_type": "channel",
            "deactivate": False,
            "created_at": created,
        }

    pool = FakeDirectPool(
        resolve_by_id={831: _card(831), 832: _card(832)},
        meta_by_id={831: {"about": "a", "linked_chat_id": None, "has_comments": False},
                    832: {"about": "a", "linked_chat_id": None, "has_comments": False}},
    )
    _run_refresh_card(db, pool, all_channels=True)

    assert _get_channel(db, 831).created_at == created  # NULL → backfilled
    assert _get_channel(db, 832).created_at == existing  # never overwritten


# ---------------------------------------------------------------------------
# update_channel_card repo semantics: one atomic statement (#1765 review pt 3)
# ---------------------------------------------------------------------------


async def test_update_channel_card_meta_absent_keeps_stored_about(db):
    await db.repos.channels.add_channel(
        Channel(channel_id=860, title="T", username="u", channel_type="channel",
                about="old about", linked_chat_id=7, has_comments=True)
    )
    created = datetime(2020, 1, 2, tzinfo=timezone.utc)

    await db.repos.channels.update_channel_card(
        860, username=None, title="New T", channel_type=None,
        created_at=created, meta_present=False,
    )

    row = await db.repos.channels.get_channel_by_channel_id(860)
    assert row.username is None  # confirmed absence written
    assert row.username_state == "known"
    assert row.title == "New T"
    assert row.channel_type == "channel"  # NULL type hint keeps stored value
    assert row.about == "old about"  # meta part absent → untouched
    assert row.linked_chat_id == 7
    assert row.has_comments is True
    assert row.created_at == created  # NULL → backfilled


async def test_update_channel_card_meta_present_overwrites_and_backfill_never_downgrades(db):
    await db.repos.channels.add_channel(
        Channel(channel_id=861, title="T", username="u", channel_type="channel")
    )
    created = datetime(2020, 1, 2, tzinfo=timezone.utc)
    await db.repos.channels.update_channel_card(
        861, username="u", title="T", channel_type="supergroup",
        created_at=created, meta_present=False,
    )
    row = await db.repos.channels.get_channel_by_channel_id(861)
    assert row.created_at == created
    assert row.channel_type == "supergroup"

    # meta part present: confirmed absence in the full-channel payload is written.
    await db.repos.channels.update_channel_card(
        861, username="u", title="T", channel_type="supergroup",
        created_at=datetime(1999, 1, 1, tzinfo=timezone.utc),  # older — must NOT overwrite
        about=None, linked_chat_id=None, has_comments=False, meta_present=True,
    )
    row = await db.repos.channels.get_channel_by_channel_id(861)
    assert row.about is None
    assert row.linked_chat_id is None
    assert row.has_comments is False
    assert row.created_at == created  # COALESCE: existing value never downgraded
