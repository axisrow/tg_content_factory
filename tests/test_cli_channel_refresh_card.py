"""`channel refresh-card` — refresh all open channel metadata from Telegram (#1764).

Integration tests over the real :memory: DB with fake pools. Both Telegram
access paths are covered: the direct ClientPool contract (``resolve_channel`` +
``fetch_channel_meta``) and the GatewayPool contract (both delegating to
``inspect_peer`` and returning the full card dict).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import typer

from tg_agent.cli.commands.channel import refresh_card_impl
from tg_agent.cli.commands.messages import messages_read_impl
from tg_agent.config import AppConfig
from tg_agent.models import Channel, Message


def _reopen(db):
    """impl закрывает БД в finally — переоткрыть fixture-инстанс (паттерн test_cli_ping)."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


def _add_stale_channel(db, *, channel_id: int, title: str, username: str | None) -> None:
    asyncio.run(
        db.add_channel(
            Channel(
                channel_id=channel_id,
                title=title,
                username=username,
                channel_type="channel",
                about="old about",
                linked_chat_id=None,
                has_comments=False,
            )
        )
    )


def _seed_messages(db, *, channel_id: int, n: int = 2) -> None:
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
        self.resolve_calls: list[str] = []
        self.meta_calls: list[tuple[int, str | None]] = []

    async def resolve_channel(self, identifier, **kwargs):
        self.resolve_calls.append(str(identifier))
        result = self._resolve_by_id[int(identifier)]
        if isinstance(result, Exception):
            raise result
        return result

    async def fetch_channel_meta(self, channel_id, channel_type=None):
        self.meta_calls.append((channel_id, channel_type))
        result = self._meta_by_id[channel_id]
        if isinstance(result, Exception):
            raise result
        return result

    async def disconnect_all(self):
        pass


class FakeGatewayPool(FakeDirectPool):
    """GatewayPool contract: both calls delegate to inspect_peer and return the
    full card dict (title/username/type/about/linked/comments/created_at)."""

    def __init__(self, cards_by_id):
        # resolve_channel ignores kwargs and returns the full inspect_peer card;
        # fetch_channel_meta returns the same full card (about/linked/comments keys).
        super().__init__(dict(cards_by_id), dict(cards_by_id))


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

    # Telegram side was queried by the STABLE numeric id, never the stale username.
    assert pool.resolve_calls == ["700"]
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
    assert pool.resolve_calls == ["811"]  # inactive channel never queried


# ---------------------------------------------------------------------------
# Gateway path: GatewayPool.resolve_channel / fetch_channel_meta both return the
# full inspect_peer card — same update flow, no data loss
# ---------------------------------------------------------------------------


def test_refresh_card_gateway_path_updates_full_card(cli_env, capsys):
    db = cli_env
    _add_stale_channel(db, channel_id=821, title="Old GW", username="oldgw")
    _seed_messages(db, channel_id=821)

    card = {
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
    pool = FakeGatewayPool({821: card})
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

    pool = FakeGatewayPool({822: GatewayResolveError()})
    _run_refresh_card(db, pool, identifier="822")

    ch = _get_channel(db, 822)
    assert ch.title == "GW Keep"
    assert ch.username == "gwkeep"
    assert "FAILED" in capsys.readouterr().out


class GatewayResolveError(Exception):
    pass


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
