"""Tests for `messages wait`: the deletion verdict over the local store (step 2)."""

from __future__ import annotations

import asyncio
import json

import pytest
import typer


def _seed(db, phone: str, chat_id: int | None, message_id: int):
    asyncio.run(db.repos.observed_deletions.record_many(phone, chat_id, [message_id]))


def _wait(message_id: int, *, phone: str | None = None, chat: int | None = None, timeout: int = 0):
    from tg_agent.cli.commands.messages import messages_wait_impl

    asyncio.run(
        messages_wait_impl(
            "config.yaml",
            message_id=message_id,
            phone=phone,
            chat=chat,
            timeout=timeout,
        )
    )


def test_a_recorded_deletion_prints_the_verdict_and_exits_zero(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", None, 41)

    _wait(41, phone="+7001")

    out = capsys.readouterr().out
    assert "Deleted: message_id=41" in out
    assert "via=+7001" in out


def test_an_unrecorded_message_stays_alive_and_exits_one(cli_env, cli_db, capsys):
    with pytest.raises(typer.Exit) as excinfo:
        _wait(41, phone="+7001")
    assert excinfo.value.exit_code == 1
    # Exit 1 reports on stderr, per the #1486 exit-code contract.
    assert "Alive: no deletion recorded for message_id=41" in capsys.readouterr().err


def test_a_chat_filter_matches_that_chat(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -1001525693788, 5)

    # The chat id arrives in the form Telegram reports (marked -100…).
    _wait(5, chat=-1001525693788)
    assert "Deleted: message_id=5" in capsys.readouterr().out


def test_a_bare_channel_id_matches_a_marked_deletion(cli_env, cli_db, capsys):
    """#1926: the bare id `messages read` prints must find the -100… store row."""
    _seed(cli_db, "+7001", -1001525693788, 5)

    _wait(5, chat=1525693788)
    assert "Deleted: message_id=5 chat=-1001525693788" in capsys.readouterr().out


def test_a_wrong_chat_never_matches(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -1001525693788, 5)

    with pytest.raises(typer.Exit) as excinfo:
        _wait(5, chat=-1009999999999)
    assert excinfo.value.exit_code == 1


def test_a_phone_without_a_chat_demands_the_dm_shape(cli_env, cli_db, capsys):
    # Only a channel row exists; a DM-shaped lookup (phone, no --chat) must not see it.
    _seed(cli_db, "+7001", -1001525693788, 5)

    with pytest.raises(typer.Exit) as excinfo:
        _wait(5, phone="+7001")
    assert excinfo.value.exit_code == 1


def test_without_phone_or_chat_any_row_matches(cli_env, cli_db, capsys):
    _seed(cli_db, "+7002", -1001525693788, 7)

    _wait(7)
    out = capsys.readouterr().out
    assert "Deleted: message_id=7" in out
    assert "via=+7002" in out


def _register(db, phone: str, chat_id: int | None, message_id: int):
    asyncio.run(
        db.repos.sent_messages.record_many(
            phone,
            chat_id,
            [message_id],
            chat_type="channel" if chat_id is not None else "dm",
            source="dialogs_send",
        )
    )


def test_a_registered_channel_send_is_judged_in_its_own_chat(cli_env, cli_db, capsys):
    """#1919: `--message-id N --phone <sender>` — the recipe printed next to `dialogs send`."""
    _register(cli_db, "+7001", -1001525693788, 5)
    # A peer account saw the deletion — the sender did not.
    _seed(cli_db, "+7002", -1001525693788, 5)

    _wait(5, phone="+7001")

    out = capsys.readouterr().out
    assert "Deleted: message_id=5 chat=-1001525693788 via=+7002" in out


def test_another_chats_deletion_of_the_same_id_never_lands_on_a_registered_send(cli_env, cli_db, capsys):
    """#1919: message ids are per-chat; a bare id used to match any chat's row."""
    _register(cli_db, "+7001", -1001525693788, 5)
    _seed(cli_db, "+7002", -1009999999999, 5)  # another chat, same number

    with pytest.raises(typer.Exit) as excinfo:
        _wait(5)
    assert excinfo.value.exit_code == 1
    assert "Alive: no deletion recorded for message_id=5" in capsys.readouterr().err


def test_a_registered_dm_send_still_matches_by_the_senders_phone(cli_env, cli_db, capsys):
    """#1919: NULL-chat rows keep the DM rule — the sender's own account saw it."""
    _register(cli_db, "+7001", None, 41)
    _seed(cli_db, "+7001", None, 41)

    _wait(41, phone="+7001")
    assert "Deleted: message_id=41" in capsys.readouterr().out


def test_a_registered_comment_is_judged_in_the_discussion_group(cli_env, cli_db, capsys):
    """#1946: a comment's id lives in the channel's discussion group — the row's
    chat is the group, and no `--chat` is needed to watch it."""
    group, channel = -1001525693788, -1001525693799
    asyncio.run(
        cli_db.repos.sent_messages.record_many(
            "+7001", group, [777], chat_type="comment", source="dialogs_send",
            recipient=str(channel), ref="42",
        )
    )
    _seed(cli_db, "+7002", group, 777)  # a peer saw the group's message go

    _wait(777, phone="+7001")

    out = capsys.readouterr().out
    assert "Deleted: message_id=777 chat=-1001525693788 via=+7002" in out


# --------------------------------------------------------------------------- #
# incoming long-poll mode (#1871): settle debounce, chat filter, both producers
# --------------------------------------------------------------------------- #

import time  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from types import SimpleNamespace  # noqa: E402
from unittest.mock import AsyncMock, MagicMock, patch  # noqa: E402

from tg_agent.telegram.incoming_wait import (  # noqa: E402
    IncomingBurst,
    WaitTimeoutError,
    build_burst,
    normalize_incoming,
    wait_burst,
)

PHONE = "+10000000001"
CHAT_ID = -1001525693788
NOW = datetime(2026, 11, 25, 12, 0, 0, tzinfo=timezone.utc)


class FakeWaitClient:
    """Native-path stand-in: records handler attach/detach."""

    def __init__(self):
        self.handlers: list = []
        self.detached: list = []

    def add_event_handler(self, callback, event=None):
        self.handlers.append(callback)

    def remove_event_handler(self, callback, event=None):
        if callback in self.handlers:
            self.handlers.remove(callback)
        self.detached.append(callback)


class FakeRelayConnection:
    """Relay-path stand-in: records the wait.messages op, exposes on_push."""

    def __init__(self):
        self.push_handler = None
        self.calls: list[tuple[str, dict]] = []

    def on_push(self, handler):
        self.push_handler = handler

    async def call(self, op, **fields):
        self.calls.append((op, fields))
        await asyncio.sleep(30)


def _event(chat_id, message_id, text, *, out=False, title="Тестовый чат", sender_name=None):
    sender = SimpleNamespace(first_name=sender_name) if sender_name else None
    return SimpleNamespace(
        chat_id=chat_id,
        chat=SimpleNamespace(title=title),
        message=SimpleNamespace(out=out, id=message_id, text=text, action=None, date=NOW, sender=sender),
    )


def _wait_incoming_pool(cli_db, *, relay=None):
    from telethon.tl.types import PeerChannel

    pool = MagicMock()
    pool.clients = {PHONE: object()}
    pool.relay = relay
    wait_client = FakeWaitClient()
    pool.get_native_client_by_phone = AsyncMock(return_value=(wait_client, PHONE))
    # A real TL peer: marked_peer_id() runs the actual telethon mapping, so the
    # resolved -100… id equals the event chat ids the tests feed.
    pool.resolve_entity_with_warm = AsyncMock(return_value=PeerChannel(channel_id=1525693788))
    pool.disconnect_all = AsyncMock()

    async def fake_init_db(_):
        from tg_agent.config import AppConfig

        return AppConfig(), cli_db

    async def fake_init_pool(_, __):
        return MagicMock(), pool

    return pool, wait_client, fake_init_db, fake_init_pool


async def _drive_incoming(cli_db, *, chat=str(CHAT_ID), timeout=0.5, settle_ms=50,
                          json_output=True, phone=PHONE, feed=None, relay=None):
    """Run the incoming impl as a task while ``feed`` produces events."""
    from tg_agent.cli.commands.messages import messages_wait_incoming_impl

    pool, wait_client, fake_init_db, fake_init_pool = _wait_incoming_pool(cli_db, relay=relay)

    async def scenario():
        task = asyncio.create_task(
            messages_wait_incoming_impl(
                "config.yaml",
                chat=chat,
                phone=phone,
                timeout=timeout,
                settle_ms=settle_ms,
                json_output=json_output,
            )
        )
        if feed is not None:
            await feed(wait_client, pool)
        try:
            await asyncio.wait_for(task, timeout=10)
        except typer.Exit as exc:
            assert exc.exit_code == 0
        return pool, wait_client

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        return await scenario()


async def _attached(client):
    """The handler the impl attached, bounded — a missing attach fails fast."""
    for _ in range(200):
        if client.handlers:
            return client.handlers[0]
        await asyncio.sleep(0.005)
    raise AssertionError("the incoming handler never attached")


async def test_three_rapid_messages_settle_into_one_burst(cli_db, capsys):
    async def feed(client, pool):
        handler = await _attached(client)
        await handler(_event(CHAT_ID, 10, "первое"))
        await handler(_event(CHAT_ID, 11, None))  # media-only line of the burst
        await handler(_event(CHAT_ID, 12, "третье"))

    await _drive_incoming(cli_db, feed=feed)

    record = json.loads(capsys.readouterr().out)
    assert record["event"] is True
    assert record["chat_id"] == CHAT_ID
    assert record["name"] == "Тестовый чат"
    assert record["phone"] == PHONE
    assert record["message_count"] == 3
    assert record["first_message_id"] == 10
    assert record["last_message_id"] == 12
    assert record["texts"] == ["первое", None, "третье"]


async def test_foreign_chat_and_own_messages_do_not_wake(cli_db, capsys):
    async def feed(client, pool):
        handler = await _attached(client)
        await handler(_event(-1009999999999, 1, "чужой чат"))
        await handler(_event(CHAT_ID, 2, "своё", out=True))
        await handler(_event(CHAT_ID, 3, "успех"))
        # settling: nothing else arrives within the window

    await _drive_incoming(cli_db, feed=feed)

    record = json.loads(capsys.readouterr().out)
    assert record["message_count"] == 1
    assert record["texts"] == ["успех"]


async def test_relayed_pool_pushes_bursts_over_wait_messages_op(cli_db, capsys):
    relay = FakeRelayConnection()

    async def feed(client, pool):
        for _ in range(200):
            if relay.push_handler is not None:
                break
            await asyncio.sleep(0.005)
        else:
            raise AssertionError("the wait.messages op never subscribed")
        item = {
            "phone": PHONE, "chat_id": CHAT_ID, "chat_title": "Тестовый чат",
            "message_id": 20, "sender_name": "@peer", "text": "по релею", "date": NOW.isoformat(),
        }
        relay.push_handler({"push": "wait.message", **item})
        relay.push_handler({"push": "wait.message", **{**item, "message_id": 21, "text": "ещё"}})

    await _drive_incoming(cli_db, feed=feed, relay=relay, timeout=5)

    record = json.loads(capsys.readouterr().out)
    assert record["message_count"] == 2
    assert record["texts"] == ["по релею", "ещё"]
    op, fields = relay.calls[0]
    assert op == "wait.messages"
    assert fields["phone"] == PHONE
    assert fields["chat_id"] == CHAT_ID
    # The owner window covers the CLI timeout plus the settle tail plus grace.
    assert fields["timeout_sec"] == 5 + 0.05 + 5.0
    # A newer owner follows the burst by the two deadlines instead (#1981).
    assert fields["first_wait_sec"] == 5
    assert fields["settle_sec"] == 0.05


def test_incoming_timeout_is_machine_readable_and_exits_one(cli_env, cli_db, capsys):
    from tg_agent.cli.commands.messages import messages_wait_incoming_impl

    pool, wait_client, fake_init_db, fake_init_pool = _wait_incoming_pool(cli_db)

    async def scenario():
        await messages_wait_incoming_impl(
            "config.yaml", chat=str(CHAT_ID), phone=PHONE, timeout=0.1,
            settle_ms=50, json_output=True,
        )

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(scenario())

    assert excinfo.value.exit_code == 1
    assert json.loads(capsys.readouterr().out) == {"event": False, "reason": "timeout"}


def test_an_unresolvable_chat_names_the_account_and_hints_phone(cli_env, cli_db, capsys):
    from tg_agent.cli.commands.messages import messages_wait_incoming_impl

    pool, wait_client, fake_init_db, fake_init_pool = _wait_incoming_pool(cli_db)
    pool.resolve_entity_with_warm = AsyncMock(side_effect=RuntimeError("peer not found"))

    async def scenario():
        await messages_wait_incoming_impl(
            "config.yaml", chat="@nobody", phone=PHONE, timeout=1, settle_ms=50, json_output=True,
        )

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(scenario())

    assert excinfo.value.exit_code == 1
    err = capsys.readouterr().err
    assert "Cannot resolve '@nobody' via account +10000000001" in err
    assert "retry with --phone" in err


# --- burst collector unit level ------------------------------------------- #

def _item(message_id, text="x"):
    return {
        "phone": PHONE, "chat_id": CHAT_ID, "chat_title": "ч",
        "message_id": message_id, "sender_name": None, "text": text, "date": None,
    }


def test_wait_burst_collects_until_the_chat_goes_quiet():
    async def scenario():
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait(_item(1))
        queue.put_nowait(_item(2))

        async def late():
            await asyncio.sleep(0.05)
            queue.put_nowait(_item(3))

        task = asyncio.create_task(late())
        burst = await wait_burst(queue, timeout=1, settle_ms=200)
        await task
        return burst

    burst = asyncio.run(scenario())
    assert isinstance(burst, IncomingBurst)
    assert burst.message_count == 3
    assert burst.message_ids == [1, 2, 3]


def test_any_mode_bursts_only_the_first_messages_chat():
    """--chat any: foreign-chat arrivals during the settle window are dropped (review)."""
    other = dict(_item(99, "другой чат"))
    other["chat_id"] = -1009999999999

    async def scenario():
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait(_item(1, "первый"))
        queue.put_nowait(other)
        queue.put_nowait(_item(2, "тот же чат"))
        return await wait_burst(queue, timeout=1, settle_ms=100)

    burst = asyncio.run(scenario())
    assert burst.chat_id == CHAT_ID
    assert burst.message_count == 2
    assert burst.texts == ["первый", "тот же чат"]


def test_a_foreign_chat_does_not_reset_the_settle_timer():
    """A busy inbox must not stretch one burst: only matching arrivals reset."""

    async def scenario():
        queue: asyncio.Queue = asyncio.Queue()

        async def noisy_inbox():
            foreign = dict(_item(50, "шум"))
            foreign["chat_id"] = -1009999999999
            await asyncio.sleep(0.02)
            queue.put_nowait(foreign)
            await asyncio.sleep(0.02)
            queue.put_nowait(dict(foreign, message_id=51))

        noise = asyncio.create_task(noisy_inbox())
        queue.put_nowait(_item(1))
        started = time.monotonic()
        burst = await wait_burst(queue, timeout=1, settle_ms=80)
        elapsed = time.monotonic() - started
        await noise
        return burst, elapsed

    burst, elapsed = asyncio.run(scenario())
    assert burst.message_count == 1  # the foreign noise never joined
    # Two foreign arrivals inside the window did not extend it past the settle
    # deadline (80ms) by more than scheduling slack.
    assert elapsed < 0.4


def test_wait_burst_times_out_without_events():
    async def scenario():
        queue: asyncio.Queue = asyncio.Queue()
        return await wait_burst(queue, timeout=0.05, settle_ms=10)

    with pytest.raises(WaitTimeoutError):
        asyncio.run(scenario())


def test_wait_burst_reraises_producer_failures():
    async def scenario():
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait(RuntimeError("owner gone"))
        return await wait_burst(queue, timeout=1, settle_ms=10)

    with pytest.raises(RuntimeError, match="owner gone"):
        asyncio.run(scenario())


def test_normalize_incoming_drops_own_and_service_messages():
    assert normalize_incoming(PHONE, _event(CHAT_ID, 1, "x", out=True)) is None
    service = _event(CHAT_ID, 2, None)
    service.message.action = SimpleNamespace()
    assert normalize_incoming(PHONE, service) is None
    kept = normalize_incoming(PHONE, _event(CHAT_ID, 3, "текст", sender_name="Аня"))
    assert kept is not None
    assert kept.sender_name == "Аня"
    assert kept.date == NOW.isoformat()
    assert build_burst([kept]).to_json()["event"] is True


async def _private_chat_events(client, *user_ids: int) -> list:
    """Run real ``UpdateShortMessage`` updates through Telethon's own update loop code."""
    from telethon import events
    from telethon.tl import types

    seen: list = []

    async def _collect(event):
        seen.append(event)

    client.add_event_handler(_collect, events.NewMessage())
    for message_id, user_id in enumerate(user_ids, start=501):
        update = types.UpdateShortMessage(
            id=message_id, user_id=user_id, message="привет", date=NOW,
            pts=message_id, pts_count=1, out=False,
        )
        # The same three steps `TelegramClient._update_loop` runs, in order.
        dispatched: list = []
        users, chats = client._message_box.process_updates(update, client._mb_entity_cache, dispatched)
        for ready in await client._preprocess_updates(dispatched, users, chats):
            await client._dispatch_update(ready)
    return seen


async def test_private_chat_sender_is_named_from_the_session_row():
    """#1986: Telegram delivers a private-chat message without its sender entity.

    ``UpdateShortMessage`` carries no ``users``, so ``message.sender`` is ``None``
    whatever the caches hold. The name the account already keeps for the user in
    its session row — written by every answer that named them — is the only one
    at hand without a request (#1985: a ``GetUsersRequest`` is what the hourly
    ``default`` budget is spent on).
    """
    from telethon import TelegramClient
    from telethon.tl import types

    from tg_shared.sessions import FreshEntityStringSession

    anya, boris, stranger = 7, 8, 9
    client = TelegramClient(FreshEntityStringSession(), api_id=1, api_hash="0")
    client._mb_entity_cache.set_self_user(42, bot=False, hash=1)
    # What Telethon itself stores after an answer that named these users.
    client.session.process_entities(types.contacts.ResolvedPeer(None, [
        types.User(id=anya, bot=False, access_hash=100, first_name="Аня", username="anya"),
        types.User(id=boris, bot=False, access_hash=101, first_name="Борис", last_name="Соколов"),
    ], []))

    calls = AsyncMock(side_effect=AssertionError("a request left the process"))
    with patch.object(TelegramClient, "__call__", calls):
        events = await _private_chat_events(client, anya, boris, stranger)

    # The defect's premise, on the real code path: no update carried the sender.
    assert [event.message.sender for event in events] == [None, None, None]
    record = build_burst([normalize_incoming(PHONE, event) for event in events]).to_json()
    # Same naming as `messages read --live`: @username, else the display name.
    assert record["senders"] == ["@anya", "Борис Соколов", None]
    assert record["senders_unknown"] == (
        "the message came without its sender entity and the account's session holds no name for it"
    )
    calls.assert_not_called()


async def test_no_sender_is_reported_unknown_when_every_sender_is_named():
    from telethon import TelegramClient
    from telethon.tl import types

    from tg_shared.sessions import FreshEntityStringSession

    client = TelegramClient(FreshEntityStringSession(), api_id=1, api_hash="0")
    client._mb_entity_cache.set_self_user(42, bot=False, hash=1)
    client.session.process_entities(types.contacts.ResolvedPeer(None, [
        types.User(id=7, bot=False, access_hash=100, first_name="Аня", username="anya"),
    ], []))
    events = await _private_chat_events(client, 7)
    record = build_burst([normalize_incoming(PHONE, event) for event in events]).to_json()
    assert record["senders"] == ["@anya"]
    assert "senders_unknown" not in record


def test_plain_text_burst_says_why_a_sender_is_unknown(capsys):
    from tg_agent.cli.commands.messages import _print_incoming_burst

    _print_incoming_burst(build_burst([_item(1, "привет"), dict(_item(2, "второе"), sender_name="Аня")]))

    lines = capsys.readouterr().out.splitlines()
    assert lines[1] == (
        "  #1 sender unknown: the message came without its sender entity "
        "and the account's session holds no name for it"
    )
    assert lines[3] == "  #2 Аня"


# --- mode dispatch on the Typer surface ----------------------------------- #

def _invoke_wait(argv):
    from typer.testing import CliRunner

    from tg_agent.cli.typer_app import app

    return CliRunner().invoke(app, ["messages", "wait", *argv])


def test_wait_without_a_mode_names_both_options():
    result = _invoke_wait(["--timeout", "1"])
    assert result.exit_code == 1
    assert "--message-id" in result.output and "--chat" in result.output


def test_wait_message_id_with_chat_is_deletion_with_filter():
    """--chat next to --message-id keeps its #1892 meaning: the deletion chat filter."""
    from tg_agent.cli.commands import messages as messages_cmd

    with patch.object(messages_cmd, "run_async") as run_async_mock:
        result = _invoke_wait(["--message-id", "5", "--chat", "-100123"])
    assert result.exit_code == 0
    coroutine = run_async_mock.call_args.args[0]
    assert coroutine.__name__ == "messages_wait_impl"
    local = coroutine.cr_frame.f_locals
    assert local["chat"] == -100123
    assert local["timeout"] == 30  # deletion default unchanged
    coroutine.close()


def test_wait_deletion_mode_demands_a_numeric_chat():
    result = _invoke_wait(["--message-id", "5", "--chat", "abc"])
    assert result.exit_code == 1
    assert "numeric chat id" in result.output


def test_wait_incoming_mode_dispatches_with_defaults():
    from tg_agent.cli.commands import messages as messages_cmd

    with patch.object(messages_cmd, "run_async") as run_async_mock:
        result = _invoke_wait(["--chat", "any"])
    assert result.exit_code == 0
    coroutine = run_async_mock.call_args.args[0]
    assert coroutine.__name__ == "messages_wait_incoming_impl"
    # Defaults documented in --help (issue #1871): settle 6000 ms, timeout 60 s.
    local = coroutine.cr_frame.f_locals
    assert local["settle_ms"] == 6000
    assert local["timeout"] == 60
    coroutine.close()
