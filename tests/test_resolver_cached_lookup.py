"""Cached peer lookups do not pay the gates of network resolves (#1842) — offline.

Since #1839 a write to a known peer needs no network resolve. Two gates that
exist to protect network resolves still ran for lookups the session store can
answer:

* the per-account username budget — reserved for every username string, so
  writes by cached usernames were refused once 20 of them ran in a minute;
* the resolve-family flood check (#1834) — after a FloodWait on a username
  resolve, a send to a peer already in the session store was refused.

Same discipline as the other resolver-family files: a REAL ``TelegramClient``
whose MTProto sender is a recording stub; ``get_entity`` /
``get_input_entity`` are never stubbed.
"""

from datetime import datetime, timezone

import pytest
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.types import Channel, PeerUser, UpdateShortSentMessage, User
from telethon.tl.types import contacts as tl_contacts

from tests.helpers import sender_responder_to_send
from tg_agent.services.telegram_actions import (
    TelegramActionEntityResolutionError,
    TelegramActionService,
)

PHONE = "+70000000842"
SESSION = "session-cached-lookup"
BARE = 1111111111
MARKED = -1_001_111_111_111
CHANNEL_USERNAME = "known_chan"
STRANGER_ID = 9001
# The burst tier of the username limiter (telethon-floodgate default).
USERNAME_BURST = 20


def make_recording_client(*seeded):
    """A real TelegramClient whose sender records every dispatched request."""
    from telethon import TelegramClient

    client = TelegramClient(StringSession(), 1, "x")
    dispatched = []

    def send(request, ordered=False):
        dispatched.append(request)
        if isinstance(request, ResolveUsernameRequest):
            user = User(
                id=STRANGER_ID, first_name="s", username=request.username, access_hash=3
            )
            client.session.process_entities([user])
            return tl_contacts.ResolvedPeer(peer=PeerUser(user_id=STRANGER_ID), users=[user], chats=[])
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77, date=datetime.now(timezone.utc), out=True, pts=1, pts_count=1
            )
        return None

    # The dispatch gate inspects sender.send's Future (MTProto contract).
    client._sender.send = sender_responder_to_send(send)
    # Offline: never attempt a real connect() from the pool's liveness check.
    client.is_connected = lambda: True
    client.session.process_entities(list(seeded))
    return client, dispatched


def _channel():
    return Channel(
        id=BARE, title="known", username=CHANNEL_USERNAME, access_hash=5, photo=None, date=None
    )


def _penpals(count):
    return [
        User(id=1000 + i, first_name=f"pen{i}", username=f"pen{i}", access_hash=7 + i)
        for i in range(count)
    ]


async def _connect_native(harness, client):
    harness.queue_cli_client(phone=PHONE)
    harness.queue_native_client(session_string=SESSION, client=client)
    await harness.add_account(PHONE, session_string=SESSION, is_primary=True)
    await harness.initialize_connected_accounts()
    session, phone = await harness.pool.get_native_client_by_phone(PHONE)
    assert session is not None
    return phone


def _names(dispatched):
    return [type(request).__name__ for request in dispatched]


def _free_username_slots(pool, phone):
    """How many username resolves the limiter would still admit right now."""
    limiter = pool._get_resolve_rate_limiter()
    free = 0
    while limiter.try_acquire(phone) == 0 and free < 10 * USERNAME_BURST:
        free += 1
    return free


# ---------------------------------------------------------------------------
# Username budget
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_writes_by_cached_usernames_do_not_spend_the_username_budget(
    real_pool_harness_factory,
):
    """25 sends by cached usernames all dispatch and leave the budget untouched.

    Red before the fix: send #21 is refused with ``resolve_username
    rate-limited`` although no ResolveUsername was ever dispatched.
    """
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(*_penpals(25))
    phone = await _connect_native(harness, client)
    actions = TelegramActionService(harness.pool)

    for i in range(25):
        await actions.send_message(phone=phone, recipient=f"@pen{i}", text="hi")

    assert _names(dispatched) == ["SendMessageRequest"] * 25
    assert _free_username_slots(harness.pool, phone) == USERNAME_BURST


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_cache_miss_still_spends_one_username_slot(real_pool_harness_factory):
    """Guard: an unknown username costs one network resolve and one budget unit."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client()
    phone = await _connect_native(harness, client)

    await TelegramActionService(harness.pool).send_message(
        phone=phone, recipient="@stranger", text="hi"
    )

    assert _names(dispatched) == ["ResolveUsernameRequest", "SendMessageRequest"]
    assert _free_username_slots(harness.pool, phone) == USERNAME_BURST - 1


# ---------------------------------------------------------------------------
# Resolve-family flood (#1834)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
@pytest.mark.parametrize(
    "recipient",
    [str(BARE), str(MARKED), f"@{CHANNEL_USERNAME}"],
    ids=["bare-id", "marked-id", "username"],
)
async def test_resolve_family_flood_does_not_block_writes_to_cached_peers(
    real_pool_harness_factory, recipient
):
    """A resolve-side FloodWait leaves publishing to known peers available (#1832).

    Red before the fix: ``Telegram resolve rate-limited`` (numeric forms) or
    ``resolve_username rate-limited`` — zero requests dispatched.
    """
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(_channel())
    phone = await _connect_native(harness, client)
    await harness.pool.report_family_flood(
        phone, 3600, "resolve", operation="telegram_resolve_input_entity"
    )

    result = await TelegramActionService(harness.pool).send_message(
        phone=phone, recipient=recipient, text="hi"
    )

    assert result.message_id == 77
    assert _names(dispatched) == ["SendMessageRequest"]
    assert dispatched[0].peer.channel_id == BARE


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_unknown_username_is_still_refused_during_a_resolve_family_flood(
    real_pool_harness_factory,
):
    """Guard: a lookup that needs the network stays behind the family check."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(_channel())
    phone = await _connect_native(harness, client)
    await harness.pool.report_family_flood(
        phone, 3600, "resolve", operation="telegram_resolve_input_entity"
    )

    with pytest.raises(TelegramActionEntityResolutionError):
        await TelegramActionService(harness.pool).send_message(
            phone=phone, recipient="@stranger", text="hi"
        )

    assert _names(dispatched) == []
