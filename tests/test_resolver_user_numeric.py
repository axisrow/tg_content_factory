"""Numeric-id resolution order (#1830) — offline, real request resolution.

A numeric user id must not cost a channel-first `GetDialogs` sweep, one action
must never request more than one sweep, and a caller that knows the target is
a user says so. Same test discipline as the id-form family: a REAL
``TelegramClient`` whose MTProto sender is a recording stub; TL requests are
built by real Telethon code; ``get_entity`` / ``get_input_entity`` are never
stubbed; the only replaced behavior is ``is_connected()`` (offline test).
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from telethon.errors.rpcerrorlist import ChannelInvalidError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.messages import GetDialogsRequest, SendMessageRequest
from telethon.tl.functions.users import GetUsersRequest
from telethon.tl.types import (
    UpdateShortSentMessage,
    User,
    UserEmpty,
)
from telethon.tl.types import (
    messages as tl_messages,
)
from telethon.tl.types import (
    users as tl_users,
)

from tg_agent.services.ping_monitor_service import PingMonitorService
from tg_agent.services.telegram_actions import (
    TelegramActionEntityResolutionError,
    TelegramActionService,
)

PHONE = "+70000000283"
SESSION = "session-user-numeric"
USER_ID = 42


def make_recording_client(*, seeded=()):
    from telethon import TelegramClient

    client = TelegramClient(StringSession(), 1, "x")
    dispatched = []

    async def send(request, ordered=False):
        dispatched.append(request)
        if isinstance(request, GetChannelsRequest):
            raise ChannelInvalidError(request=None)
        if type(request).__name__ == "ImportContactsRequest":
            # Telethon resolves a phone recipient by (re-)importing the
            # contact; the seeded contact answers with the same user.
            from telethon.tl.types import (
                contacts as tl_contacts,
            )

            return tl_contacts.ImportedContacts(
                imported=[
                    tl_contacts.ImportedContact(user_id=USER_ID, client_id=0)
                ],
                users=[_contact_entity()],
                retry_contacts=[],
            )
        if type(request).__name__ == "GetContactsRequest":
            # get_entity("+79…") scans the contacts list for the phone.
            from telethon.tl.types import (
                contacts as tl_contacts,
            )

            return tl_contacts.Contacts(contacts=[], saved_count=0, users=[_contact_entity()])
        if isinstance(request, GetUsersRequest):
            # The seeded contact resolves; a foreign id stays a verified miss
            # (UserEmpty), exactly like Telegram would answer.
            wanted = request.id[0].user_id
            if wanted == USER_ID:
                return tl_users.Users(
                    users=[User(id=USER_ID, first_name="penpal", access_hash=1)]
                )
            return tl_users.Users(users=[UserEmpty()])
        if isinstance(request, GetDialogsRequest):
            return tl_messages.Dialogs(dialogs=[], users=[], chats=[], messages=[])
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77, date=datetime.now(timezone.utc), out=True, pts=1, pts_count=1
            )
        return None

    def dispatch(request, ordered=False):
        if isinstance(request, list):
            return [asyncio.ensure_future(send(item, ordered=ordered)) for item in request]
        return asyncio.ensure_future(send(request, ordered=ordered))

    client._sender.send = dispatch
    client.is_connected = lambda: True
    for entity in seeded:
        client.session.process_entities([entity])
    return client, dispatched


def _user_entity():
    return User(id=USER_ID, first_name="penpal", access_hash=1)


def _contact_entity():
    # Telegram stores contact phones without the '+' prefix.
    return User(id=USER_ID, first_name="penpal", phone="79991234567", access_hash=1)


async def _connect_native(harness, client):
    harness.queue_cli_client(phone=PHONE)
    harness.queue_native_client(session_string=SESSION, client=client)
    await harness.add_account(PHONE, session_string=SESSION, is_primary=True)
    await harness.initialize_connected_accounts()
    session, phone = await harness.pool.get_native_client_by_phone(PHONE)
    assert session is not None
    return session, phone


def _request_names(dispatched):
    return [type(request).__name__ for request in dispatched]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_numeric_user_known_to_session_needs_no_sweep(real_pool_harness_factory):
    """send_message(..., is_user=True) to a known user id: the send only."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(seeded=[_user_entity()])
    session, phone = await _connect_native(harness, client)

    actions = TelegramActionService(harness.pool)
    result = await actions.send_message(phone=phone, recipient=str(USER_ID), text="hi", is_user=True)

    assert result.message_id == 77
    assert _request_names(dispatched) == ["SendMessageRequest"]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_unknown_numeric_target_costs_at_most_one_sweep(real_pool_harness_factory):
    """A failing numeric action asks for one dialog sweep, not one per candidate."""
    harness = real_pool_harness_factory()
    # Neutralize the local dialogs gate: it would refuse a second sweep before
    # any RPC and mask the count this test is about (the fix must make the
    # second/third warm structurally absent, not merely gated).
    harness.pool._rate_limit_gate = None
    client, dispatched = make_recording_client()
    session, phone = await _connect_native(harness, client)

    actions = TelegramActionService(harness.pool)
    with pytest.raises(TelegramActionEntityResolutionError):
        await actions.send_message(phone=phone, recipient="9999999", text="hi")

    sweeps = [r for r in dispatched if isinstance(r, GetDialogsRequest)]
    assert len(sweeps) == 1


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_ping_monitor_send_declares_user_kind(real_pool_harness_factory):
    """The ping monitor's nudge goes out as a user-kind numeric send (#1830)."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(seeded=[_user_entity()])
    session, phone = await _connect_native(harness, client)

    from unittest.mock import AsyncMock, patch

    from tg_agent.models import DialogMessage, MessagesResult, PingMonitor

    now = datetime.now(timezone.utc)
    monitor = PingMonitor(
        id=1,
        phone=phone,
        dialog_id=USER_ID,
        silence_threshold_days=30,
        nudge_text="пинг",
        cycle_count=0,
        is_active=True,
    )
    repo = AsyncMock()
    repo.list = AsyncMock(return_value=[monitor])
    repo.update = AsyncMock()
    frozen = patch(
        "tg_agent.services.ping_monitor_service.datetime",
        **{"now.return_value": now + timedelta(days=31)},
    )
    actions = TelegramActionService(harness.pool)
    with patch(
        "tg_agent.services.telegram_actions.TelegramActionService.read_history",
        new_callable=AsyncMock,
        return_value=MessagesResult(
            phone=phone,
            dialog_id=USER_ID,
            messages=[
                DialogMessage(
                    id=10,
                    dialog_id=USER_ID,
                    out=True,
                    date=now - timedelta(days=31, minutes=5),
                    text="...",
                )
            ],
        ),
    ):
        service = PingMonitorService(repo, actions)
        with frozen:
            checked = await service.run_due()

    assert checked == 1
    # The nudge reached the known user with zero resolve RPCs — no channel
    # probe, no dialog sweep, only the send itself.
    assert _request_names(dispatched) == ["SendMessageRequest"]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_phone_number_recipient_is_not_a_numeric_id(real_pool_harness_factory):
    """A '+79…' recipient is a phone, not a Telegram id: it must go through
    the string path, where Telethon resolves contacts (#1830 review follow-up).
    Before the fix the numeric gate captured it and raised unconditionally."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(seeded=[_contact_entity()])
    session, phone = await _connect_native(harness, client)

    actions = TelegramActionService(harness.pool)
    result = await actions.send_message(phone=phone, recipient="+79991234567", text="hi")

    assert result.message_id == 77
    names = _request_names(dispatched)
    assert "GetDialogsRequest" not in names
    assert "SendMessageRequest" in names
