"""A write by a moved username reaches its current owner (#1843) — offline.

Writes resolve cached usernames from the session store without asking Telegram
(#1839). Telethon's in-memory store used to keep the previous owner's row next
to the new one, so the lookup — and the write — could land on either. Pool
clients now use ``FreshEntityStringSession`` (``tg_shared.sessions``).

Same discipline as the other resolver-family files: a REAL ``TelegramClient``
whose MTProto sender is a recording stub; nothing is stubbed in resolution.
"""

from datetime import datetime, timezone

import pytest
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.types import Channel, UpdateShortSentMessage

from tests.helpers import sender_responder_to_send
from tg_agent.services.telegram_actions import TelegramActionService
from tg_shared.sessions import FreshEntityStringSession

PHONE = "+70000000843"
SESSION = "session-username-move"
CHAT_A = 8000000001
CHAT_B = 8000000002
USERNAME = "moved_name"


def _channel(channel_id, username):
    return Channel(
        id=channel_id, title="chat", username=username, access_hash=channel_id + 1, photo=None, date=None
    )


def make_recording_client():
    """A real TelegramClient on the store pool clients get, with a recording sender."""
    from telethon import TelegramClient

    client = TelegramClient(FreshEntityStringSession(), 1, "x")
    dispatched = []

    def send(request, ordered=False):
        dispatched.append(request)
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77, date=datetime.now(timezone.utc), out=True, pts=1, pts_count=1
            )
        return None

    # The dispatch gate inspects sender.send's Future (MTProto contract).
    client._sender.send = sender_responder_to_send(send)
    # Offline: never attempt a real connect() from the pool's liveness check.
    client.is_connected = lambda: True
    return client, dispatched


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
@pytest.mark.parametrize("previous_owner_seen_again", [True, False])
async def test_write_by_a_moved_username_reaches_the_current_owner(
    real_pool_harness_factory, previous_owner_seen_again
):
    """The username moved from A to B while the client was alive; the write goes to B."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client()
    client.session.process_entities([_channel(CHAT_A, USERNAME)])
    refreshed = [_channel(CHAT_B, USERNAME)]
    if previous_owner_seen_again:
        refreshed.append(_channel(CHAT_A, None))
    client.session.process_entities(refreshed)

    harness.queue_cli_client(phone=PHONE)
    harness.queue_native_client(session_string=SESSION, client=client)
    await harness.add_account(PHONE, session_string=SESSION, is_primary=True)
    await harness.initialize_connected_accounts()
    actions = TelegramActionService(harness.pool)

    result = await actions.send_message(phone=PHONE, recipient=f"@{USERNAME}", text="hi")

    assert result.message_id == 77
    # One request, no re-resolve — and it is addressed to the current owner.
    assert [type(request).__name__ for request in dispatched] == ["SendMessageRequest"]
    assert dispatched[0].peer.channel_id == CHAT_B

    # The previous owner is still reachable by id without a request.
    dispatched.clear()
    await actions.send_message(phone=PHONE, recipient=str(CHAT_A), text="hi")
    assert [type(request).__name__ for request in dispatched] == ["SendMessageRequest"]
    assert dispatched[0].peer.channel_id == CHAT_A
