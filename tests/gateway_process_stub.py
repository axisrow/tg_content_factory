"""Offline Telegram behind a real gateway process, for the multi-process worker scenario.

Since #1868 project workers and CLIs reach the gateway's connection through the
relay: they send raw MTProto requests and parse the raw answer themselves. This
stub stands in for that connection — it answers the TL requests a collection run
and ``dialogs send`` make with serialized TL results, the way Telethon's sender
hands back raw bytes once the relay swaps in its reader. Requests it does not
know are journaled to ``test-unknown.json`` and refused with an RPC error.
"""

import asyncio
import inspect
import json
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path

from telethon import functions, types
from telethon.errors import RPCError
from telethon.extensions import BinaryReader
from telethon.sessions import MemorySession

from tg_agent.gateway.runtime import serve

VECTOR = 0x1CB5C415
DATE = datetime(2026, 10, 29, tzinfo=timezone.utc)
CHANNEL = types.Channel(id=123, title="source", photo=types.ChatPhotoEmpty(), date=DATE, access_hash=99,
                        username="source", broadcast=True)
SELF = types.User(id=1000, is_self=True, access_hash=1, first_name="Gateway", phone="10000000000")
POSTS = (5, 7)


def _tl_bytes(result) -> bytes:
    if isinstance(result, list):
        return struct.pack("<Ii", VECTOR, len(result)) + b"".join(bytes(item) for item in result)
    if result is True:
        return bytes(types.BoolTrue())
    return bytes(result)


def _post(message_id: int) -> types.Message:
    return types.Message(id=message_id, peer_id=types.PeerChannel(123), date=DATE, message=f"post {message_id}",
                         post=True)


def _dialog() -> types.Dialog:
    # Unread counters come and go between TL layers; zero every one this layer requires.
    counters = {name: 0 for name in inspect.signature(types.Dialog.__init__).parameters if name.startswith("unread_")}
    return types.Dialog(peer=types.PeerChannel(123), top_message=POSTS[-1], read_inbox_max_id=0,
                        read_outbox_max_id=0, notify_settings=types.PeerNotifySettings(), **counters)


def _history(request) -> types.messages.ChannelMessages:
    if request.add_offset < 0:  # reverse iteration: the page starting at offset_id
        ids = [mid for mid in POSTS if mid >= request.offset_id]
    else:
        ids = [mid for mid in POSTS if not request.offset_id or mid < request.offset_id]
    ids = sorted((mid for mid in ids if mid > request.min_id), reverse=True)[:request.limit]
    return types.messages.ChannelMessages(pts=1, count=len(POSTS), messages=[_post(mid) for mid in ids],
                                          topics=[], chats=[CHANNEL], users=[])


class FakeTelegram:
    """The gateway-side connection: answers like a relayed Telethon client."""

    def __init__(self, root: Path):
        self.root = root
        self.session = MemorySession()
        self.session.process_entities(types.contacts.ResolvedPeer(peer=types.PeerChannel(123), chats=[CHANNEL],
                                                                  users=[SELF]))

    def is_connected(self) -> bool:
        return True

    def _journal(self, name: str, entry) -> None:
        path = self.root / name
        entries = json.loads(path.read_text()) if path.exists() else []
        path.write_text(json.dumps([*entries, entry]))

    def _answer(self, request):
        if isinstance(request, functions.updates.GetStateRequest):
            return types.updates.State(pts=1, qts=0, date=DATE, seq=0, unread_count=0)
        if isinstance(request, functions.users.GetUsersRequest):
            return [SELF]
        if isinstance(request, functions.messages.GetDialogsRequest):
            return types.messages.Dialogs(dialogs=[_dialog()], messages=[_post(POSTS[-1])], chats=[CHANNEL],
                                          users=[])
        if isinstance(request, functions.contacts.ResolveUsernameRequest):
            return types.contacts.ResolvedPeer(peer=types.PeerChannel(123), chats=[CHANNEL], users=[])
        if isinstance(request, functions.channels.GetChannelsRequest):
            return types.messages.Chats(chats=[CHANNEL])
        if isinstance(request, functions.messages.GetHistoryRequest):
            return _history(request)
        if isinstance(request, functions.messages.SendMessageRequest):
            (self.root / "test-sent.json").write_text(json.dumps({"text": request.message}))
            return types.UpdateShortSentMessage(id=88, pts=2, pts_count=1, date=DATE, out=True)
        self._journal("test-unknown.json", type(request).__name__)
        raise RPCError(request, "UNSUPPORTED_BY_STUB", 400)

    async def __call__(self, request):
        with BinaryReader(_tl_bytes(self._answer(request))) as reader:
            return request.read_result(reader)


class Connection:
    def __init__(self, raw_client: FakeTelegram):
        self.raw_client = raw_client


class TelegramStub:
    """``access_factory`` for :func:`serve`: one fake connection per shared account."""

    def __init__(self, store):
        self.store = store
        self.connections: dict[str, Connection] = {}

    def account_status(self, account):
        return {"connected": account in self.connections}

    async def connection(self, project, account):
        await self.store.session(project, account)  # the grant is checked like the live access does
        if account not in self.connections:
            self.connections[account] = Connection(FakeTelegram(self.store.root))
            (self.store.root / "test-connections.json").write_text(json.dumps(sorted(self.connections)))
        return self.connections[account]

    async def close(self):
        pass


if __name__ == "__main__":
    asyncio.run(serve(Path(sys.argv[1]), access_factory=TelegramStub))
