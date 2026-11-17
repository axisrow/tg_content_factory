"""Count real Telethon page and retry dispatches, independently of gates."""
import asyncio
from datetime import datetime, timezone

import pytest
from telethon import TelegramClient, errors, functions, types
from telethon.sessions import MemorySession

from tests.test_rpc_guard_integration import PHONE, make_pool
from tg_agent.telegram.backends import TelegramTransportSession


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["history", "search", "replies", "search_retry"])
async def test_every_physical_page_and_internal_retry_spends_one_ceiling_slot(mode, monkeypatch):
    client = TelegramClient(MemorySession(), 1, "offline")
    pool = make_pool()
    sent = []
    failed = False

    def send(request, ordered=False):
        nonlocal failed
        sent.append(request)
        future = asyncio.get_running_loop().create_future()
        if mode == "search_retry" and not failed:
            failed = True
            future.set_exception(errors.RpcCallFailError(request=request))
            return future
        assert isinstance(request, (functions.messages.GetHistoryRequest,
                                    functions.messages.SearchRequest, functions.messages.GetRepliesRequest))
        high = min(250, (request.offset_id or 251) - 1)
        messages = [types.Message(id=mid, peer_id=types.PeerChannel(100), message="body",
                                  date=datetime.now(timezone.utc))
                    for mid in range(high, max(0, high - request.limit), -1)]
        future.set_result(types.messages.MessagesSlice(count=250, messages=messages,
                                                       chats=[], users=[], topics=[]))
        return future

    async def instant_sleep(seconds):
        pass

    monkeypatch.setattr(asyncio, "sleep", instant_sleep)
    client._sender.send = send
    session = TelegramTransportSession(client, phone=PHONE, pool=pool)
    options = {"limit": 250, "wait_time": 0}
    if mode.startswith("search"):
        options["search"] = "body"
    if mode == "replies":
        options["reply_to"] = 500
    messages = [message async for message in session.stream_messages(types.InputPeerChannel(100, 100), **options)]
    assert [message.id for message in messages] == list(range(250, 0, -1))
    assert len(sent) == (4 if mode == "search_retry" else 3)
    assert pool._outgoing_ceiling._limiter.used("process") == len(sent)
    assert pool._rate_limit_gate.snapshot(PHONE)["categories"]["history"]["used"] == 1
    assert pool._resolve_rate_limiter.used(PHONE) == 0
