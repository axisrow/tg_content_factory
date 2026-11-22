"""Child-process launcher for the offline gateway send regression."""

import asyncio
import datetime
import json
import os
import runpy
import sys
import time
from pathlib import Path
from unittest.mock import patch


def use_test_clock(offset_file: Path) -> None:
    real_time = time.time
    real_datetime = datetime.datetime

    def offset() -> int:
        return int(offset_file.read_text())

    class ClockType(type):
        def __instancecheck__(cls, instance):
            return isinstance(instance, real_datetime)

    class TestDateTime(real_datetime, metaclass=ClockType):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.now(tz) + datetime.timedelta(seconds=offset())

    # Keep asyncio and subprocess timeout clocks real.
    time.time = lambda: real_time() + offset()
    datetime.datetime = TestDateTime


def run_gateway(shared: Path) -> None:
    from telethon import TelegramClient, errors, functions, types
    from telethon.extensions import BinaryReader

    from tests.gateway_process_stub import DATE, SELF, FakeTelegram, _tl_bytes
    from tests.helpers import sender_responder_to_send
    from tg_agent.gateway.runtime import serve, setup_gateway_logging
    from tg_shared.auth import TelegramAuth

    fake = FakeTelegram(shared)
    recipient = types.User(id=42, access_hash=42, first_name="Offline")
    fake.session.process_entities(types.contacts.ResolvedPeer(peer=types.PeerUser(42), chats=[], users=[recipient]))

    def record(kind: str, **details) -> None:
        with (shared / "transport.jsonl").open("a") as journal:
            journal.write(json.dumps({"kind": kind, "pid": os.getpid(), "at": time.time(), **details}) + "\n")

    def respond(request):
        rpc = request
        while hasattr(rpc, "query"):
            rpc = rpc.query
        if isinstance(rpc, functions.messages.SendMessageRequest):
            flooded = shared / "injected-flood"
            if not flooded.exists():
                flooded.touch()
                record("send", text=rpc.message, outcome="FLOOD_WAIT", seconds=600)
                raise errors.FloodWaitError(request=rpc, capture=600)
            record("send", text=rpc.message, outcome="sent")
            answer = types.UpdateShortSentMessage(id=88, pts=2, pts_count=1, date=DATE, out=True)
        elif isinstance(rpc, functions.users.GetUsersRequest):
            answer = [recipient if getattr(peer, "user_id", None) == recipient.id else SELF for peer in rpc.id]
        elif isinstance(rpc, functions.updates.GetDifferenceRequest):
            answer = types.updates.DifferenceEmpty(date=DATE, seq=0)
        else:
            answer = fake._answer(rpc)
        with BinaryReader(_tl_bytes(answer)) as reader:
            return request.read_result(reader)

    async def create_client(_auth, session_string, *, before_connect=None, base_logger=None):
        assert session_string == "offline-synthetic-session"
        record("connect")
        client = TelegramClient(fake.session, 1, "offline", flood_sleep_threshold=0, base_logger=base_logger)
        client._sender.send = sender_responder_to_send(respond)
        client.is_connected = lambda: True
        if before_connect is not None:
            before_connect(client)
        # Exercise the guarded authorization read normally made during connection.
        await client(functions.users.GetUsersRequest([types.InputUserSelf()]))
        return client

    setup_gateway_logging(shared)
    with patch.object(TelegramAuth, "create_client_from_session", create_client):
        asyncio.run(serve(shared))


if __name__ == "__main__":
    shared = Path(sys.argv[1])
    mode, *args = sys.argv[2:]
    use_test_clock(shared / "clock-offset")
    from telethon.network.mtprotosender import MTProtoSender

    async def reject_network(*_args, **_kwargs):
        raise AssertionError("Real Telegram transport requested in the offline scenario")

    with patch.object(MTProtoSender, "connect", reject_network):
        if mode == "gateway":
            run_gateway(shared)
        else:
            assert mode == "cli"
            sys.argv = ["tg-agent", *args]
            runpy.run_module("tg_agent.main", run_name="__main__")
