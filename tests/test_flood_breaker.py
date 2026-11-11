"""Circuit breaker that stops hammering a flood-waited (operation, phone) (#1330/#1368)."""

from __future__ import annotations

import pytest
from telethon_floodgate import (
    FloodCircuitBreaker,
    TelegramOperationSuspendedError,
)

pytestmark = pytest.mark.telegram_unit

OP = "telegram_warm_dialog_cache"
PHONE = "+1234567890"


# --- wiring into the transport layer ----------------------------------------


@pytest.mark.anyio
async def test_transport_session_suspends_operation_after_repeated_floods():
    """End-to-end: repeated floods stop the call before it reaches Telegram."""
    from unittest.mock import AsyncMock

    from telethon.errors import FloodWaitError

    from tests.helpers import FakeCliTelethonClient
    from tg_agent.telegram.backends import TelegramTransportSession

    err = FloodWaitError(request=None, capture=0)
    err.seconds = 23

    calls = {"n": 0}

    def _always_floods(_arg):
        calls["n"] += 1
        return err

    pool = AsyncMock()
    pool._flood_breaker = FloodCircuitBreaker(threshold=3, cooldown_seconds=300.0)
    pool._rate_limit_gate = None  # gate is orthogonal here

    session = TelegramTransportSession(
        FakeCliTelethonClient(entity_resolver=_always_floods),
        phone="+7000",
        pool=pool,
    )

    from telethon_floodgate import HandledFloodWaitError

    for _ in range(3):
        with pytest.raises(HandledFloodWaitError):
            await session.get_entity("@channel")
    assert calls["n"] == 3

    # Fourth call must be refused locally: no further hammering.
    with pytest.raises(TelegramOperationSuspendedError):
        await session.get_entity("@channel")
    assert calls["n"] == 3, "suspended call still reached Telegram"


@pytest.mark.anyio
async def test_transport_session_success_clears_the_breaker():
    from unittest.mock import AsyncMock

    from telethon.errors import FloodWaitError
    from telethon_floodgate import HandledFloodWaitError

    from tests.helpers import FakeCliTelethonClient
    from tg_agent.telegram.backends import TelegramTransportSession

    err = FloodWaitError(request=None, capture=0)
    err.seconds = 23
    state = {"flood": True}

    def _resolver(_arg):
        return err if state["flood"] else object()

    pool = AsyncMock()
    pool._flood_breaker = FloodCircuitBreaker(threshold=3, cooldown_seconds=300.0)
    pool._rate_limit_gate = None

    session = TelegramTransportSession(
        FakeCliTelethonClient(entity_resolver=_resolver),
        phone="+7002",
        pool=pool,
    )

    for _ in range(2):
        with pytest.raises(HandledFloodWaitError):
            await session.get_entity("@channel")

    state["flood"] = False
    await session.get_entity("@channel")  # success resets the counter

    state["flood"] = True
    for _ in range(2):
        with pytest.raises(HandledFloodWaitError):
            await session.get_entity("@channel")
    # Counter was reset, so two more floods stay below the threshold of three.
    pool._flood_breaker.check("telegram_resolve_entity", "+7002")


@pytest.mark.anyio
async def test_stream_messages_iterator_exposes_callable_client():
    """#1806: floodgate.wrap_messages_iterator refuses iterators whose ``client``
    is not callable — Telethon iterators carry it, but the transport-session
    proxy stripped it, so every tg_messenger history read through the gateway
    died with 'TypeError: message iterator must expose a callable client'
    before reaching Telegram."""
    from unittest.mock import AsyncMock

    from telethon.tl.types import PeerChannel

    from tests.helpers import FakeCliTelethonClient
    from tg_agent.telegram.backends import TelegramTransportSession

    class _StubIter:
        def __init__(self, client):
            self.client = lambda: client

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        def aclose(self):
            return None

    pool = AsyncMock()
    pool._flood_breaker = FloodCircuitBreaker(threshold=3, cooldown_seconds=300.0)
    pool._rate_limit_gate = None

    stub = FakeCliTelethonClient()
    stub.iter_messages_factory = lambda *a, **k: _StubIter(stub)

    session = TelegramTransportSession(stub, phone=PHONE, pool=pool)
    iterator = session.iter_messages(PeerChannel(1))
    client = getattr(iterator, "client", None)
    assert callable(client), (
        "stream_messages proxy must expose a callable client — floodgate's "
        "wrap_messages_iterator refuses iterators without one (#1806)"
    )
    assert client() is session.raw_client
