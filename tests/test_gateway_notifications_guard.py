"""#1825: gateway notification fallback and BotFather flows ride the shared guard.

``ConnectedTarget.use_client`` must yield the guarded transport session — the
same gate, process ceiling, flood breaker and family-scoped flood reporting the
adapter-bound callers get — instead of unwrapping ``raw_client`` and bypassing
all of it. No notification-only limiter is added (per the issue's non-goal):
the guard is the pool's own upstream machinery.

Offline only: real Telethon error types and the real gate/breaker objects over
deterministic fakes; no Telegram is contacted and nothing is really sent.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError
from telethon_floodgate import FloodCircuitBreaker, TelegramRateLimitGate

from tests.helpers import FakeCliTelethonClient

ACC = "11111111-1111-1111-1111-111111111111"


class _CountingGate(TelegramRateLimitGate):
    """Real gate that records admissions — the shared-guard witness."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.admissions: list[tuple[str, str]] = []

    def try_acquire(self, phone, category, slots=1, peer=None):  # noqa: D102
        self.admissions.append((phone, category))
        return super().try_acquire(phone, category, slots=slots, peer=peer)


class _FakeCursor:
    """aiosqlite Cursor contract: sync-returning, awaitable and an async CM."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    async def fetchone(self):
        return None

    def __await__(self):
        async def _self():
            return self

        return _self().__await__()


class _FakeStore:
    """Just enough store for BotStorage.get_bot → None (no configured bot)."""

    class cipher:  # noqa: N801
        @staticmethod
        def decrypt(value):
            return value

    @asynccontextmanager
    async def transaction(self):
        yield self

    def execute(self, *_args, **_kwargs):
        return _FakeCursor()


def _guarded_pool() -> SimpleNamespace:
    """Pool double carrying the REAL gate and breaker (the shared guard)."""
    return SimpleNamespace(
        _rate_limit_gate=_CountingGate(),
        _outgoing_ceiling=None,
        _flood_breaker=FloodCircuitBreaker(),
        report_flood=AsyncMock(),
        report_family_flood=AsyncMock(),
        family_flood_deadline=AsyncMock(return_value=None),
    )


def _connected_target(pool, *, send=None, me=None) -> SimpleNamespace:
    """ConnectedTarget-alike built exactly like execute_notification builds it."""
    from tg_agent.gateway.notifications import ConnectedTarget
    from tg_agent.telegram.backends import TelegramTransportSession

    me = me or SimpleNamespace(id=999, username="gateway", premium=False)
    client = FakeCliTelethonClient(send_message_side_effect=send, me=me)
    session = TelegramTransportSession(client, phone=ACC, pool=pool)
    return SimpleNamespace(session=session, client=client, target=ConnectedTarget(session, ACC))


def _operation(action: str = "send", text: str = "hello") -> SimpleNamespace:
    return SimpleNamespace(
        account_id=ACC,
        action=action,
        text=text,
        chat_id=222,
        bot_name_prefix="LeadHunter",
        bot_username_prefix="leadhunter_",
    )


def _flood(seconds: int) -> FloodWaitError:
    err = FloodWaitError(request=None, capture=0)
    err.seconds = seconds
    return err


@pytest.mark.anyio
async def test_use_client_yields_guarded_session():
    """The native client is never handed out raw; the adapter is the contract."""
    from tg_agent.gateway.notifications import ConnectedTarget
    from tg_agent.telegram.backends import TelegramTransportSession

    session = TelegramTransportSession(FakeCliTelethonClient(), phone=ACC)
    async with ConnectedTarget(session, ACC).use_client() as (client, account):
        assert client is session, "raw_client leaked past the shared guard"
        assert account == ACC


@pytest.mark.anyio
async def test_direct_fallback_send_dispatches_through_gate():
    """No configured bot → direct send still reserves a real gate slot (#1825)."""
    from tg_agent.gateway.notifications import execute_notification

    pool = _guarded_pool()
    wired = _connected_target(pool)
    send_calls: list[tuple] = []

    async def _record_send(target, text, *_a, **_k):
        send_calls.append((target, text))
        return SimpleNamespace(id=1)

    wired.client._send_message_side_effect = _record_send

    result = await execute_notification(
        SimpleNamespace(store=_FakeStore(), connection=AsyncMock(return_value=wired.session)),
        "proj",
        _operation(),
    )

    assert result == {"delivered": True}
    assert len(send_calls) == 1, "send never dispatched"
    assert pool._rate_limit_gate.admissions, "send bypassed the shared gate"


@pytest.mark.anyio
async def test_flood_keeps_structured_cause_and_deadline(caplog):
    """A refused send reports account-wide with the retry deadline intact (#1825 AC3)."""
    from tg_agent.gateway.notifications import execute_notification

    pool = _guarded_pool()
    wired = _connected_target(pool, send=lambda *_a, **_k: _flood(120))

    with pytest.raises(TimeoutError):
        await execute_notification(
            SimpleNamespace(store=_FakeStore(), connection=AsyncMock(return_value=wired.session)),
            "proj",
            _operation(),
        )

    pool.report_flood.assert_awaited_once()
    phone, seconds = pool.report_flood.await_args.args
    assert phone == ACC and seconds == 120
    # Structured cause survived the notification handling (not a bare message).
    assert "Flood wait" in caplog.text and "120" in caplog.text


@pytest.mark.anyio
async def test_setup_bot_uses_guarded_session_for_conversation_flow():
    """BotFather conversation, /start and resolve all run on the guarded session."""
    from tg_agent.services.notification_service import NotificationService

    pool = _guarded_pool()
    me = SimpleNamespace(id=777, username="notifyowner", premium=False)
    wired = _connected_target(pool, me=me)

    fake_token = "1234567890:" + "A" * 40
    responses = [
        "Alright, what's the bot's name?",
        "Good. Now the username — it must end in `bot`.",
        f"{fake_token} done!",
    ]
    sent: list[object] = []

    async def _record(text):
        sent.append(text)

    conversation = MagicMock()
    conversation.__aenter__ = AsyncMock(return_value=conversation)
    conversation.__aexit__ = AsyncMock(return_value=None)
    conversation.send_message = AsyncMock(side_effect=_record)
    conversation.get_response = AsyncMock(side_effect=[SimpleNamespace(text=t) for t in responses])
    wired.client._conversation_factory = lambda *a, **_k: conversation

    service = NotificationService(
        SimpleNamespace(save_bot=AsyncMock(), get_bot=AsyncMock(return_value=None)),
        wired.target,
    )
    bot = await service.setup_bot()

    assert bot.bot_username == "leadhunter_notifyowner_bot"
    assert conversation.sent_messages, "BotFather conversation never ran"
    assert pool._rate_limit_gate.admissions, "guarded session never consulted the gate"
