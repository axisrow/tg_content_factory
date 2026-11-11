"""RemoteActionService result coercion (#1806 follow-up).

invoke() must return the declared TelegramActionService return type, not a raw
dict: dialogs read does ``result.messages`` on whatever invoke hands back, and
MessagesResult is a pydantic model — the old dataclass-only branch silently
returned the raw receipt dict instead.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

pytestmark = pytest.mark.telegram_unit


class _FakePool:
    """Just enough of the gateway pool for RemoteActionService.invoke."""

    def __init__(self) -> None:
        self.account_id = uuid4()

    def select(self, preferred_phone=None, *, exclude=()):
        return {"id": str(self.account_id), "phone": "+1234567890", "active": True,
                "flood_until": 0.0, "is_primary": True}

    async def request(self, operation, intent_key):
        assert "read_history" in str(operation.kind)
        return {
            "phone": "+1234567890",
            "dialog_id": 1997034059,
            "messages": [
                {
                    "id": 24276,
                    "dialog_id": 1997034059,
                    "sender_id": 1,
                    "sender_name": "Michael",
                    "out": False,
                    "date": "2026-11-10T06:07:18+00:00",
                    "text": "самое смешное что на макс эфорте он жрет в 4 раза больше",
                },
                {
                    "id": 24313,
                    "dialog_id": 1997034059,
                    "sender_id": 2,
                    "sender_name": None,
                    "out": True,
                    "date": "2026-11-11T10:30:00+00:00",
                    "text": "хайку за такой ценник надо брать и пробывать",
                },
            ],
        }


@pytest.mark.anyio
async def test_read_history_invoke_returns_messages_result():
    from tg_agent.gateway.remote_actions import RemoteActionService
    from tg_agent.models import MessagesResult

    service = RemoteActionService(_FakePool())
    result = await service.read_history(phone="+1234567890", chat_id=1997034059, limit=5)
    assert isinstance(result, MessagesResult), (
        f"invoke must return the declared MessagesResult, got {type(result).__name__} "
        "— dialogs read does result.messages on it (#1806 follow-up)"
    )
    assert result.dialog_id == 1997034059
    assert [m.id for m in result.messages] == [24276, 24313]
    assert result.messages[1].out is True
