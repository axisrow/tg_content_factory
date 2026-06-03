from __future__ import annotations

from dataclasses import dataclass

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text

pytestmark = pytest.mark.real_tg_manual


@dataclass
class _LiveSandboxPool:
    phone: str
    client: object

    @property
    def clients(self) -> dict[str, object]:
        return {self.phone: self.client}

    async def get_native_client_by_phone(self, phone: str, *, wait_for_flood: bool = False):
        if phone != self.phone:
            return None
        return self.client, self.phone

    async def get_client_by_phone(self, phone: str, *, wait_for_flood: bool = False):
        if phone != self.phone:
            return None
        return self.client, self.phone

    async def release_client(self, phone: str) -> None:
        return None


@pytest.mark.anyio
async def test_send_reaction_agent_tool_live_sandbox(db, real_telegram_sandbox):
    client = real_telegram_sandbox.client
    message = await client.send_message(real_telegram_sandbox.saved_messages_target, "agent send_reaction live smoke")
    try:
        handlers = _get_tool_handlers(
            db,
            client_pool=_LiveSandboxPool(real_telegram_sandbox.phone, client),
        )
        result = await handlers["send_reaction"](
            {
                "phone": real_telegram_sandbox.phone,
                "chat_id": real_telegram_sandbox.saved_messages_target,
                "message_id": message.id,
                "emoji": "👍",
                "confirm": True,
            }
        )

        assert "Реакция 👍 поставлена" in _text(result)
    finally:
        await client.delete_messages(real_telegram_sandbox.saved_messages_target, [message.id])
