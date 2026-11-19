"""A project's link to the shared gateway: one relay connection (#1868).

The same connection carries the project's Telegram requests (a ``ClientPool``
built with ``relay=client.connection``) and its control operations below.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from tg_agent.telegram.relay import RelayConnection, RelayEndpoint, RelayRemoteError, probe_endpoint


class GatewayRequestError(RuntimeError):
    """A refused gateway operation; ``status`` keeps the HTTP-style code."""

    def __init__(self, status: int | None, message: str):
        super().__init__(message)
        self.status = status


def _project_token(config) -> str:
    return json.loads(Path(config.gateway["token_file"]).read_text(encoding="utf-8"))["token"]


def _endpoint(record: dict, token: str) -> RelayEndpoint:
    return RelayEndpoint(port=int(record["port"]), token=token, pid=int(record["pid"]),
                         protocol=int(record["protocol"]))


class TelegramGatewayClient:
    def __init__(self, connection: RelayConnection):
        self.connection = connection

    @classmethod
    async def inspect_config(cls, config) -> dict:
        """Read project readiness without starting or replacing a gateway."""
        from tg_agent.gateway.runtime import endpoint

        try:
            record = await asyncio.to_thread(endpoint, Path(config.gateway["root"]))
            if record is None:
                return {"available": False, "accounts": []}
            token = await asyncio.to_thread(_project_token, config)
            client = cls(await RelayConnection.open(_endpoint(record, token)))
            try:
                return await client.status()
            finally:
                await client.close()
        except (OSError, ValueError, RuntimeError, KeyError) as exc:
            return {"available": False, "accounts": [], "reason": type(exc).__name__}

    @classmethod
    async def from_config(cls, config) -> TelegramGatewayClient:
        """Connect (starting the gateway when it is down); reconnects after a gateway restart."""
        from tg_agent.gateway.runtime import ensure_gateway

        root = Path(config.gateway["root"])
        token = await asyncio.to_thread(_project_token, config)

        async def fresh_endpoint() -> RelayEndpoint:
            return _endpoint(await asyncio.to_thread(ensure_gateway, root), token)

        connection = await RelayConnection.open(await fresh_endpoint(), reconnect=fresh_endpoint)
        return cls(connection)

    async def close(self) -> None:
        await self.connection.close()

    async def call(self, op: str, **fields):
        try:
            return await self.connection.call(op, **fields)
        except RelayRemoteError as exc:
            raise GatewayRequestError(exc.status, str(exc)) from None

    async def status(self) -> dict:
        return await self.call("status")

    async def accounts(self) -> list[dict]:
        return await self.call("accounts")

    async def update_account(self, account_id: str, *, active=None, primary=False) -> None:
        await self.call("account.update", account_id=account_id, body={"active": active, "primary": primary})

    async def clear_flood(self, account_id: str) -> list[str]:
        return await self.call("account.flood_clear", account_id=account_id)

    async def detach(self, account_id: str) -> None:
        await self.call("account.detach", account_id=account_id)

    async def export_session(self, account_id: str) -> str:
        return await self.call("account.export_session", account_id=account_id)

    async def auth(self, action: str, payload: dict) -> dict:
        allowed = {"phone", "code", "phone_code_hash", "password_2fa", "qr_id", "session_string"}
        body = {"action": action, **{key: value for key, value in payload.items() if key in allowed}}
        try:
            return await self.call("auth", body=body)
        except GatewayRequestError as exc:
            if exc.status == 409 and str(exc) == "TwoFactorRequiredError":
                from tg_shared.auth import TwoFactorRequiredError

                raise TwoFactorRequiredError() from None
            raise

    async def events(self, after: int = 0) -> list[dict]:
        return await self.call("events", after=after)

    async def event_floor(self) -> int:
        return await self.call("events.recovery")

    async def acknowledge(self, event_id: int) -> None:
        await self.call("events.ack", event_id=event_id)

    async def subscribe_events(self, on_events) -> None:
        """``on_events()`` runs whenever the gateway journals new DM events."""
        self.connection.on_push(lambda message: on_events() if message.get("push") == "events" else None)
        await self.connection.subscribe("events.subscribe")

    async def reply_owner(self, account_id: str, dialog_id: int, *, release: bool = False) -> bool:
        return await self.call("reply_owner", body={"account_id": account_id, "dialog_id": dialog_id,
                                                    "release": release})

    async def can_auto_reply(self, account_id: str, dialog_id: int) -> bool:
        return await self.call("reply_owner.check", account_id=account_id, dialog_id=dialog_id)

    async def export_bots(self) -> list[dict]:
        return await self.call("bots.export")


def probe(record: dict, token: str) -> dict | None:
    """Blocking readiness probe for synchronous callers."""
    return probe_endpoint(_endpoint(record, token))
