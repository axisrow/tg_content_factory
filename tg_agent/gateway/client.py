"""Internal client. Callers persist request IDs before executing writes."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID

import httpx

from tg_agent.gateway.protocol import Operation


class TelegramGatewayClient:
    @classmethod
    async def inspect_config(cls, config):
        """Read project readiness without starting or replacing a daemon."""
        from tg_agent.gateway.runtime import endpoint

        try:
            record = await asyncio.to_thread(endpoint, Path(config.gateway["root"]))
            if record is None:
                return {"http_available": False, "dispatcher_running": False, "accounts": []}
            raw = await asyncio.to_thread(Path(config.gateway["token_file"]).read_text, encoding="utf-8")
            client = cls(f"http://127.0.0.1:{record['port']}", json.loads(raw)["token"])
            try:
                return await client.status()
            finally:
                await client.close()
        except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
            return {"http_available": False, "dispatcher_running": False, "reason": type(exc).__name__}

    @classmethod
    async def from_config(cls, config):
        from tg_agent.gateway.runtime import ensure_gateway

        root = Path(config.gateway["root"])
        url = await asyncio.to_thread(ensure_gateway, root)
        raw = await asyncio.to_thread(Path(config.gateway["token_file"]).read_text, encoding="utf-8")
        return cls(url, json.loads(raw)["token"])

    def __init__(self, url: str, token: str, *, transport=None):
        self.http = httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {token}"},
                                     transport=transport, timeout=30, trust_env=False)

    async def close(self) -> None:
        await self.http.aclose()

    async def status(self) -> dict:
        response = await self.http.get("/v1/status")
        response.raise_for_status()
        result = response.json()
        if result["protocol"] != 1 or result.get("capabilities_version") != 2:
            raise RuntimeError("Incompatible Telegram gateway protocol")
        return result

    async def submit(self, request_id: UUID, operation: Operation) -> dict:
        response = await self.http.post("/v1/operations", json={"request_id": str(request_id),
                                                              "operation": operation.model_dump(mode="json")})
        response.raise_for_status()
        return response.json()

    async def receipt(self, request_id: UUID) -> dict:
        response = await self.http.get(f"/v1/operations/{request_id}")
        response.raise_for_status()
        receipt = response.json()
        for field, total in receipt.pop("result_pages", {}).items():
            values = receipt["result"][field]
            while len(values) < total:
                response = await self.http.get(f"/v1/operations/{request_id}/results/{field}",
                                                params={"offset": len(values), "limit": 100})
                response.raise_for_status()
                page = response.json()
                if not page["items"] or page["total"] != total:
                    raise RuntimeError("Gateway result pagination did not advance consistently")
                values.extend(page["items"])
        return receipt

    async def wait(self, request_id: UUID, *, timeout: float = 120) -> dict:
        async with asyncio.timeout(timeout):
            while True:
                receipt = await self.receipt(request_id)
                if receipt["state"] not in {"pending", "running"}:
                    return receipt
                await asyncio.sleep(0.05)

    async def cancel(self, request_id: UUID) -> dict:
        response = await self.http.post(f"/v1/operations/{request_id}/cancel")
        response.raise_for_status()
        return response.json()

    async def events(self, after: int = 0) -> list[dict]:
        response = await self.http.get("/v1/events", params={"after": after})
        response.raise_for_status()
        return response.json()["events"]

    async def acknowledge(self, event_id: int) -> None:
        response = await self.http.post("/v1/events/ack", json={"event_id": event_id})
        response.raise_for_status()

    async def accounts(self) -> list[dict]:
        response = await self.http.get("/v1/accounts")
        response.raise_for_status()
        return response.json()["accounts"]

    async def update_account(self, account_id: str, *, active=None, primary=False) -> None:
        response = await self.http.patch(f"/v1/accounts/{account_id}", json={"active": active, "primary": primary})
        response.raise_for_status()

    async def detach(self, account_id: str) -> None:
        response = await self.http.delete(f"/v1/accounts/{account_id}")
        response.raise_for_status()
