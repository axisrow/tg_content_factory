"""Internal client. Callers persist request IDs before executing writes."""

from __future__ import annotations

import asyncio
from uuid import UUID

import httpx

from tg_agent.gateway.protocol import Operation


class TelegramGatewayClient:
    def __init__(self, url: str, token: str, *, transport=None):
        self.http = httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {token}"},
                                     transport=transport, timeout=30, trust_env=False)

    async def close(self) -> None:
        await self.http.aclose()

    async def status(self) -> dict:
        response = await self.http.get("/v1/status")
        response.raise_for_status()
        result = response.json()
        if result["protocol"] != 1:
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
        return response.json()

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
