"""Safe project account projections and explicit machine-owner enrollment."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.runtime import ensure_gateway


class GatewayAdministrator:
    def __init__(self, client: httpx.AsyncClient):
        self.http = client

    @classmethod
    async def connect(cls, root: Path):
        url = await asyncio.to_thread(ensure_gateway, root)
        token = await asyncio.to_thread((root / "gateway-admin.token").read_text, encoding="utf-8")
        return cls(httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {token.strip()}"},
                                    trust_env=False, timeout=30))

    async def call(self, path: str, payload: dict) -> dict:
        response = await self.http.post("/v1/admin/" + path, json=payload)
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.http.aclose()


class ProjectAccountCatalog:
    def __init__(self, db, client: TelegramGatewayClient):
        self.db = db
        self.client = client

    async def refresh(self) -> None:
        rows = await self.client.accounts()
        await self.db.repos.accounts.sync_gateway_catalog(rows)

    async def reference(self, local_id: int) -> str:
        async with self.db.db.execute("SELECT account_id FROM gateway_account_refs WHERE local_id=?",
                                      (local_id,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise ValueError("Project account not found")
        return row[0]

    async def update(self, local_id: int, *, active=None, primary=False):
        await self.client.update_account(await self.reference(local_id), active=active, primary=primary)
        await self.refresh()

    async def detach(self, local_id: int):
        await self.client.detach(await self.reference(local_id))
        await self.refresh()

    async def import_session(self, phone, session):
        response = await self.client.http.post("/v1/auth", json={"action": "import", "phone": phone,
                                                               "session_string": session})
        response.raise_for_status()
        await self.refresh()

    async def export_session(self, local_id):
        account = await self.reference(local_id)
        response = await self.client.http.post(f"/v1/accounts/{account}/export-session")
        response.raise_for_status()
        return response.json()["session_string"]


@asynccontextmanager
async def project_catalog(config, db):
    """An authenticated, short-lived account catalog for CLI, Web, and tools."""
    client = await TelegramGatewayClient.from_config(config)
    try:
        yield ProjectAccountCatalog(db, client)
    finally:
        await client.close()


def shared_project(config):
    return isinstance(getattr(config, "gateway", None), dict) and bool(config.gateway)
