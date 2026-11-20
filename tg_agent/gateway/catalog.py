"""Safe project account projections and explicit machine-owner enrollment."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from tg_agent.gateway.client import GatewayRequestError, TelegramGatewayClient
from tg_agent.telegram.relay import RelayConnection, RelayEndpoint, RelayRemoteError

logger = logging.getLogger(__name__)


class GatewayAdministrator:
    """The machine owner's relay link: enrollment and shared-account administration."""

    def __init__(self, connection: RelayConnection):
        self.connection = connection

    @classmethod
    async def connect(cls, root: Path) -> GatewayAdministrator:
        from tg_agent.gateway.runtime import ensure_gateway

        record = await asyncio.to_thread(ensure_gateway, root)
        token = await asyncio.to_thread((root / "gateway-admin.token").read_text, encoding="utf-8")
        connection = await RelayConnection.open(
            RelayEndpoint(port=int(record["port"]), token=token.strip(), pid=int(record["pid"]),
                          protocol=int(record["protocol"]))
        )
        return cls(connection)

    async def call(self, op: str, payload: dict | None = None, **fields):
        try:
            if payload is not None:
                fields["body"] = payload
            return await self.connection.call("admin." + op, **fields)
        except RelayRemoteError as exc:
            raise GatewayRequestError(exc.status, str(exc)) from None

    async def close(self) -> None:
        await self.connection.close()


class ProjectAccountCatalog:
    def __init__(self, db, client: TelegramGatewayClient):
        self.db = db
        self.client = client

    async def refresh(self) -> None:
        rows = await self.client.accounts()
        await self.db.repos.accounts.sync_gateway_catalog(rows)

    async def reference(self, local_id: int) -> str:
        cursor = await self.db.db.execute("SELECT account_id FROM gateway_account_refs WHERE local_id=?",
                                          (local_id,))
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
        await self.client.auth("import", {"phone": phone, "session_string": session})
        await self.refresh()

    async def export_session(self, local_id):
        return await self.client.export_session(await self.reference(local_id))


@asynccontextmanager
async def project_catalog(config, db):
    """An authenticated, short-lived account catalog for the CLI and tools."""
    client = await TelegramGatewayClient.from_config(config)
    try:
        yield ProjectAccountCatalog(db, client)
    finally:
        await client.close()


async def gateway_account_id(db, phone: str) -> str | None:
    """The shared gateway's account id for a project phone, from the mirrored catalog."""
    cursor = await db.db.execute(
        "SELECT r.account_id FROM gateway_account_refs r JOIN accounts a ON a.id = r.local_id WHERE a.phone = ?",
        (phone,),
    )
    row = await cursor.fetchone()
    return row[0] if row else None


async def restore_gateway_bots(db, client: TelegramGatewayClient, project_id: str) -> int:
    """Take back notification bots a pre-#1868 enrollment moved into the gateway.

    Projects send notifications natively again; a bot whose token was blanked
    locally at enrollment gets it back from the gateway. Returns the count.
    """
    from tg_agent.models import NotificationBot

    marker = f"gateway_bots_restored:{project_id}"
    if await db.get_setting(marker) is not None:
        return 0
    bots = [NotificationBot.model_validate(payload) for payload in await client.export_bots()]
    return await db.repos.notification_bots.restore_legacy_bots(marker, bots)


async def connect_project(config, db) -> TelegramGatewayClient:
    """Connect a gateway project: mirror its catalog and take back its notification bots.

    The worker and every CLI process start here, so a project that only ever
    runs the CLI also gets the bot tokens an older enrollment moved away.
    """
    client = await TelegramGatewayClient.from_config(config)
    try:
        await ProjectAccountCatalog(db, client).refresh()
        if await restore_gateway_bots(db, client, config.gateway["project_id"]):
            logger.info("Notification bots restored from the shared gateway")
    except BaseException:
        await client.close()
        raise
    return client


def shared_project(config):
    return isinstance(getattr(config, "gateway", None), dict) and bool(config.gateway)
