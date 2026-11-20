"""Notification bots that pre-#1868 enrollments moved into the shared gateway.

Projects send notifications natively again (Bot API with their own token; the
BotFather conversation runs on the gateway's client through the relay's
``botfather.*`` operations, since only the owner receives the account's
updates). The gateway only keeps the bots it was handed, so a project can take
them back (``bots.export``).
"""

from tg_agent.models import NotificationBot


async def list_legacy_bots(store, project: str) -> list[NotificationBot]:
    async with store.transaction() as conn:
        async with conn.execute("SELECT payload FROM gateway_project_bots WHERE project_id=?", (project,)) as cursor:
            rows = await cursor.fetchall()
    return [NotificationBot.model_validate_json(store.cipher.decrypt(row[0])) for row in rows]
