"""Notification bots that pre-#1868 enrollments moved into the shared gateway.

Projects send notifications natively again (Bot API with their own token; the
BotFather conversation runs on the gateway's client through the relay's
``botfather.*`` operations, since only the owner receives the account's
updates). The gateway only keeps the bots it was handed, so a project can take
them back (``bots.export``).
"""

from tg_agent.models import NotificationBot


class BotStorage:
    def __init__(self, store, project):
        self.store = store
        self.project = project

    async def get_bot(self, user):
        async with self.store.transaction() as conn:
            async with conn.execute("SELECT payload FROM gateway_project_bots WHERE project_id=? AND user_id=?",
                                    (self.project, user)) as cursor:
                row = await cursor.fetchone()
        return NotificationBot.model_validate_json(self.store.cipher.decrypt(row[0])) if row else None

    async def list_bots(self) -> list[NotificationBot]:
        async with self.store.transaction() as conn:
            async with conn.execute("SELECT payload FROM gateway_project_bots WHERE project_id=?",
                                    (self.project,)) as cursor:
                rows = await cursor.fetchall()
        return [NotificationBot.model_validate_json(self.store.cipher.decrypt(row[0])) for row in rows]

    async def save_bot(self, bot):
        async with self.store.transaction() as conn:
            await conn.execute("INSERT INTO gateway_project_bots VALUES (?,?,?) ON CONFLICT(project_id,user_id) "
                               "DO UPDATE SET payload=excluded.payload",
                               (self.project, bot.tg_user_id, self.store.cipher.encrypt(bot.model_dump_json())))
        return bot.id

    async def delete_bot(self, user):
        async with self.store.transaction() as conn:
            await conn.execute("DELETE FROM gateway_project_bots WHERE project_id=? AND user_id=?",
                               (self.project, user))
