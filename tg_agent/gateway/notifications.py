"""BotFather and notification operations owned by the shared gateway."""

from contextlib import asynccontextmanager
from uuid import UUID

from tg_agent.gateway.context import next_intent_key
from tg_agent.gateway.protocol import Notification
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


class ConnectedTarget:
    def __init__(self, session, account):
        self.session, self.account = session, account

    @asynccontextmanager
    async def use_client(self):
        yield self.session.raw_client, self.account


async def execute_notification(access, project, operation):
    from tg_agent.services.notification_service import NotificationService
    from tg_agent.telegram.notifier import Notifier

    account = str(operation.account_id)
    session = await access.connection(project, account)
    target = ConnectedTarget(session, account)
    storage = BotStorage(access.store, project)
    service = NotificationService(storage, target, operation.bot_name_prefix, operation.bot_username_prefix)
    if operation.action == "send":
        delivered = await Notifier(target, operation.chat_id, storage).notify(operation.text)
        if not delivered:
            raise TimeoutError("Notification delivery was not confirmed")
        return {"delivered": True}
    if operation.action == "delete":
        await service.teardown_bot()
        return {"deleted": True}
    bot = await (service.setup_bot() if operation.action == "setup" else service.get_status())
    return {"bot": bot.model_dump(exclude={"bot_token"}) if bot is not None else None}


class GatewayNotificationService:
    def __init__(self, notifications, target_service, bot_name_prefix="LeadHunter", bot_username_prefix="leadhunter_"):
        self.target = target_service
        self.pool = target_service._pool
        self.name_prefix, self.username_prefix = bot_name_prefix, bot_username_prefix

    async def execute(self, action, *, text="", chat_id=None):
        target = await self.target.describe_target()
        if target.state != "available":
            raise RuntimeError(target.message)
        account = self.pool.select(target.effective_phone)
        if account["phone"] != target.effective_phone:
            raise PermissionError("Notification account is not available to this project")
        return await self.pool.request(Notification(kind="notification", account_id=UUID(account["id"]),
            action=action, text=text, chat_id=chat_id, bot_name_prefix=self.name_prefix,
            bot_username_prefix=self.username_prefix), next_intent_key())

    async def setup_bot(self):
        result = await self.execute("setup")
        return NotificationBot(**result["bot"], bot_token="")

    async def get_status(self):
        result = await self.execute("status")
        return NotificationBot(**result["bot"], bot_token="") if result["bot"] is not None else None

    async def teardown_bot(self):
        await self.execute("delete")

    async def send_notification(self, message):
        return (await self.execute("send", text=message))["delivered"]
