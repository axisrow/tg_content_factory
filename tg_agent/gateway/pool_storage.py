"""Storage ports for the existing ClientPool, backed only by gateway state."""

from datetime import datetime, timezone
from types import SimpleNamespace

from tg_agent.database.repositories.dialog_cache import DialogCacheRepository
from tg_agent.models import Account, AccountSummary

CACHE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS gateway_routing(channel_id INTEGER PRIMARY KEY,phone TEXT)",
    "CREATE TABLE IF NOT EXISTS dialog_cache(id INTEGER PRIMARY KEY,phone TEXT NOT NULL,dialog_id INTEGER NOT NULL,"
    "title TEXT,username TEXT,channel_type TEXT NOT NULL,deactivate INTEGER NOT NULL DEFAULT 0,"
    "is_own INTEGER NOT NULL DEFAULT 0,cached_at TEXT NOT NULL DEFAULT (datetime('now')),UNIQUE(phone,dialog_id))",
)


class GatewayPoolStorage:
    _gateway_owner = True
    def __init__(self, store):
        self.store = store
        self.db = store._connection
        self.repos = SimpleNamespace(accounts=self, channels=self,
                                     dialog_cache=DialogCacheRepository(self.db, database=self))

    async def initialize(self):
        async with self.transaction() as conn:
            for sql in CACHE_SCHEMA:
                await conn.execute(sql)

    def transaction(self):
        return self.store.transaction()

    async def execute_write(self, sql, params=()):
        async with self.transaction() as conn:
            return await conn.execute(sql, params)

    async def executemany_write(self, sql, rows):
        async with self.transaction() as conn:
            return await conn.executemany(sql, rows)

    async def get_accounts(self, active_only=False):
        async with self.transaction() as conn:
            async with conn.execute("SELECT a.*,s.value AS premium FROM accounts a LEFT JOIN settings s "
                                    "ON s.key='premium:'||a.id WHERE EXISTS(SELECT 1 FROM grants g "
                                    "WHERE g.account_id=a.id AND g.active=1) "
                                    + ("AND a.active=1" if active_only else "")) as cur:
                rows = await cur.fetchall()
        return [Account(phone=row["id"], session_string=self.store.cipher.decrypt(row["session"]),
                        is_premium=row["premium"] == "1",
                        is_active=bool(row["active"]), flood_wait_until=(
                            datetime.fromtimestamp(row["flood_until"], timezone.utc) if row["flood_until"] else None))
                for row in rows]

    async def get_live_usable_accounts(self, active_only=False):
        return await self.get_accounts(active_only)

    async def get_account_summaries(self, active_only=False):
        accounts = await self.get_accounts(active_only)
        return [AccountSummary(**row.model_dump(exclude={"session_string"})) for row in accounts]

    async def update_account_flood(self, phone, until):
        await self.execute_write("UPDATE accounts SET flood_until=? WHERE id=?",
                                 (until.timestamp() if until else 0, phone))

    async def update_account_premium(self, phone, value):
        await self.set_setting(f"premium:{phone}", "1" if value else "0")

    async def get_setting(self, key):
        async with self.transaction() as conn:
            async with conn.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
                row = await cur.fetchone()
        return row[0] if row else None

    async def set_setting(self, key, value):
        await self.execute_write("INSERT INTO settings VALUES (?,?) ON CONFLICT(key) "
                                 "DO UPDATE SET value=excluded.value", (key, value))

    async def get_preferred_phone(self, channel_id):
        async with self.transaction() as conn:
            async with conn.execute("SELECT phone FROM gateway_routing WHERE channel_id=?", (channel_id,)) as cur:
                row = await cur.fetchone()
        return row[0] if row else None

    async def update_channel_preferred_phone(self, channel_id, phone):
        await self.execute_write("INSERT INTO gateway_routing VALUES (?,?) ON CONFLICT(channel_id) "
                                 "DO UPDATE SET phone=excluded.phone", (channel_id, phone))

    async def clear_preferred_phone_if_matches(self, channel_id, phone):
        await self.execute_write("DELETE FROM gateway_routing WHERE channel_id=? AND phone=?", (channel_id, phone))

    async def get_channel_by_channel_id(self, channel_id):
        return None  # Project channel state is intentionally not available to the gateway.
