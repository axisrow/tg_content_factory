from __future__ import annotations

import logging

from src.database import Database
from src.telegram.client_pool import ClientPool

logger = logging.getLogger(__name__)


class AccountService:
    def __init__(self, db: Database, pool: ClientPool | None = None):
        self._db = db
        self._pool = pool

    async def list(self):
        return await self._db.get_accounts()

    async def toggle(self, account_id: int) -> None:
        accounts = await self._db.get_accounts()
        for acc in accounts:
            if acc.id == account_id:
                await self._db.set_account_active(account_id, not acc.is_active)
                if self._pool:
                    if not acc.is_active:
                        try:
                            await self._pool.add_client(acc.phone, acc.session_string)
                        except Exception as e:
                            logger.warning("Failed to add client for %s: %s", acc.phone, e)
                    else:
                        await self._pool.remove_client(acc.phone)
                return

    async def delete(self, account_id: int) -> None:
        if self._pool:
            accounts = await self._db.get_accounts()
            for acc in accounts:
                if acc.id == account_id:
                    await self._pool.remove_client(acc.phone)
                    break
        await self._db.delete_account(account_id)
