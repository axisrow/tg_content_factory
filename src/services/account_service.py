from __future__ import annotations

import logging

from src.database import Database
from src.database.bundles import AccountBundle
from src.telegram.client_pool import ClientPool

logger = logging.getLogger(__name__)


class AccountService:
    def __init__(self, accounts: AccountBundle | Database, pool: ClientPool | None = None):
        if isinstance(accounts, Database):
            accounts = AccountBundle.from_database(accounts)
        self._accounts = accounts
        self._pool = pool

    async def list(self):
        return await self._accounts.list_accounts()

    async def toggle(self, account_id: int) -> None:
        accounts = await self._accounts.list_accounts()
        for acc in accounts:
            if acc.id == account_id:
                await self._accounts.set_active(account_id, not acc.is_active)
                if self._pool:
                    if not acc.is_active:
                        try:
                            await self._pool.add_client(acc.phone, acc.session_string)
                        except Exception as e:
                            logger.warning("Failed to add client for %s: %s", acc.phone, e)
                    else:
                        # set_active already updated the DB (source of truth), so a
                        # pool.remove_client failure must not propagate (#1029) —
                        # symmetric with the add_client branch above and delete().
                        try:
                            await self._pool.remove_client(acc.phone)
                        except Exception as e:
                            logger.warning("Failed to remove client for %s: %s", acc.phone, e)
                return

    async def delete(self, account_id: int) -> None:
        if self._pool:
            accounts = await self._accounts.list_accounts()
            for acc in accounts:
                if acc.id == account_id:
                    # The DB is the source of truth: the pool is rebuilt from it on
                    # restart. A pool.remove_client failure must NOT abort the DB
                    # delete (#1029) — otherwise the account stays in the DB (a
                    # "ghost" the operator thinks is gone) while its client lingers.
                    # Mirror toggle()'s add_client handling: log, don't propagate.
                    try:
                        await self._pool.remove_client(acc.phone)
                    except Exception as e:
                        logger.warning("Failed to remove client for %s: %s", acc.phone, e)
                    break
        await self._accounts.delete_account(account_id)
