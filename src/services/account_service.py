from __future__ import annotations

import logging
from collections.abc import Sequence

from src.database import Database
from src.database.bundles import AccountBundle
from src.models import Account, AccountSummary
from src.telegram.client_pool import ClientPool

logger = logging.getLogger(__name__)


def _account_session_ok(account: Account | AccountSummary) -> bool:
    # `Account` has no session_status (only AccountSummary does) — absent means ok.
    status = getattr(account, "session_status", "ok")
    return status == "ok" if isinstance(status, str) else True


def pick_default_account_phone(accounts: Sequence[Account | AccountSummary]) -> str | None:
    """Pick the deterministic default account phone: primary first (#1480).

    Usable means ``is_active`` and ``session_status == "ok"`` (same rule the
    agent-tool ``resolve_phone`` has always applied). The primary usable account
    wins; otherwise the first usable one. ``None`` when nothing is usable.
    """
    usable = [
        account
        for account in accounts
        if getattr(account, "is_active", True) and _account_session_ok(account)
    ]
    if not usable:
        return None
    primary = next((a for a in usable if getattr(a, "is_primary", False)), usable[0])
    return primary.phone


async def resolve_default_phone(
    db: Database, *, connected: set[str] | None = None
) -> str | None:
    """Default account for commands run without ``--phone`` (#1480).

    DB-driven pick via :func:`pick_default_account_phone`; when ``connected``
    is given (caller holds a live pool), a picked phone that is not connected
    falls back to the first connected phone, so read commands still work when
    the primary account is down.
    """
    phone = pick_default_account_phone(await db.get_account_summaries())
    if connected is not None and (phone is None or phone not in connected):
        phone = sorted(connected)[0] if connected else None
    return phone


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
