"""Account connection lifecycle command handlers (#1047).

Domain: ``accounts.*`` — connect (materialize session into the pool), toggle
active, delete. ``accounts.connect`` is also chained from ``auth.verify_code``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.database.live_accounts import load_live_usable_accounts
from src.models import Account, AccountSummary

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object

logger = logging.getLogger(__name__)


class AccountsCommandsMixin(_Base):
    """``accounts.*`` command handlers."""

    async def _handle_accounts_connect(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        accounts = await load_live_usable_accounts(self._db, active_only=False)
        account = next((a for a in accounts if a.phone == phone), None)
        if account is None:
            summaries = await self._db.get_account_summaries(active_only=False)
            summary = next((a for a in summaries if a.phone == phone), None)
            if summary is not None:
                status = getattr(summary, "session_status", "unavailable")
                raise RuntimeError(f"account_session_unavailable:{phone}:{status}")
            raise RuntimeError(f"account_not_found:{phone}")
        await self._pool.add_client(phone, account.session_string)
        result = await self._pool.get_client_by_phone(phone)
        is_premium = False
        if result is not None:
            session, acquired_phone = result
            try:
                me = await session.fetch_me()
                is_premium = bool(getattr(me, "premium", False))
            finally:
                await self._pool.release_client(acquired_phone)
        await self._db.update_account_premium(phone, is_premium)
        return {"phone": phone, "is_premium": is_premium}

    async def _handle_accounts_toggle(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = int(payload["account_id"])
        summaries = await self._db.get_account_summaries(active_only=False)
        account_summary: AccountSummary | Account | None = next((a for a in summaries if a.id == account_id), None)
        live_account: Account | None = None
        if account_summary is None:
            accounts = await load_live_usable_accounts(self._db, active_only=False)
            live_account = next((a for a in accounts if a.id == account_id), None)
            account_summary = live_account
        if account_summary is None:
            raise RuntimeError(f"account_not_found:{account_id}")
        new_active = not account_summary.is_active
        await self._db.set_account_active(account_id, new_active)
        if new_active:
            try:
                if live_account is None:
                    accounts = await load_live_usable_accounts(self._db, active_only=False)
                    live_account = next((a for a in accounts if a.id == account_id), None)
                if live_account is None:
                    logger.warning(
                        "accounts.toggle: account session unavailable for %s",
                        account_summary.phone,
                    )
                else:
                    await self._pool.add_client(live_account.phone, live_account.session_string)
            except Exception as exc:
                logger.warning("accounts.toggle: failed to add client %s: %s", account_summary.phone, exc)
        else:
            try:
                await self._pool.remove_client(account_summary.phone)
            except Exception as exc:
                logger.warning("accounts.toggle: failed to remove client %s: %s", account_summary.phone, exc)
        return {"account_id": account_id, "is_active": new_active}

    async def _handle_accounts_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = int(payload["account_id"])
        phone = str(payload.get("phone") or "").strip()
        delete_from_db = not phone
        if not phone:
            accounts = await self._db.get_account_summaries(active_only=False)
            account = next((a for a in accounts if a.id == account_id), None)
            phone = account.phone if account is not None else ""
        if phone:
            try:
                await self._pool.remove_client(phone)
            except Exception as exc:
                logger.warning("accounts.delete: failed to remove client %s: %s", phone, exc)
        if delete_from_db:
            await self._db.delete_account(account_id)
        return {
            "account_id": account_id,
            "deleted": delete_from_db,
            "client_removed": bool(phone),
        }
