from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import Account


class AccountsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add_account(self, account: Account) -> int:
        cur = await self._db.execute(
            """INSERT INTO accounts (phone, session_string, is_primary, is_active, is_premium)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   session_string=excluded.session_string,
                   is_premium=excluded.is_premium""",
            (
                account.phone,
                account.session_string,
                int(account.is_primary),
                int(account.is_active),
                int(account.is_premium),
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_accounts(self, active_only: bool = False) -> list[Account]:
        sql = "SELECT * FROM accounts"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            Account(
                id=r["id"],
                phone=r["phone"],
                session_string=r["session_string"],
                is_primary=bool(r["is_primary"]),
                is_active=bool(r["is_active"]),
                is_premium=bool(r["is_premium"]) if r["is_premium"] is not None else False,
                flood_wait_until=(
                    datetime.fromisoformat(r["flood_wait_until"]) if r["flood_wait_until"] else None
                ),
                created_at=datetime.fromisoformat(r["created_at"]) if r["created_at"] else None,
            )
            for r in rows
        ]

    async def update_account_flood(self, phone: str, until: datetime | None) -> None:
        await self._db.execute(
            "UPDATE accounts SET flood_wait_until = ? WHERE phone = ?",
            (until.isoformat() if until else None, phone),
        )
        await self._db.commit()

    async def update_account_premium(self, phone: str, is_premium: bool) -> None:
        await self._db.execute(
            "UPDATE accounts SET is_premium = ? WHERE phone = ?",
            (int(is_premium), phone),
        )
        await self._db.commit()

    async def set_account_active(self, account_id: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE accounts SET is_active = ? WHERE id = ?", (int(active), account_id)
        )
        await self._db.commit()

    async def delete_account(self, account_id: int) -> None:
        await self._db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        await self._db.commit()
