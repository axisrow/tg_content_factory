from __future__ import annotations

import logging
from datetime import datetime

import aiosqlite

from src.models import Account
from src.security import SessionCipher

logger = logging.getLogger(__name__)


class AccountsRepository:
    def __init__(self, db: aiosqlite.Connection, session_cipher: SessionCipher | None = None):
        self._db = db
        self._session_cipher = session_cipher

    async def add_account(self, account: Account) -> int:
        session_string = account.session_string
        if self._session_cipher:
            session_string = self._session_cipher.encrypt(session_string)

        cur = await self._db.execute(
            """INSERT INTO accounts (phone, session_string, is_primary, is_active, is_premium)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   session_string=excluded.session_string,
                   is_active=excluded.is_active,
                   is_premium=excluded.is_premium""",
            (
                account.phone,
                session_string,
                int(account.is_primary),
                int(account.is_active),
                int(account.is_premium),
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def migrate_sessions(self) -> int:
        """Migrate plaintext and legacy encrypted sessions to the current format."""
        if not self._session_cipher:
            return 0

        cur = await self._db.execute("SELECT id, phone, session_string FROM accounts")
        rows = await cur.fetchall()
        if not rows:
            return 0

        migrated = 0

        try:
            await self._db.execute("BEGIN")
            for row in rows:
                raw_session = row["session_string"]
                try:
                    migrated_value = self._session_cipher.encrypt(raw_session)
                except ValueError as exc:
                    raise RuntimeError(
                        f"Failed to migrate session for phone={row['phone']}"
                    ) from exc

                if migrated_value != raw_session:
                    await self._db.execute(
                        "UPDATE accounts SET session_string = ? WHERE id = ?",
                        (migrated_value, row["id"]),
                    )
                    migrated += 1
                    logger.info("Migrated session format for phone=%s", row["phone"])
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise

        return migrated

    async def get_accounts(self, active_only: bool = False) -> list[Account]:
        sql = "SELECT * FROM accounts"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        accounts: list[Account] = []

        for row in rows:
            raw_session = row["session_string"]
            session_string = raw_session
            if self._session_cipher:
                if self._session_cipher.is_encrypted(raw_session):
                    try:
                        session_string = self._session_cipher.decrypt(raw_session)
                    except ValueError as exc:
                        raise RuntimeError(
                            f"Failed to decrypt session for phone={row['phone']}"
                        ) from exc
                # If plaintext is still in DB, use it as-is (migrate_sessions
                # should have been called during initialize).

            accounts.append(
                Account(
                    id=row["id"],
                    phone=row["phone"],
                    session_string=session_string,
                    is_primary=bool(row["is_primary"]),
                    is_active=bool(row["is_active"]),
                    is_premium=bool(row["is_premium"]) if row["is_premium"] is not None else False,
                    flood_wait_until=(
                        datetime.fromisoformat(row["flood_wait_until"])
                        if row["flood_wait_until"]
                        else None
                    ),
                    created_at=(
                        datetime.fromisoformat(row["created_at"]) if row["created_at"] else None
                    ),
                )
            )

        return accounts

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
