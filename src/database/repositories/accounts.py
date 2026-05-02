from __future__ import annotations

import logging
from datetime import datetime

import aiosqlite

from src.database.repositories._transactions import begin_immediate
from src.models import Account, AccountSessionStatus, AccountSummary
from src.security import SessionCipher, decrypt_failure_status, log_expected_decrypt_failure

logger = logging.getLogger(__name__)

_RESTORE_ACCOUNT_ACTION = "restore_key_or_relogin"


class AccountSessionDecryptError(RuntimeError):
    def __init__(self, *, phone: str, status: str):
        super().__init__(
            "Failed to decrypt Telegram account session "
            f"for phone={phone} status={status}. "
            "Restore the original SESSION_ENCRYPTION_KEY or re-login this account."
        )
        self.resource = "telegram_account"
        self.identifier = phone
        self.status = status
        self.action = _RESTORE_ACCOUNT_ACTION


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
                    status = decrypt_failure_status(exc)
                    log_expected_decrypt_failure(
                        logger,
                        resource="telegram_account",
                        identifier=str(row["phone"]),
                        status=status,
                        action=_RESTORE_ACCOUNT_ACTION,
                    )
                    continue

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

    def _parse_dt(self, raw: str | None) -> datetime | None:
        return datetime.fromisoformat(raw) if raw else None

    def _row_to_summary(
        self,
        row: aiosqlite.Row,
        *,
        session_status: AccountSessionStatus,
    ) -> AccountSummary:
        return AccountSummary(
            id=row["id"],
            phone=row["phone"],
            is_primary=bool(row["is_primary"]),
            is_active=bool(row["is_active"]),
            is_premium=bool(row["is_premium"]) if row["is_premium"] is not None else False,
            flood_wait_until=self._parse_dt(row["flood_wait_until"]),
            created_at=self._parse_dt(row["created_at"]),
            session_status=session_status,
        )

    def _classify_session_status(self, raw_session: str, phone: str) -> AccountSessionStatus:
        version = SessionCipher.encryption_version(raw_session)
        if version is None:
            if raw_session.startswith("enc:v"):
                log_expected_decrypt_failure(
                    logger,
                    resource="telegram_account",
                    identifier=phone,
                    status="unsupported_version",
                    action=_RESTORE_ACCOUNT_ACTION,
                    level=logging.DEBUG,
                )
                return AccountSessionStatus.UNSUPPORTED_VERSION
            if raw_session.startswith("enc:"):
                log_expected_decrypt_failure(
                    logger,
                    resource="telegram_account",
                    identifier=phone,
                    status="encrypted_unknown",
                    action=_RESTORE_ACCOUNT_ACTION,
                    level=logging.DEBUG,
                )
                return AccountSessionStatus.ENCRYPTED_UNKNOWN
            return AccountSessionStatus.OK

        if self._session_cipher is None:
            log_expected_decrypt_failure(
                logger,
                resource="telegram_account",
                identifier=phone,
                status="missing_key",
                action=_RESTORE_ACCOUNT_ACTION,
                level=logging.DEBUG,
            )
            return AccountSessionStatus.MISSING_KEY

        try:
            self._session_cipher.decrypt(raw_session)
        except ValueError as exc:
            status = decrypt_failure_status(exc)
            log_expected_decrypt_failure(
                logger,
                resource="telegram_account",
                identifier=phone,
                status=status,
                action=_RESTORE_ACCOUNT_ACTION,
                level=logging.DEBUG,
            )
            if status == "unsupported_version":
                return AccountSessionStatus.UNSUPPORTED_VERSION
            return AccountSessionStatus.DECRYPT_FAILED
        return AccountSessionStatus.OK

    def _decrypt_session_for_live_use(self, raw_session: str, phone: str) -> str:
        version = SessionCipher.encryption_version(raw_session)
        if version is None:
            if raw_session.startswith("enc:v"):
                status = "unsupported_version"
                log_expected_decrypt_failure(
                    logger,
                    resource="telegram_account",
                    identifier=phone,
                    status=status,
                    action=_RESTORE_ACCOUNT_ACTION,
                )
                raise AccountSessionDecryptError(phone=phone, status=status)
            if raw_session.startswith("enc:"):
                status = "encrypted_unknown"
                log_expected_decrypt_failure(
                    logger,
                    resource="telegram_account",
                    identifier=phone,
                    status=status,
                    action=_RESTORE_ACCOUNT_ACTION,
                )
                raise AccountSessionDecryptError(phone=phone, status=status)
            return raw_session

        if self._session_cipher is None:
            status = "missing_key"
            log_expected_decrypt_failure(
                logger,
                resource="telegram_account",
                identifier=phone,
                status=status,
                action=_RESTORE_ACCOUNT_ACTION,
            )
            raise AccountSessionDecryptError(phone=phone, status=status)

        try:
            return self._session_cipher.decrypt(raw_session)
        except ValueError as exc:
            status = decrypt_failure_status(exc)
            log_expected_decrypt_failure(
                logger,
                resource="telegram_account",
                identifier=phone,
                status=status,
                action=_RESTORE_ACCOUNT_ACTION,
            )
            raise AccountSessionDecryptError(phone=phone, status=status) from exc

    async def get_account_summaries(self, active_only: bool = False) -> list[AccountSummary]:
        sql = "SELECT * FROM accounts"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            self._row_to_summary(
                row,
                session_status=self._classify_session_status(
                    str(row["session_string"] or ""),
                    str(row["phone"]),
                ),
            )
            for row in rows
        ]

    async def get_accounts(self, active_only: bool = False) -> list[Account]:
        sql = "SELECT * FROM accounts"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        accounts: list[Account] = []

        for row in rows:
            raw_session = str(row["session_string"] or "")
            session_string = self._decrypt_session_for_live_use(raw_session, str(row["phone"]))

            accounts.append(
                Account(
                    id=row["id"],
                    phone=row["phone"],
                    session_string=session_string,
                    is_primary=bool(row["is_primary"]),
                    is_active=bool(row["is_active"]),
                    is_premium=bool(row["is_premium"]) if row["is_premium"] is not None else False,
                    flood_wait_until=self._parse_dt(row["flood_wait_until"]),
                    created_at=self._parse_dt(row["created_at"]),
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
        try:
            await begin_immediate(self._db)
            cur = await self._db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            if (cur.rowcount or 0) > 0:
                await self._db.execute(
                    """
                    UPDATE accounts
                    SET is_primary = 1
                    WHERE id = (
                        SELECT id
                        FROM accounts
                        ORDER BY id ASC
                        LIMIT 1
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM accounts
                        WHERE is_primary = 1
                    )
                    """
                )
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise
