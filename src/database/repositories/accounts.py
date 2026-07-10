"""Репозиторий Telegram-аккаунтов пула (таблица ``accounts``).

Доступ через `db.repos.accounts`. Поверх обычного CRUD держит один инвариант и
один слой безопасности:

* «не более одного primary» (#733) — атомарно при вставке/активации/удалении,
  с partial-unique-индексом как жёстким бэкстопом. 0 primary допустимо: первая
  вставка с ``is_primary=False`` и деактивация/удаление последнего primary
  оставляют пул без primary-аккаунта;
* шифрование StringSession — `session_string` хранится как `enc:v2:*`, когда
  задан `SESSION_ENCRYPTION_KEY`; чтение разделено на «для UI» (статус сессии без
  расшифровки) и «для живого использования» (расшифровка, иначе аккаунт
  пропускается).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import Account, AccountSessionStatus, AccountSummary
from src.security import SessionCipher, decrypt_failure_status, log_expected_decrypt_failure
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database

logger = logging.getLogger(__name__)

_RESTORE_ACCOUNT_ACTION = "restore_key_or_relogin"


class AccountSessionDecryptError(RuntimeError):
    """Расшифровать StringSession аккаунта не удалось (нет ключа, чужая версия, повреждение).

    Несёт ``resource``/``identifier``/``status``/``action`` для единообразного
    отчёта; поднимается на пути «живого» использования, где аккаунт без
    читаемой сессии работать не может.
    """

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
    """CRUD Telegram-аккаунтов с инвариантом single-primary и шифрованием сессий."""

    def __init__(
        self,
        db: ReadConnection,
        session_cipher: SessionCipher | None = None,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._session_cipher = session_cipher
        self._database = database

    async def add_account(self, account: Account) -> int:
        """Добавить аккаунт (или обновить существующий по телефону через UPSERT); вернуть id.

        Возвращает `cur.lastrowid`: это надёжный id только на ветке вставки; при
        конфликте-обновлении SQLite оставляет lastrowid от последней вставки в
        соединении, поэтому для гарантированного id обновлённой строки читайте её
        отдельно по `phone`.

        Сессия шифруется при наличии cipher. Флаг primary назначается атомарно —
        запрошенный ``is_primary`` срабатывает, только если primary-аккаунта ещё
        нет (#733), иначе сохраняется 0.
        """
        assert self._database is not None, (
            "AccountsRepository.add_account requires a Database reference"
        )
        session_string = account.session_string
        if self._session_cipher:
            session_string = self._session_cipher.encrypt(session_string)

        # Derive is_primary atomically (#733): the requested primary flag only
        # takes effect when no primary account exists yet. Two concurrent inserts
        # that each pass is_primary=1 can no longer both win — the subquery is
        # evaluated against the table at insert time, so the second insert sees
        # the first's primary row and stores 0. The partial unique index
        # idx_accounts_single_primary is the hard backstop behind this.
        want_primary = int(account.is_primary)
        cur = await self._database.execute_write(
            """INSERT INTO accounts (phone, session_string, is_primary, is_active, is_premium)
               VALUES (
                   ?, ?,
                   CASE WHEN ? = 1 AND NOT EXISTS (SELECT 1 FROM accounts WHERE is_primary = 1)
                        THEN 1 ELSE 0 END,
                   ?, ?
               )
               ON CONFLICT(phone) DO UPDATE SET
                   session_string=excluded.session_string,
                   is_active=excluded.is_active,
                   is_premium=excluded.is_premium""",
            (
                account.phone,
                session_string,
                want_primary,
                int(account.is_active),
                int(account.is_premium),
            ),
        )
        return cur.lastrowid or 0

    async def add_account_if_absent(self, account: Account) -> int | None:
        """Insert an account ONLY if its phone is not already present; never overwrite.

        Returns the new row id, or ``None`` if a row for this phone already exists.
        Unlike :meth:`add_account` (an ``ON CONFLICT DO UPDATE`` upsert), this is
        ``ON CONFLICT(phone) DO NOTHING``, so a concurrent duplicate import can't
        clobber a working session via a check-then-act TOCTOU — the DB is the single
        source of truth for "already exists" (#1146 review). Session is encrypted as
        usual; is_primary is still derived atomically (#733).
        """
        assert self._database is not None, (
            "AccountsRepository.add_account_if_absent requires a Database reference"
        )
        session_string = account.session_string
        if self._session_cipher:
            session_string = self._session_cipher.encrypt(session_string)

        want_primary = int(account.is_primary)
        cur = await self._database.execute_write(
            """INSERT INTO accounts (phone, session_string, is_primary, is_active, is_premium)
               VALUES (
                   ?, ?,
                   CASE WHEN ? = 1 AND NOT EXISTS (SELECT 1 FROM accounts WHERE is_primary = 1)
                        THEN 1 ELSE 0 END,
                   ?, ?
               )
               ON CONFLICT(phone) DO NOTHING""",
            (
                account.phone,
                session_string,
                want_primary,
                int(account.is_active),
                int(account.is_premium),
            ),
        )
        # rowcount is 1 on insert, 0 when the conflict was ignored.
        return cur.lastrowid if cur.rowcount else None

    async def migrate_sessions(self) -> int:
        """Re-encrypt plaintext sessions to the current (enc:v2) format.

        Unsupported encrypted strings (e.g. removed enc:v1, or enc:v* without a key)
        are skipped and logged via the expected-decrypt-failure path, not migrated.
        """
        if not self._session_cipher:
            return 0

        cur = await self._db.execute("SELECT id, phone, session_string FROM accounts")
        rows = await cur.fetchall()
        if not rows:
            return 0

        assert self._database is not None, (
            "AccountsRepository.migrate_sessions requires a Database reference"
        )

        migrated = 0
        async with self._database.transaction() as conn:
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
                    await conn.execute(
                        "UPDATE accounts SET session_string = ? WHERE id = ?",
                        (migrated_value, row["id"]),
                    )
                    migrated += 1
                    logger.info("Migrated session format for phone=%s", row["phone"])

        return migrated

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
            flood_wait_until=parse_datetime(row["flood_wait_until"]),
            created_at=parse_datetime(row["created_at"]),
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
        """Список аккаунтов как безопасные [`AccountSummary`][src.models.AccountSummary] (без секрета сессии).

        Сессия не расшифровывается — вместо этого классифицируется её статус
        (`session_status`), так что нечитаемый аккаунт всё равно отображается.
        Порядок: primary первым, затем по id. С ``active_only`` — только активные.
        """
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

    def _account_from_row(self, row, session_string: str) -> Account:
        """Build an [`Account`][src.models.Account] from a row + pre-resolved session.

        Shared by the listing paths that decrypt every row's session up front
        (`get_accounts`, `get_live_usable_accounts`); the caller decides whether
        a decrypt failure aborts the whole list (`get_accounts`) or skips just
        that account (`get_live_usable_accounts`).
        """
        return Account(
            id=row["id"],
            phone=row["phone"],
            session_string=session_string,
            is_primary=bool(row["is_primary"]),
            is_active=bool(row["is_active"]),
            is_premium=bool(row["is_premium"]) if row["is_premium"] is not None else False,
            flood_wait_until=parse_datetime(row["flood_wait_until"]),
            created_at=parse_datetime(row["created_at"]),
        )

    async def get_accounts(self, active_only: bool = False) -> list[Account]:
        """Полные [`Account`][src.models.Account] с расшифрованными сессиями для живого использования.

        В отличие от :meth:`get_account_summaries`, расшифровывает каждую сессию —
        нечитаемая сессия поднимает
        [`AccountSessionDecryptError`][src.database.repositories.accounts.AccountSessionDecryptError]
        (если нужен отказоустойчивый вариант, фильтрующий битые аккаунты, см.
        :meth:`get_live_usable_accounts`). Порядок: primary первым, затем по id.
        """
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
            accounts.append(self._account_from_row(row, session_string))

        return accounts

    async def get_decrypted_session(
        self, *, account_id: int | None = None, phone: str | None = None
    ) -> str | None:
        """Decrypt and return ONE account's session string, or ``None`` if no such account.

        Unlike :meth:`get_accounts`, this reads and decrypts only the single selected
        row, so a broken/undecryptable *sibling* account cannot abort the lookup — the
        exact recovery path SSO export needs (#828). Exactly one of ``account_id`` /
        ``phone`` must be given. A decrypt failure on the *target* still raises
        :class:`AccountSessionDecryptError` (the caller wants to know).
        """
        exported = await self.get_session_export(account_id=account_id, phone=phone)
        if exported is None:
            return None
        return exported[1]

    async def get_session_export(
        self, *, account_id: int | None = None, phone: str | None = None
    ) -> tuple[str, str] | None:
        """Return ``(phone, decrypted_session)`` from a SINGLE row, or ``None`` if absent.

        Unlike resolving identity and decrypting in two separate awaits, this binds
        the phone and the session to the SAME row read, so a concurrent delete+reinsert
        between the two steps can't pair a fresh session with a stale phone (SQLite
        reuses rowids) — a session-export consistency hazard (#1145 review). Exactly
        one of ``account_id`` / ``phone`` must be given. Decrypt failure on the target
        still raises :class:`AccountSessionDecryptError`.
        """
        if (account_id is None) == (phone is None):
            raise ValueError("provide exactly one of account_id / phone")
        if account_id is not None:
            cur = await self._db.execute(
                "SELECT phone, session_string FROM accounts WHERE id = ?", (account_id,)
            )
        else:
            cur = await self._db.execute(
                "SELECT phone, session_string FROM accounts WHERE phone = ?", (phone,)
            )
        row = await cur.fetchone()
        if row is None:
            return None
        row_phone = str(row["phone"])
        raw_session = str(row["session_string"] or "")
        return row_phone, self._decrypt_session_for_live_use(raw_session, row_phone)

    async def get_live_usable_accounts(self, active_only: bool = False) -> list[Account]:
        """Return accounts whose sessions can be decrypted for live Telegram use."""
        sql = "SELECT * FROM accounts"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        accounts: list[Account] = []

        for row in rows:
            raw_session = str(row["session_string"] or "")
            phone = str(row["phone"])
            try:
                session_string = self._decrypt_session_for_live_use(raw_session, phone)
            except AccountSessionDecryptError as exc:
                logger.warning(
                    "Skipping degraded Telegram account for live use: phone=%s status=%s",
                    exc.identifier,
                    exc.status,
                )
                continue

            accounts.append(self._account_from_row(row, session_string))

        return accounts

    async def update_account_flood(self, phone: str, until: datetime | None) -> None:
        """Записать (или снять через ``None``) момент окончания Telegram FLOOD_WAIT для аккаунта.

        `ClientPool` пропускает аккаунты, у которых ``flood_wait_until`` ещё в будущем.
        """
        assert self._database is not None, (
            "AccountsRepository.update_account_flood requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE accounts SET flood_wait_until = ? WHERE phone = ?",
            (until.isoformat() if until else None, phone),
        )

    async def update_account_premium(self, phone: str, is_premium: bool) -> None:
        """Обновить флаг Telegram Premium у аккаунта (нужен для premium-only операций)."""
        assert self._database is not None, (
            "AccountsRepository.update_account_premium requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE accounts SET is_premium = ? WHERE phone = ?",
            (int(is_premium), phone),
        )

    @staticmethod
    async def _promote_primary_if_none(conn, *, active_only: bool) -> None:
        """Promote the lowest-id candidate to primary iff no primary exists.

        Single owner of the "fill the primary gap" half of the one-primary
        invariant (#733), shared by set_account_active and delete_account so the
        candidate-selection rule cannot drift between call sites. With
        ``active_only`` the candidate pool is restricted to active accounts.
        """
        candidate_filter = "WHERE is_active = 1\n            " if active_only else ""
        await conn.execute(
            f"""
            UPDATE accounts SET is_primary = 1
            WHERE id = (
                SELECT id FROM accounts
                {candidate_filter}ORDER BY id ASC LIMIT 1
            )
            AND NOT EXISTS (SELECT 1 FROM accounts WHERE is_primary = 1)
            """
        )

    async def set_account_active(self, account_id: int, active: bool) -> None:
        """Включить/выключить аккаунт, поддерживая инвариант single-primary (#733).

        При активации — назначает primary, если ни одного нет. При деактивации
        текущего primary — снимает флаг и продвигает на роль primary самый ранний
        оставшийся активный аккаунт (если активных не осталось, primary временно нет).
        """
        assert self._database is not None, (
            "AccountsRepository.set_account_active requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                "UPDATE accounts SET is_active = ? WHERE id = ?", (int(active), account_id)
            )
            if (cur.rowcount or 0) == 0:
                return
            if active:
                await conn.execute(
                    """
                    UPDATE accounts SET is_primary = 1
                    WHERE id = ?
                    AND NOT EXISTS (SELECT 1 FROM accounts WHERE is_primary = 1)
                    """,
                    (account_id,),
                )
            else:
                # Deactivating the current primary: demote it and promote the
                # lowest-id remaining ACTIVE account. If none stay active, leave
                # zero primary (acceptable — the user may disable everything).
                await conn.execute(
                    "UPDATE accounts SET is_primary = 0 WHERE id = ? AND is_primary = 1",
                    (account_id,),
                )
                await self._promote_primary_if_none(conn, active_only=True)

    async def set_account_primary(self, account_id: int) -> bool:
        """Atomically make *account_id* the sole primary, demoting the previous one.

        Returns False if the account does not exist (no-op). The partial unique
        index idx_accounts_single_primary (#733) is the hard backstop; doing both
        UPDATEs inside one transaction keeps the intermediate state valid at COMMIT.
        """
        assert self._database is not None, (
            "AccountsRepository.set_account_primary requires a Database reference"
        )
        async with self._database.transaction() as conn:
            # The partial unique index idx_accounts_single_primary (#733) is
            # checked immediately, so demote MUST precede promote — two primaries
            # can never coexist even mid-transaction. That forces the existence
            # check up front (rowcount-on-promote would arrive too late), so the
            # SELECT is load-bearing, not redundant.
            cur = await conn.execute("SELECT 1 FROM accounts WHERE id = ?", (account_id,))
            if await cur.fetchone() is None:
                return False
            await conn.execute("UPDATE accounts SET is_primary = 0 WHERE is_primary = 1")
            await conn.execute("UPDATE accounts SET is_primary = 1 WHERE id = ?", (account_id,))
        return True

    async def delete_account(self, account_id: int) -> None:
        """Удалить аккаунт; если удалён был primary — продвинуть на его роль самый ранний оставшийся (#733)."""
        assert self._database is not None, (
            "AccountsRepository.delete_account requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            if (cur.rowcount or 0) > 0:
                await self._promote_primary_if_none(conn, active_only=False)
