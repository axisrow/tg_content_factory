"""Репозиторий key/value настроек приложения (таблица settings)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tg_agent.database.pool import ReadConnection
from tg_agent.security import SessionCipher

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


class SettingsRepository:
    """Хранилище key/value настроек приложения в таблице `settings`.

    Простое строковое key/value (значения сериализуются вызывающим кодом):
    держит секреты вроде ключа подписи сессий, флаги dev-режима, конфиги
    агента/фильтров и т.п. Запись — upsert через `set_setting`.

    Секреты (API-ключи и т.п.) пишутся через `set_secret` — шифруются тем же
    SessionCipher, что и StringSession аккаунтов (enc:v2, SESSION_ENCRYPTION_KEY).
    Без настроенного секрета запись секрета отклоняется: plaintext в БД хуже
    явного отказа (issue #1597).
    """

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
        session_cipher: SessionCipher | None = None,
    ):
        self._db = db
        self._database = database
        self._session_cipher = session_cipher

    @property
    def secrets_enabled(self) -> bool:
        """Доступно ли шифрование секретов (задан SESSION_ENCRYPTION_KEY)."""
        return self._session_cipher is not None

    async def get_setting(self, key: str) -> str | None:
        """Значение настройки по ключу, либо None если ключа нет."""
        cur = await self._db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def get_secret(self, key: str) -> str | None:
        """Расшифрованное значение секрета; legacy-plaintext проходит как есть."""
        value = await self.get_setting(key)
        if value is None or not SessionCipher.is_encrypted(value):
            return value
        if self._session_cipher is None:
            raise RuntimeError(
                f"Setting {key!r} is stored encrypted but SESSION_ENCRYPTION_KEY is not "
                "configured; set SESSION_ENCRYPTION_KEY or re-enter the secret."
            )
        return self._session_cipher.decrypt(value)

    async def set_secret(self, key: str, value: str) -> None:
        """Записать секрет шифрованным (enc:v2); без SESSION_ENCRYPTION_KEY — отказ."""
        if self._session_cipher is None:
            raise RuntimeError(
                "SESSION_ENCRYPTION_KEY is required to store secrets in settings "
                f"({key!r}); without it the value would be saved as plaintext."
            )
        await self.set_setting(key, self._session_cipher.encrypt(value))

    async def list_all(self) -> list[tuple[str, str]]:
        """Все настройки как список пар (key, value), отсортированный по ключу."""
        cur = await self._db.execute("SELECT key, value FROM settings ORDER BY key")
        rows = await cur.fetchall()
        return [(r["key"], r["value"]) for r in rows]

    async def get_settings_by_prefix(self, prefix: str) -> dict[str, str]:
        """Настройки, чей ключ начинается с `prefix`, как dict (спецсимволы LIKE экранируются)."""
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        cur = await self._db.execute(
            "SELECT key, value FROM settings WHERE key LIKE ? ESCAPE '\\'",
            (f"{escaped}%",),
        )
        rows = await cur.fetchall()
        return {r["key"]: r["value"] for r in rows}

    async def set_setting(self, key: str, value: str) -> None:
        """Записать/обновить настройку (upsert по ключу)."""
        assert self._database is not None, (
            "SettingsRepository.set_setting requires a Database reference"
        )
        await self._database.execute_write(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    async def record_setting_decision(
        self,
        key: str,
        value: str,
        *,
        origin: str,
        actor: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Записать настройку как осознанное решение и занести его в журнал (#1315).

        Значение и запись журнала ``decisions`` (``entity='setting'``,
        ``entity_name=key``, ``field='value'``) коммитятся одной транзакцией, так
        что по журналу можно отличить решение оператора от унаследованного
        состояния, которое автоматика вправе мигрировать.
        """
        assert self._database is not None, (
            "SettingsRepository.record_setting_decision requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cur.fetchone()
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            await self._database.repos.decisions.record(
                entity="setting",
                entity_name=key,
                field="value",
                old_value=row["value"] if row else None,
                new_value=value,
                origin=origin,
                actor=actor,
                reason=reason,
                commit=False,
            )

    async def has_setting_decision(self, key: str) -> bool:
        """Есть ли в журнале хоть одно решение по ключу ``key`` (#1315)."""
        assert self._database is not None, (
            "SettingsRepository.has_setting_decision requires a Database reference"
        )
        history = await self._database.repos.decisions.history(
            "setting", None, field="value", limit=1, entity_name=key
        )
        return bool(history)
