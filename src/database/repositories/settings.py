"""Репозиторий key/value настроек приложения (таблица settings)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from src.database.facade import Database


class SettingsRepository:
    """Хранилище key/value настроек приложения в таблице `settings`.

    Простое строковое key/value (значения сериализуются вызывающим кодом):
    держит секреты вроде ключа подписи сессий, флаги dev-режима, конфиги
    агента/фильтров и т.п. Запись — upsert через `set_setting`.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def get_setting(self, key: str) -> str | None:
        """Значение настройки по ключу, либо None если ключа нет."""
        cur = await self._db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

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
