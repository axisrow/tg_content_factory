from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from src.database import Database
from src.models import Account
from src.telegram.client_pool import ClientPool

SETTING_KEY = "notification_account_phone"


@dataclass(frozen=True)
class NotificationTargetStatus:
    mode: str
    state: str
    message: str
    configured_phone: str | None = None
    effective_phone: str | None = None


class NotificationTargetService:
    def __init__(self, db: Database, pool: ClientPool):
        self._db = db
        self._pool = pool

    async def get_configured_phone(self) -> str | None:
        raw = (await self._db.get_setting(SETTING_KEY) or "").strip()
        return raw or None

    async def set_configured_phone(self, phone: str | None) -> None:
        await self._db.set_setting(SETTING_KEY, phone or "")

    async def describe_target(self) -> NotificationTargetStatus:
        accounts = await self._db.get_accounts()
        configured_phone = await self.get_configured_phone()

        if configured_phone:
            account = next((acc for acc in accounts if acc.phone == configured_phone), None)
            return self._describe_account(
                account,
                configured_phone=configured_phone,
                mode="selected",
                missing_message="Выбранный аккаунт уведомлений удалён.",
            )

        primary = next((acc for acc in accounts if acc.is_primary), None)
        return self._describe_account(
            primary,
            configured_phone=None,
            mode="primary",
            missing_message="Primary-аккаунт для уведомлений не найден.",
        )

    def _describe_account(
        self,
        account: Account | None,
        *,
        configured_phone: str | None,
        mode: str,
        missing_message: str,
    ) -> NotificationTargetStatus:
        clients = getattr(self._pool, "clients", None)
        if not isinstance(clients, dict):
            clients = {}

        if account is None:
            return NotificationTargetStatus(
                mode=mode,
                state="missing",
                message=missing_message,
                configured_phone=configured_phone,
            )
        if not account.is_active:
            return NotificationTargetStatus(
                mode=mode,
                state="inactive",
                message=f"Аккаунт {account.phone} отключён.",
                configured_phone=configured_phone,
                effective_phone=account.phone,
            )

        flood_until = self._normalize_utc(account.flood_wait_until)
        if flood_until and flood_until > datetime.now(timezone.utc):
            return NotificationTargetStatus(
                mode=mode,
                state="flood_wait",
                message=(
                    f"Аккаунт {account.phone} находится в FloodWait "
                    f"до {flood_until.isoformat()}."
                ),
                configured_phone=configured_phone,
                effective_phone=account.phone,
            )

        if account.phone not in clients:
            return NotificationTargetStatus(
                mode=mode,
                state="disconnected",
                message=f"Аккаунт {account.phone} не подключён.",
                configured_phone=configured_phone,
                effective_phone=account.phone,
            )

        return NotificationTargetStatus(
            mode=mode,
            state="available",
            message=f"Уведомления идут через {account.phone}.",
            configured_phone=configured_phone,
            effective_phone=account.phone,
        )

    @asynccontextmanager
    async def use_client(self):
        status = await self.describe_target()
        if status.state != "available" or status.effective_phone is None:
            raise RuntimeError(status.message)

        result = await self._pool.get_client_by_phone(status.effective_phone)
        if result is None:
            raise RuntimeError(f"Аккаунт {status.effective_phone} временно недоступен.")

        client, phone = result
        try:
            yield client, phone
        finally:
            await self._pool.release_client(phone)

    @staticmethod
    def _normalize_utc(value):
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
