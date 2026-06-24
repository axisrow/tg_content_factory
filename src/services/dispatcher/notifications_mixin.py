"""Notification bot command handlers (#1047).

Domain: ``notifications.*`` — setup / teardown of the personal notification bot
and worker me-cache invalidation.

``notifications.test`` and the ``_notification_target_service`` factory stay on
the facade class: the existing suite patches ``Notifier`` /
``NotificationTargetService`` through the facade module namespace, so their call
sites must resolve those names from there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class NotificationsCommandsMixin(_Base):
    """``notifications.*`` command handlers (except the patch-sensitive test)."""

    def _notification_service(self) -> NotificationService:
        from src.database.bundles import NotificationBundle

        target_service = NotificationTargetService(NotificationBundle.from_database(self._db), self._pool)
        kwargs: dict[str, Any] = {}
        if self._config is not None:
            kwargs["bot_name_prefix"] = self._config.notifications.bot_name_prefix
            kwargs["bot_username_prefix"] = self._config.notifications.bot_username_prefix
        return NotificationService(self._db, target_service, **kwargs)

    async def _handle_notifications_setup_bot(self, payload: dict[str, Any]) -> dict[str, Any]:
        bot = await self._notification_service().setup_bot()
        return {"bot_username": bot.bot_username, "bot_id": bot.bot_id}

    async def _handle_notifications_delete_bot(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._notification_service().teardown_bot()
        return {"deleted": True}

    async def _handle_notifications_invalidate_cache(self, payload: dict[str, Any]) -> dict[str, Any]:
        # The web process calls notifier.invalidate_me_cache(), but its container's
        # notifier is None — the live Notifier lives only in the worker. So a
        # notification-account change must reach the worker over the command queue
        # and invalidate the SHARED worker Notifier here, or it keeps sending from
        # the old account's me.id until the worker restarts (#832).
        if self._notifier is not None:
            self._notifier.invalidate_me_cache()
        return {"invalidated": True}
