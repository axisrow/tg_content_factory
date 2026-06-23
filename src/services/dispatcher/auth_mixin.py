"""Auth command handlers for :class:`TelegramCommandDispatcher` (#1047).

Domain: ``auth.*`` — phone-code send/resend/verify and account materialization.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.models import Account

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class AuthCommandsMixin(_Base):
    """``auth.*`` command handlers.

    Relies on the facade for ``self._auth`` (TelegramAuth) and ``self._db``;
    ``_handle_auth_verify_code`` chains into ``self._handle_accounts_connect``
    which lives on the accounts side of the dispatcher.
    """

    async def _handle_auth_send_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        result = await self._auth.send_code(phone)
        return {"result": {"phone": phone, **result}}

    async def _handle_auth_resend_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        result = await self._auth.resend_code(phone)
        return {"result": {"phone": phone, **result}}

    async def _handle_auth_verify_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        password_2fa = str(payload.get("password_2fa", "")).strip() or None
        payload["password_2fa"] = ""
        session_string = await self._auth.verify_code(
            phone,
            str(payload["code"]),
            str(payload["phone_code_hash"]),
            password_2fa,
        )
        existing = await self._db.get_account_summaries(active_only=False)
        account = Account(
            phone=phone,
            session_string=session_string,
            is_primary=not any(acc.phone == phone for acc in existing) and len(existing) == 0,
            is_premium=False,
        )
        await self._db.add_account(account)
        connect_result = await self._handle_accounts_connect({"phone": phone})
        return {"result": {"phone": phone, **connect_result}, "payload_update": {**payload}}
