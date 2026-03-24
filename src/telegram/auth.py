from __future__ import annotations

import logging

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.functions.auth import ResendCodeRequest
from telethon.tl.types.auth import (
    CodeTypeCall,
    CodeTypeFlashCall,
    CodeTypeFragmentSms,
    CodeTypeMissedCall,
    CodeTypeSms,
    SentCodeTypeApp,
    SentCodeTypeCall,
    SentCodeTypeEmailCode,
    SentCodeTypeFirebaseSms,
    SentCodeTypeFlashCall,
    SentCodeTypeFragmentSms,
    SentCodeTypeMissedCall,
    SentCodeTypeSetUpEmailRequired,
    SentCodeTypeSms,
    SentCodeTypeSmsPhrase,
    SentCodeTypeSmsWord,
)

logger = logging.getLogger(__name__)


def _describe_code_type(sent_code_type: object) -> str:
    """Map Telethon SentCodeType to human-readable Russian description."""
    mapping = {
        SentCodeTypeApp: "приложение Telegram",
        SentCodeTypeSms: "SMS",
        SentCodeTypeCall: "телефонный звонок",
        SentCodeTypeFlashCall: "flash-звонок",
        SentCodeTypeMissedCall: "пропущенный звонок",
        SentCodeTypeFirebaseSms: "SMS",
        SentCodeTypeFragmentSms: "SMS (Fragment)",
        SentCodeTypeSmsPhrase: "SMS (фраза)",
        SentCodeTypeSmsWord: "SMS (слово)",
        SentCodeTypeEmailCode: "email",
        SentCodeTypeSetUpEmailRequired: "email (требуется настройка)",
    }
    for cls, label in mapping.items():
        if isinstance(sent_code_type, cls):
            return label
    return "Telegram"


def _describe_next_type(next_type: object | None) -> str | None:
    """Map Telethon CodeType (next_type) to human-readable Russian description."""
    if next_type is None:
        return None
    mapping = {
        CodeTypeSms: "SMS",
        CodeTypeCall: "звонок",
        CodeTypeMissedCall: "пропущенный звонок",
        CodeTypeFlashCall: "flash-звонок",
        CodeTypeFragmentSms: "SMS (Fragment)",
    }
    for cls, label in mapping.items():
        if isinstance(next_type, cls):
            return label
    return None


class TelegramAuth:
    def __init__(self, api_id: int, api_hash: str):
        self._api_id = api_id
        self._api_hash = api_hash
        self._pending: dict[str, tuple[TelegramClient, str]] = {}

    @property
    def api_id(self) -> int:
        return self._api_id

    @property
    def api_hash(self) -> str:
        return self._api_hash

    @property
    def is_configured(self) -> bool:
        return self._api_id != 0 and self._api_hash != ""

    def update_credentials(self, api_id: int, api_hash: str) -> None:
        self._api_id = api_id
        self._api_hash = api_hash

    async def _disconnect_pending_client(self, phone: str) -> None:
        pending = self._pending.pop(phone, None)
        if pending is None:
            return
        client, _ = pending
        try:
            await client.disconnect()
        except Exception:
            logger.warning("Failed to disconnect previous pending auth client for %s", phone)

    async def send_code(self, phone: str) -> dict:
        """Send auth code to phone. Returns dict with hash, type info, timeout."""
        await self._disconnect_pending_client(phone)
        client = TelegramClient(StringSession(), self._api_id, self._api_hash)
        await client.connect()
        try:
            result = await client.send_code_request(phone)
        except Exception:
            try:
                await client.disconnect()
            except Exception:
                logger.warning("Failed to disconnect temporary auth client for %s", phone)
            raise
        self._pending[phone] = (client, result.phone_code_hash)
        logger.info(
            "Auth code sent to %s: type=%s, next_type=%s, timeout=%s",
            phone,
            type(result.type).__name__,
            type(getattr(result, "next_type", None)).__name__,
            getattr(result, "timeout", None),
        )
        return {
            "phone_code_hash": result.phone_code_hash,
            "code_type": _describe_code_type(result.type),
            "next_type": _describe_next_type(getattr(result, "next_type", None)),
            "timeout": getattr(result, "timeout", None),
        }

    async def resend_code(self, phone: str) -> dict:
        """Resend auth code via next delivery method. Returns same dict as send_code."""
        if phone not in self._pending:
            raise ValueError(f"No pending auth for {phone}. Send code first.")
        client, phone_code_hash = self._pending[phone]
        result = await client(
            ResendCodeRequest(
                phone_number=phone,
                phone_code_hash=phone_code_hash,
            )
        )
        new_hash = result.phone_code_hash
        self._pending[phone] = (client, new_hash)
        logger.info(
            "Auth code resent to %s: type=%s, next_type=%s, timeout=%s",
            phone,
            type(result.type).__name__,
            type(getattr(result, "next_type", None)).__name__,
            getattr(result, "timeout", None),
        )
        return {
            "phone_code_hash": new_hash,
            "code_type": _describe_code_type(result.type),
            "next_type": _describe_next_type(getattr(result, "next_type", None)),
            "timeout": getattr(result, "timeout", None),
        }

    async def verify_code(
        self,
        phone: str,
        code: str,
        phone_code_hash: str,
        password_2fa: str | None = None,
    ) -> str:
        """Verify code and return session string."""
        if phone not in self._pending:
            raise ValueError(f"No pending auth for {phone}. Send code first.")

        client, stored_hash = self._pending[phone]
        if stored_hash != phone_code_hash:
            raise ValueError("Phone code hash mismatch")

        try:
            try:
                await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
            except SessionPasswordNeededError:
                if not password_2fa:
                    raise ValueError("2FA password required")
                await client.sign_in(password=password_2fa)
            session_string = client.session.save()
        finally:
            del self._pending[phone]
            try:
                await client.disconnect()
            except Exception:
                logger.warning("Failed to disconnect temporary auth client for %s", phone)

        logger.info("Successfully authenticated %s", phone)
        return session_string

    async def create_client_from_session(self, session_string: str) -> TelegramClient:
        """Create and connect a client from saved session string."""
        client = TelegramClient(
            StringSession(session_string), self._api_id, self._api_hash,
            connection_retries=None, retry_delay=2,
        )
        await client.connect()
        if not await client.is_user_authorized():
            raise ConnectionError("Session is no longer valid")
        return client

    async def cleanup(self) -> None:
        """Disconnect any pending clients."""
        for phone, (client, _) in self._pending.items():
            try:
                await client.disconnect()
            except Exception:
                pass
        self._pending.clear()
