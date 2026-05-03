from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable
from typing import TypeVar

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

AUTH_CONNECT_TIMEOUT_SECONDS = 30
AUTH_RPC_TIMEOUT_SECONDS = 60

_T = TypeVar("_T")


class TelegramAuthTimeoutError(TimeoutError):
    """Raised when a Telegram auth operation exceeds its explicit timeout."""


def _format_timeout(seconds: float) -> str:
    if float(seconds).is_integer():
        return f"{int(seconds)}s"
    return f"{seconds:g}s"


async def _with_auth_timeout(
    awaitable: Awaitable[_T],
    operation: str,
    timeout_seconds: float,
) -> _T:
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_seconds)
    except TimeoutError as exc:
        raise TelegramAuthTimeoutError(
            f"telegram_auth_timeout: {operation} timed out after {_format_timeout(timeout_seconds)}"
        ) from exc


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
        started_at = time.monotonic()
        logger.info("auth.send_code start phone=%s", phone)
        await self._disconnect_pending_client(phone)
        client = TelegramClient(StringSession(), self._api_id, self._api_hash)
        try:
            logger.info(
                "auth.send_code connect start phone=%s timeout_s=%s",
                phone,
                AUTH_CONNECT_TIMEOUT_SECONDS,
            )
            await _with_auth_timeout(client.connect(), "connect", AUTH_CONNECT_TIMEOUT_SECONDS)
            logger.info(
                "auth.send_code rpc start phone=%s rpc=send_code_request timeout_s=%s",
                phone,
                AUTH_RPC_TIMEOUT_SECONDS,
            )
            result = await _with_auth_timeout(
                client.send_code_request(phone),
                "send_code_request",
                AUTH_RPC_TIMEOUT_SECONDS,
            )
        except Exception:
            try:
                await client.disconnect()
            except Exception:
                logger.warning("Failed to disconnect temporary auth client for %s", phone)
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.exception("auth.send_code error phone=%s duration_ms=%d", phone, duration_ms)
            raise
        self._pending[phone] = (client, result.phone_code_hash)
        duration_ms = int((time.monotonic() - started_at) * 1000)
        logger.info(
            "auth.send_code success phone=%s duration_ms=%d code_type=%s next_type=%s timeout=%s",
            phone,
            duration_ms,
            type(result.type).__name__,
            type(getattr(result, "next_type", None)).__name__,
            getattr(result, "timeout", None),
        )
        return {
            "phone_code_hash": result.phone_code_hash,
            "session_str": client.session.save(),
            "code_type": _describe_code_type(result.type),
            "next_type": _describe_next_type(getattr(result, "next_type", None)),
            "timeout": getattr(result, "timeout", None),
        }

    async def resend_code(self, phone: str) -> dict:
        """Resend auth code via next delivery method. Returns same dict as send_code."""
        started_at = time.monotonic()
        logger.info("auth.resend_code start phone=%s", phone)
        if phone not in self._pending:
            raise ValueError(f"No pending auth for {phone}. Send code first.")
        client, phone_code_hash = self._pending[phone]
        try:
            logger.info(
                "auth.resend_code rpc start phone=%s rpc=resend_code timeout_s=%s",
                phone,
                AUTH_RPC_TIMEOUT_SECONDS,
            )
            result = await _with_auth_timeout(
                client(
                    ResendCodeRequest(
                        phone_number=phone,
                        phone_code_hash=phone_code_hash,
                    )
                ),
                "resend_code",
                AUTH_RPC_TIMEOUT_SECONDS,
            )
        except Exception:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.exception("auth.resend_code error phone=%s duration_ms=%d", phone, duration_ms)
            raise
        new_hash = result.phone_code_hash
        self._pending[phone] = (client, new_hash)
        duration_ms = int((time.monotonic() - started_at) * 1000)
        logger.info(
            "auth.resend_code success phone=%s duration_ms=%d code_type=%s next_type=%s timeout=%s",
            phone,
            duration_ms,
            type(result.type).__name__,
            type(getattr(result, "next_type", None)).__name__,
            getattr(result, "timeout", None),
        )
        return {
            "phone_code_hash": new_hash,
            "session_str": client.session.save(),
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
        started_at = time.monotonic()
        logger.info("auth.verify_code start phone=%s", phone)
        if phone not in self._pending:
            raise ValueError(f"No pending auth for {phone}. Send code first.")

        client, stored_hash = self._pending[phone]
        if stored_hash != phone_code_hash:
            raise ValueError("Phone code hash mismatch")

        needs_2fa = False
        try:
            try:
                logger.info(
                    "auth.verify_code rpc start phone=%s rpc=sign_in timeout_s=%s",
                    phone,
                    AUTH_RPC_TIMEOUT_SECONDS,
                )
                await _with_auth_timeout(
                    client.sign_in(phone, code, phone_code_hash=phone_code_hash),
                    "sign_in",
                    AUTH_RPC_TIMEOUT_SECONDS,
                )
            except SessionPasswordNeededError:
                if not password_2fa:
                    needs_2fa = True
                    raise ValueError("2FA password required")
                logger.info(
                    "auth.verify_code rpc start phone=%s rpc=sign_in_2fa timeout_s=%s",
                    phone,
                    AUTH_RPC_TIMEOUT_SECONDS,
                )
                await _with_auth_timeout(
                    client.sign_in(password=password_2fa),
                    "sign_in",
                    AUTH_RPC_TIMEOUT_SECONDS,
                )
            session_string = client.session.save()
        except Exception:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.exception("auth.verify_code error phone=%s duration_ms=%d", phone, duration_ms)
            raise
        finally:
            if not needs_2fa:
                del self._pending[phone]
                try:
                    await client.disconnect()
                except Exception:
                    logger.warning("Failed to disconnect temporary auth client for %s", phone)

        duration_ms = int((time.monotonic() - started_at) * 1000)
        logger.info("auth.verify_code success phone=%s duration_ms=%d", phone, duration_ms)
        return session_string

    async def sign_in_fresh(
        self,
        phone: str,
        code: str,
        phone_code_hash: str,
        session_str: str = "",
        password_2fa: str | None = None,
        code_consumed: bool = False,
    ) -> str:
        """Sign in using a phone_code_hash from a previous send_code call (possibly in a different process).

        Pass session_str from the send_code result to reuse the same MTProto session —
        required because Telegram binds phone_code_hash to the session that sent the request.
        """
        started_at = time.monotonic()
        logger.info("auth.sign_in_fresh start phone=%s", phone)
        client = TelegramClient(StringSession(session_str), self._api_id, self._api_hash)
        try:
            logger.info(
                "auth.sign_in_fresh connect start phone=%s timeout_s=%s",
                phone,
                AUTH_CONNECT_TIMEOUT_SECONDS,
            )
            await _with_auth_timeout(client.connect(), "connect", AUTH_CONNECT_TIMEOUT_SECONDS)
            if code_consumed:
                if not password_2fa:
                    raise ValueError("2FA password required")
                logger.info(
                    "auth.sign_in_fresh rpc start phone=%s rpc=sign_in_2fa timeout_s=%s",
                    phone,
                    AUTH_RPC_TIMEOUT_SECONDS,
                )
                await _with_auth_timeout(
                    client.sign_in(password=password_2fa),
                    "sign_in",
                    AUTH_RPC_TIMEOUT_SECONDS,
                )
            else:
                try:
                    logger.info(
                        "auth.sign_in_fresh rpc start phone=%s rpc=sign_in timeout_s=%s",
                        phone,
                        AUTH_RPC_TIMEOUT_SECONDS,
                    )
                    await _with_auth_timeout(
                        client.sign_in(phone, code, phone_code_hash=phone_code_hash),
                        "sign_in",
                        AUTH_RPC_TIMEOUT_SECONDS,
                    )
                except SessionPasswordNeededError:
                    if not password_2fa:
                        raise ValueError("2FA password required")
                    logger.info(
                        "auth.sign_in_fresh rpc start phone=%s rpc=sign_in_2fa timeout_s=%s",
                        phone,
                        AUTH_RPC_TIMEOUT_SECONDS,
                    )
                    await _with_auth_timeout(
                        client.sign_in(password=password_2fa),
                        "sign_in",
                        AUTH_RPC_TIMEOUT_SECONDS,
                    )
            session_string = client.session.save()
        except Exception:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.exception("auth.sign_in_fresh error phone=%s duration_ms=%d", phone, duration_ms)
            raise
        finally:
            try:
                await client.disconnect()
            except Exception:
                logger.warning("Failed to disconnect fresh auth client for %s", phone)
        duration_ms = int((time.monotonic() - started_at) * 1000)
        logger.info("auth.sign_in_fresh success phone=%s duration_ms=%d", phone, duration_ms)
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
