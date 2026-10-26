from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar
from uuid import uuid4

from telethon import TelegramClient
from telethon.errors import AuthKeyDuplicatedError, PasswordHashInvalidError, SessionPasswordNeededError
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
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception_type, stop_after_attempt
from tenacity.wait import wait_exponential

logger = logging.getLogger(__name__)

AUTH_CONNECT_TIMEOUT_SECONDS = 30
AUTH_RPC_TIMEOUT_SECONDS = 60

# Bounded connect retries for AuthKeyDuplicatedError (#1548, ported from
# telegram-mcp runner.py): a transient duplicate-key report (session briefly
# seen from another IP) should not kill the caller, but retries stay bounded —
# a truly duplicated auth key never recovers by waiting.
AUTH_KEY_RETRY_ATTEMPTS = 4
AUTH_KEY_RETRY_INITIAL_DELAY_SECONDS = 2.0
AUTH_KEY_RETRY_MAX_DELAY_SECONDS = 15.0

# How many times an unscanned QR token is regenerated (recreate) before the
# flow gives up (#1551). Mirrors the interactive reference loop.
QR_MAX_REFRESHES = 10

_T = TypeVar("_T")


def validate_session_string(session_string: str) -> bool:
    """Return True iff ``session_string`` is a structurally usable Telegram StringSession.

    A usable session must parse AND carry the four fields a live client needs:
    ``auth_key``, ``server_address``, ``port`` and ``dc_id`` (mirrors the check in
    :meth:`SessionMaterializer.materialize`). This lives in the telegram layer on
    purpose: the CLI / web / agent entrypoints are forbidden from importing
    ``telethon`` directly (import-linter), so they call this helper instead.

    The value is a secret (full account access) — never log it here.
    """
    if not session_string:
        return False
    try:
        source = StringSession(session_string)
    except Exception:  # noqa: BLE001 - malformed input must never raise, just fail validation
        return False
    return bool(source.auth_key and source.server_address and source.port and source.dc_id)


class TelegramAuthTimeoutError(TimeoutError):
    """Raised when a Telegram auth operation exceeds its explicit timeout."""


class TwoFactorRequiredError(ValueError):
    """Raised when sign-in needs a 2FA password that was not supplied.

    Subclasses ``ValueError`` and keeps the ``"2FA password required"`` message
    so existing callers that branch on ``isinstance(exc, ValueError)`` or on the
    ``"2FA"``/``"password"`` substring keep working. It exists so the auth flow
    can log this *expected* outcome at info level instead of as an ERROR
    stack trace (#633 bug #27).
    """

    def __init__(self, message: str = "2FA password required") -> None:
        super().__init__(message)


def _format_timeout(seconds: float) -> str:
    if float(seconds).is_integer():
        return f"{int(seconds)}s"
    return f"{seconds:g}s"


@dataclass
class _QrSession:
    """Stateful QR-login flow (#1551). One active flow per TelegramAuth.

    ``task`` holds the background wait/recreate loop so the command queue is
    never blocked by a pending scan; ``session_string`` is a secret and only
    ever leaves this class through :meth:`TelegramAuth.consume_qr_session`.
    """

    qr_id: str
    client: TelegramClient
    qrlogin: Any
    state: str  # pending | password_needed | success | consumed | failed
    url: str
    expires_at: datetime | None
    task: asyncio.Task | None = None
    phone: str | None = None
    session_string: str | None = None
    error: str | None = None
    refreshes: int = 0

    def snapshot(self, *, include_session_string: bool = False) -> dict:
        snap = {
            "qr_id": self.qr_id,
            "state": self.state,
            "url": self.url,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "phone": self.phone,
            "error": self.error,
            "refreshes": self.refreshes,
        }
        if include_session_string:
            snap["session_string"] = self.session_string
        return snap


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


def _log_auth_key_retry(phone: str, max_attempts: int) -> Callable[[RetryCallState], None]:
    def _log(retry_state: RetryCallState) -> None:
        logger.warning(
            "AuthKeyDuplicatedError connecting %s (attempt %d/%d): session in use "
            "elsewhere; retrying with backoff",
            phone or "session",
            retry_state.attempt_number,
            max_attempts,
        )

    return _log


async def connect_with_auth_key_retry(
    client: TelegramClient,
    *,
    phone: str = "",
    max_attempts: int = AUTH_KEY_RETRY_ATTEMPTS,
    initial_delay: float = AUTH_KEY_RETRY_INITIAL_DELAY_SECONDS,
    max_delay: float = AUTH_KEY_RETRY_MAX_DELAY_SECONDS,
) -> None:
    """Connect a client, tolerating transient AuthKeyDuplicatedError (#1548).

    Up to ``max_attempts`` attempts with exponential backoff (``initial_delay``,
    2×, … capped at ``max_delay`` — upstream telegram-mcp ladder); the
    half-open transport is disconnected between attempts. Any other error and
    exhausted attempts re-raise immediately; ``reraise=True`` keeps the original
    AuthKeyDuplicatedError instead of tenacity's RetryError.
    """

    async def _connect_once() -> None:
        try:
            await client.connect()
        except AuthKeyDuplicatedError:
            try:
                await client.disconnect()
            except Exception:
                logger.debug(
                    "Failed to disconnect %s after AuthKeyDuplicatedError", phone, exc_info=True
                )
            raise

    retryer = AsyncRetrying(
        retry=retry_if_exception_type(AuthKeyDuplicatedError),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=initial_delay, max=max_delay),
        sleep=asyncio.sleep,
        before_sleep=_log_auth_key_retry(phone, max_attempts),
        reraise=True,
    )
    await retryer(_connect_once)


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
    def __init__(self, api_id: int, api_hash: str, proxy: dict | None = None):
        self._api_id = api_id
        self._api_hash = api_hash
        # Network-level MTProto proxy (Telethon ``proxy=`` dict, see
        # parse_proxy_url) applied to every client this factory builds;
        # None = direct connection.
        self._proxy = proxy
        self._pending: dict[str, tuple[TelegramClient, str]] = {}
        self._qr: _QrSession | None = None

    @property
    def api_id(self) -> int:
        return self._api_id

    @property
    def api_hash(self) -> str:
        return self._api_hash

    @property
    def proxy(self) -> dict | None:
        return self._proxy

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
        client = TelegramClient(StringSession(), self._api_id, self._api_hash, proxy=self._proxy)
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
            except SessionPasswordNeededError as exc:
                if not password_2fa:
                    raise TwoFactorRequiredError() from exc
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
            # sign_in succeeded — Telegram now considers this client authorized.
            # session.save() is the ONLY way to capture that session for reuse, so
            # if it raises we must NOT tear down the pending client (#1029): doing
            # so would strand an authorized session with no string to persist,
            # forcing a fresh send-code/verify onboarding. Keep the pending entry
            # so the operator can retry verify_code and recover the session.
            session_string = client.session.save()
        except TwoFactorRequiredError:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.info("auth.verify_code needs 2FA password phone=%s duration_ms=%d", phone, duration_ms)
            raise
        except Exception:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.exception("auth.verify_code error phone=%s duration_ms=%d", phone, duration_ms)
            raise

        # Only now that the session string is captured is it safe to drop the
        # pending client and disconnect — a failure before this point leaves the
        # pending entry intact for a retry (#1029).
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
        client = TelegramClient(
            StringSession(session_str), self._api_id, self._api_hash, proxy=self._proxy
        )
        try:
            logger.info(
                "auth.sign_in_fresh connect start phone=%s timeout_s=%s",
                phone,
                AUTH_CONNECT_TIMEOUT_SECONDS,
            )
            await _with_auth_timeout(client.connect(), "connect", AUTH_CONNECT_TIMEOUT_SECONDS)
            if code_consumed:
                if not password_2fa:
                    raise TwoFactorRequiredError()
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
                except SessionPasswordNeededError as exc:
                    if not password_2fa:
                        raise TwoFactorRequiredError() from exc
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
        except TwoFactorRequiredError:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.info("auth.sign_in_fresh needs 2FA password phone=%s duration_ms=%d", phone, duration_ms)
            raise
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

    async def create_client_from_session(
        self,
        session_string: str,
        *,
        base_logger: logging.Logger | None = None,
    ) -> TelegramClient:
        """Create and connect a client from saved session string.

        ``base_logger`` routes the client's Telethon loggers under a per-phone
        name so the MTProto security watchdog can attribute warnings (#556).
        """
        extra_kwargs = {"base_logger": base_logger} if base_logger is not None else {}
        client = TelegramClient(
            StringSession(session_string), self._api_id, self._api_hash,
            connection_retries=None, retry_delay=2,
            proxy=self._proxy,
            **extra_kwargs,
        )
        await connect_with_auth_key_retry(client)
        if not await client.is_user_authorized():
            raise ConnectionError("Session is no longer valid")
        return client

    async def cleanup(self) -> None:
        """Disconnect any pending clients."""
        for _phone, (client, _) in self._pending.items():
            try:
                await client.disconnect()
            except Exception:
                pass
        self._pending.clear()
        await self._teardown_qr()

    # --- QR login (#1551) -------------------------------------------------
    # Mechanics ported from telegram-mcp's session_string_generator (--qr):
    # client.qr_login() → poll wait() → recreate() on token expiry → 2FA via
    # sign_in(password) → StringSession.save(). Rewritten for the command
    # queue: the wait loop lives in a background task so auth.qr_start /
    # auth.qr_status stay fast and the dispatcher loop is never blocked.

    async def qr_start(self) -> dict:
        """Start a QR login flow. Returns {qr_id, url, expires_at} for rendering."""
        await self._teardown_qr()
        client = TelegramClient(StringSession(), self._api_id, self._api_hash, proxy=self._proxy)
        try:
            await _with_auth_timeout(client.connect(), "connect", AUTH_CONNECT_TIMEOUT_SECONDS)
            qrlogin = await _with_auth_timeout(client.qr_login(), "qr_login", AUTH_RPC_TIMEOUT_SECONDS)
        except Exception:
            try:
                await client.disconnect()
            except Exception:
                logger.warning("Failed to disconnect QR auth client after start failure")
            raise
        session = _QrSession(
            qr_id=uuid4().hex[:12],
            client=client,
            qrlogin=qrlogin,
            state="pending",
            url=qrlogin.url,
            expires_at=qrlogin.expires,
        )
        session.task = asyncio.create_task(
            self._qr_wait_loop(session), name=f"qr_login_wait_{session.qr_id}"
        )
        self._qr = session
        logger.info("auth.qr_start started qr_id=%s", session.qr_id)
        return {"qr_id": session.qr_id, "url": session.url, "expires_at": session.snapshot()["expires_at"]}

    def qr_status(self, qr_id: str) -> dict | None:
        """Snapshot of the QR flow. None when ``qr_id`` is not the active flow."""
        session = self._qr
        if session is None or session.qr_id != qr_id:
            return None
        return session.snapshot()

    def qr_fail(self, qr_id: str, error: str) -> None:
        """Mark a consumed-but-unmaterialized flow failed (#1555 review).

        After the one-shot consume a failed materialization would otherwise
        leave the flow in ``consumed`` — later polls then report success and
        the web page redirects to /settings with no account created. Only a
        ``consumed`` flow is flipped; terminal/active states are untouched.
        """
        session = self._qr
        if session is not None and session.qr_id == qr_id and session.state == "consumed":
            session.state = "failed"
            session.error = error

    async def consume_qr_session(self, qr_id: str) -> dict | None:
        """Atomically flip a successful flow to ``consumed`` and return its secrets.

        The one-shot handoff between the QR wait loop and account
        materialization: a second caller (duplicate poll) gets None instead of
        creating a duplicate account. Disconnects the login client — the
        session is captured in the returned ``session_string``.
        """
        session = self._qr
        if session is None or session.qr_id != qr_id or session.state != "success":
            return None
        session.state = "consumed"
        snapshot = session.snapshot(include_session_string=True)
        try:
            await session.client.disconnect()
        except Exception:
            logger.warning("Failed to disconnect consumed QR auth client")
        return snapshot

    async def qr_password(self, qr_id: str, password: str) -> dict:
        """Submit the 2FA password for a ``password_needed`` QR flow.

        Returns {"state": "success"} on completion, or
        {"state": "password_needed", "error": "invalid_password"} so the caller
        can retry. Other errors raise.
        """
        session = self._qr
        if session is None or session.qr_id != qr_id:
            raise ValueError("qr_session_not_found")
        if session.state != "password_needed":
            raise ValueError(f"qr_state_not_password_needed:{session.state}")
        if not password:
            session.error = "password_required"
            return {"state": "password_needed", "error": "password_required"}
        try:
            await _with_auth_timeout(
                session.client.sign_in(password=password), "sign_in", AUTH_RPC_TIMEOUT_SECONDS
            )
        except PasswordHashInvalidError:
            session.error = "invalid_password"
            return {"state": "password_needed", "error": "invalid_password"}
        session.state = "success"
        session.error = None
        try:
            session.session_string = session.client.session.save()
            me = await session.client.get_me()
            session.phone = getattr(me, "phone", None) or None
        except Exception:
            logger.exception("auth.qr_password failed to capture session string")
            session.state = "failed"
            session.error = "qr_session_save_failed"
            await self._disconnect_qr_client(session)
            raise
        await self._disconnect_qr_client(session)
        return {"state": "success"}

    async def _disconnect_qr_client(self, session: _QrSession) -> None:
        try:
            await session.client.disconnect()
        except Exception:
            logger.warning("Failed to disconnect QR auth client")

    async def _teardown_qr(self) -> None:
        """Cancel the background wait loop and drop the active flow, if any."""
        session = self._qr
        self._qr = None
        if session is None:
            return
        if session.task is not None and not session.task.done():
            session.task.cancel()
            try:
                await session.task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.warning("QR wait loop raised during teardown", exc_info=True)
        await self._disconnect_qr_client(session)

    async def _qr_wait_loop(self, session: _QrSession) -> None:
        """Background wait/recreate loop for one QR token.

        wait() must be running while the QR is on screen — Telethon completes
        the login through its UpdateLoginToken handler. Token expiry (~30s)
        refreshes the URL in place; the web side picks the fresh URL up via
        auth.qr_status polling.
        """
        try:
            while True:
                try:
                    user = await session.qrlogin.wait()
                except TimeoutError:  # asyncio.TimeoutError is an alias on py311+
                    if session.refreshes >= QR_MAX_REFRESHES:
                        session.state = "failed"
                        session.error = "qr_expired_max_refreshes"
                        break
                    session.refreshes += 1
                    await session.qrlogin.recreate()
                    session.url = session.qrlogin.url
                    session.expires_at = session.qrlogin.expires
                    continue
                except SessionPasswordNeededError:
                    session.state = "password_needed"
                    break
                session.state = "success"
                try:
                    session.session_string = session.client.session.save()
                    session.phone = getattr(user, "phone", None) or None
                except Exception:
                    logger.exception("auth.qr_login failed to save authorized session")
                    session.state = "failed"
                    session.error = "qr_session_save_failed"
                    raise
                break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            session.state = "failed"
            session.error = str(exc)
        finally:
            if session.state == "failed":
                await self._disconnect_qr_client(session)
            logger.info(
                "auth.qr_login loop finished qr_id=%s state=%s refreshes=%d",
                session.qr_id,
                session.state,
                session.refreshes,
            )
