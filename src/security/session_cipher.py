from __future__ import annotations

import base64
import hashlib
import logging

from cryptography.fernet import Fernet, InvalidToken

_ENCRYPTED_PREFIX_V1 = "enc:v1:"
_ENCRYPTED_PREFIX_V2 = "enc:v2:"
_PBKDF2_SALT = b"tg_session_key_v2"
_PBKDF2_ITERATIONS = 200_000


class EncryptedPayloadError(ValueError):
    def __init__(self, message: str, *, status: str):
        super().__init__(message)
        self.status = status


def decrypt_failure_status(exc: BaseException) -> str:
    if isinstance(exc, EncryptedPayloadError):
        return exc.status
    message = str(exc)
    if "SESSION_ENCRYPTION_KEY" in message:
        return "missing_key"
    if "unsupported encrypted session version" in message:
        return "unsupported_version"
    if "invalid encrypted session payload" in message:
        return "key_mismatch"
    return "decrypt_failed"


def log_expected_decrypt_failure(
    logger: logging.Logger,
    *,
    resource: str,
    identifier: str,
    status: str,
    action: str,
    level: int = logging.ERROR,
) -> None:
    logger.log(
        level,
        "decrypt failed: resource=%s identifier=%s status=%s action=%s",
        resource,
        identifier,
        status,
        action,
    )


def _derive_fernet_key_v1(secret: str) -> bytes:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _derive_fernet_key_v2(secret: str) -> bytes:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode("utf-8"),
        _PBKDF2_SALT,
        _PBKDF2_ITERATIONS,
        dklen=32,
    )
    return base64.urlsafe_b64encode(digest)


class SessionCipher:
    def __init__(self, secret: str):
        self._fernet_v1 = Fernet(_derive_fernet_key_v1(secret))
        self._fernet_v2 = Fernet(_derive_fernet_key_v2(secret))

    @staticmethod
    def is_encrypted(value: str) -> bool:
        return SessionCipher.encryption_version(value) is not None

    @staticmethod
    def encryption_version(value: str) -> int | None:
        if value.startswith(_ENCRYPTED_PREFIX_V1):
            return 1
        if value.startswith(_ENCRYPTED_PREFIX_V2):
            return 2
        return None

    def encrypt(self, value: str) -> str:
        version = self.encryption_version(value)
        if version == 2:
            return value
        if version is None and value.startswith("enc:v"):
            raise EncryptedPayloadError(
                "unsupported encrypted session version",
                status="unsupported_version",
            )

        plaintext = value
        if version == 1:
            plaintext = self.decrypt(value)

        token = self._fernet_v2.encrypt(plaintext.encode("utf-8")).decode("ascii")
        return f"{_ENCRYPTED_PREFIX_V2}{token}"

    def decrypt(self, value: str) -> str:
        version = self.encryption_version(value)
        if version is None:
            if value.startswith("enc:v"):
                raise EncryptedPayloadError(
                    "unsupported encrypted session version",
                    status="unsupported_version",
                )
            return value

        if version == 1:
            token = value[len(_ENCRYPTED_PREFIX_V1) :]
            fernet = self._fernet_v1
        else:
            token = value[len(_ENCRYPTED_PREFIX_V2) :]
            fernet = self._fernet_v2

        try:
            decrypted = fernet.decrypt(token.encode("ascii"))
        except InvalidToken as exc:
            raise EncryptedPayloadError(
                "invalid encrypted session payload",
                status="key_mismatch",
            ) from exc

        return decrypted.decode("utf-8")
