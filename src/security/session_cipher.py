from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

_ENCRYPTED_PREFIX_V1 = "enc:v1:"
_ENCRYPTED_PREFIX_V2 = "enc:v2:"
_PBKDF2_SALT = b"tg_session_key_v2"
_PBKDF2_ITERATIONS = 200_000


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
            raise ValueError("unsupported encrypted session version")

        plaintext = value
        if version == 1:
            plaintext = self.decrypt(value)

        token = self._fernet_v2.encrypt(plaintext.encode("utf-8")).decode("ascii")
        return f"{_ENCRYPTED_PREFIX_V2}{token}"

    def decrypt(self, value: str) -> str:
        version = self.encryption_version(value)
        if version is None:
            if value.startswith("enc:v"):
                raise ValueError("unsupported encrypted session version")
            return value

        if version == 1:
            token = value[len(_ENCRYPTED_PREFIX_V1):]
            fernet = self._fernet_v1
        else:
            token = value[len(_ENCRYPTED_PREFIX_V2):]
            fernet = self._fernet_v2

        try:
            decrypted = fernet.decrypt(token.encode("ascii"))
        except InvalidToken as exc:
            raise ValueError("invalid encrypted session payload") from exc

        return decrypted.decode("utf-8")
