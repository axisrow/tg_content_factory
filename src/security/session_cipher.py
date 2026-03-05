from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

_ENCRYPTED_PREFIX = "enc:v1:"


def _derive_fernet_key(secret: str) -> bytes:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


class SessionCipher:
    def __init__(self, secret: str):
        self._fernet = Fernet(_derive_fernet_key(secret))

    @staticmethod
    def is_encrypted(value: str) -> bool:
        return value.startswith(_ENCRYPTED_PREFIX)

    def encrypt(self, value: str) -> str:
        if self.is_encrypted(value):
            return value
        token = self._fernet.encrypt(value.encode("utf-8")).decode("ascii")
        return f"{_ENCRYPTED_PREFIX}{token}"

    def decrypt(self, value: str) -> str:
        if not self.is_encrypted(value):
            return value

        token = value[len(_ENCRYPTED_PREFIX):]
        try:
            decrypted = self._fernet.decrypt(token.encode("ascii"))
        except InvalidToken as exc:
            raise ValueError("invalid encrypted session payload") from exc

        return decrypted.decode("utf-8")
