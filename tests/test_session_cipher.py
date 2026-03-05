import base64
import hashlib

import pytest
from cryptography.fernet import Fernet

from src.security import SessionCipher


def _encrypt_v1(secret: str, plaintext: str) -> str:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    key = base64.urlsafe_b64encode(digest)
    token = Fernet(key).encrypt(plaintext.encode("utf-8")).decode("ascii")
    return f"enc:v1:{token}"


def test_encrypt_decrypt_roundtrip():
    cipher = SessionCipher("test-secret")
    plaintext = "1BQANOTEuMTA4Ljk2LjE1NwFvvvQk"
    encrypted = cipher.encrypt(plaintext)
    assert encrypted != plaintext
    assert encrypted.startswith("enc:v2:")
    assert cipher.decrypt(encrypted) == plaintext


def test_is_encrypted():
    assert SessionCipher.is_encrypted("enc:v1:something") is True
    assert SessionCipher.is_encrypted("enc:v2:something") is True
    assert SessionCipher.is_encrypted("plaintext_session") is False
    assert SessionCipher.is_encrypted("") is False


def test_encrypt_is_idempotent():
    cipher = SessionCipher("secret")
    plaintext = "session_string"
    encrypted = cipher.encrypt(plaintext)
    assert cipher.encrypt(encrypted) == encrypted


def test_encrypt_v1_reencrypts_to_v2():
    cipher = SessionCipher("secret")
    legacy_value = _encrypt_v1("secret", "session_string")
    migrated_value = cipher.encrypt(legacy_value)
    assert migrated_value.startswith("enc:v2:")
    assert migrated_value != legacy_value
    assert cipher.decrypt(migrated_value) == "session_string"


def test_decrypt_plaintext_returns_as_is():
    cipher = SessionCipher("secret")
    assert cipher.decrypt("plaintext_session") == "plaintext_session"


def test_decrypt_legacy_v1():
    cipher = SessionCipher("secret")
    legacy_value = _encrypt_v1("secret", "my_session")
    assert cipher.decrypt(legacy_value) == "my_session"


def test_decrypt_with_wrong_key_raises():
    cipher1 = SessionCipher("key-one")
    cipher2 = SessionCipher("key-two")
    encrypted = cipher1.encrypt("my_session")
    with pytest.raises(ValueError, match="invalid encrypted session payload"):
        cipher2.decrypt(encrypted)


def test_decrypt_with_unsupported_version_raises():
    cipher = SessionCipher("secret")
    with pytest.raises(ValueError, match="unsupported encrypted session version"):
        cipher.decrypt("enc:v3:unknown")


def test_encrypt_with_unsupported_version_raises():
    cipher = SessionCipher("secret")
    with pytest.raises(ValueError, match="unsupported encrypted session version"):
        cipher.encrypt("enc:v3:unknown")


def test_empty_string():
    cipher = SessionCipher("secret")
    encrypted = cipher.encrypt("")
    assert encrypted.startswith("enc:v2:")
    assert cipher.decrypt(encrypted) == ""
