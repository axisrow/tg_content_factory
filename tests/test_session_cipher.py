import pytest

from src.security import SessionCipher


def test_encrypt_decrypt_roundtrip():
    cipher = SessionCipher("test-secret")
    plaintext = "1BQANOTEuMTA4Ljk2LjE1NwFvvvQk"
    encrypted = cipher.encrypt(plaintext)
    assert encrypted != plaintext
    assert encrypted.startswith("enc:v1:")
    assert cipher.decrypt(encrypted) == plaintext


def test_is_encrypted():
    assert SessionCipher.is_encrypted("enc:v1:something") is True
    assert SessionCipher.is_encrypted("plaintext_session") is False
    assert SessionCipher.is_encrypted("") is False


def test_encrypt_is_idempotent():
    cipher = SessionCipher("secret")
    plaintext = "session_string"
    encrypted = cipher.encrypt(plaintext)
    assert cipher.encrypt(encrypted) == encrypted


def test_decrypt_plaintext_returns_as_is():
    cipher = SessionCipher("secret")
    assert cipher.decrypt("plaintext_session") == "plaintext_session"


def test_decrypt_with_wrong_key_raises():
    cipher1 = SessionCipher("key-one")
    cipher2 = SessionCipher("key-two")
    encrypted = cipher1.encrypt("my_session")
    with pytest.raises(ValueError, match="invalid encrypted session payload"):
        cipher2.decrypt(encrypted)


def test_empty_string():
    cipher = SessionCipher("secret")
    encrypted = cipher.encrypt("")
    assert encrypted.startswith("enc:v1:")
    assert cipher.decrypt(encrypted) == ""
