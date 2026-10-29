"""The test-only KDF shortcut must preserve real encryption and key changes."""

from contextlib import contextmanager

import pytest

from tests import conftest
from tg_agent.security import session_cipher

_TEST_SECRET = "test-session-encryption-key"


def _count_derivations(monkeypatch):
    original = session_cipher._derive_fernet_key_v2
    calls = []

    def derive(secret):
        calls.append(secret)
        return original(secret)

    monkeypatch.setattr(session_cipher, "_derive_fernet_key_v2", derive)
    return derive, calls


def test_shared_test_key_is_derived_once_with_real_randomized_encryption(monkeypatch):
    reference = session_cipher.SessionCipher(_TEST_SECRET)
    derive, calls = _count_derivations(monkeypatch)
    # Exercise a fresh fixture lifetime even when the suite's autouse instance
    # is already active. Its patch must also be restored at teardown.
    with contextmanager(conftest._reuse_test_session_key.__wrapped__)():
        first = session_cipher.SessionCipher(_TEST_SECRET)
        second = session_cipher.SessionCipher(_TEST_SECRET)
        token = first.encrypt("session")
        assert token.startswith("enc:v2:")
        assert reference.decrypt(token) == "session"
        assert first.decrypt(reference.encrypt("session")) == "session"
        assert second.decrypt(token) == "session"
        assert second.encrypt("session") != token
        assert calls == [_TEST_SECRET]

        for _ in range(2):
            other = session_cipher.SessionCipher("another-secret")
            with pytest.raises(ValueError, match="invalid encrypted session payload"):
                other.decrypt(token)
        assert calls == [_TEST_SECRET, "another-secret", "another-secret"]

    assert session_cipher._derive_fernet_key_v2 is derive


@pytest.mark.parametrize(
    ("parameter", "value"),
    [("_PBKDF2_SALT", b"different-salt"), ("_PBKDF2_ITERATIONS", 1)],
)
def test_changed_kdf_parameters_bypass_shared_test_key(monkeypatch, parameter, value):
    _, calls = _count_derivations(monkeypatch)
    with contextmanager(conftest._reuse_test_session_key.__wrapped__)():
        token = session_cipher.SessionCipher(_TEST_SECRET).encrypt("session")
        with monkeypatch.context() as changed:
            changed.setattr(session_cipher, parameter, value)
            other = session_cipher.SessionCipher(_TEST_SECRET)
            with pytest.raises(ValueError, match="invalid encrypted session payload"):
                other.decrypt(token)
        assert session_cipher.SessionCipher(_TEST_SECRET).decrypt(token) == "session"
        assert calls == [_TEST_SECRET, _TEST_SECRET]


def test_patched_pbkdf2_still_runs(monkeypatch):
    original = session_cipher.hashlib.pbkdf2_hmac
    calls = []

    def pbkdf2(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    with contextmanager(conftest._reuse_test_session_key.__wrapped__)():
        token = session_cipher.SessionCipher(_TEST_SECRET).encrypt("session")
        monkeypatch.setattr(session_cipher.hashlib, "pbkdf2_hmac", pbkdf2)
        assert session_cipher.SessionCipher(_TEST_SECRET).decrypt(token) == "session"
    assert len(calls) == 1
