"""Shared secret-masking policy for settings surfaces (issue #1597).

CLI and web both print settings values; secret-looking keys must never be
shown verbatim — CLI output lands in agent transcripts and logs.
"""

from __future__ import annotations

CREDENTIALS_MASK = "••••••••"

_SECRET_KEY_SUFFIXES = ("_api_key", "_api_hash", "_token", "_secret", "_password")
_SECRET_KEY_NAMES = frozenset({"session_secret_key"})


def is_secret_setting_key(key: str) -> bool:
    """True if a settings key holds a secret and its value must be masked."""
    lowered = key.lower()
    return lowered in _SECRET_KEY_NAMES or lowered.endswith(_SECRET_KEY_SUFFIXES)
