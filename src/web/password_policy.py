"""Password checks used before exposing the panel beyond localhost."""
from __future__ import annotations

import math
import re

_COMMON_PASSWORDS = {
    "123456", "12345678", "123456789", "password", "password123", "qwerty",
    "qwerty123", "admin", "admin123", "letmein", "welcome", "changeme",
}
# Enough to reject short/common guesses while retaining compatibility with
# existing deployments such as the eight-character ``testpass`` test secret.
_MIN_ENTROPY_BITS = 32.0


def estimated_entropy_bits(password: str) -> float:
    """Estimate entropy from charset size, penalizing repeated/predictable shapes."""
    if not password:
        return 0.0
    patterns = (r"[a-z]", r"[A-Z]", r"[0-9]", r"[^A-Za-z0-9]")
    alphabet = sum((26, 26, 10, 33)[i] for i, pattern in enumerate(patterns) if re.search(pattern, password))
    bits = len(password) * math.log2(max(alphabet, 1))
    if len(set(password)) <= 2 or re.fullmatch(r"(.)\1+|(.{1,4})\2+", password):
        return 0.0
    if re.search(r"(?i)(0123456789|9876543210|abcdefghijklmnopqrstuvwxyz|zyxwvutsrqponmlkjihgfedcba)", password):
        return min(bits, 10.0)
    return bits


def is_strong_password(password: str) -> bool:
    if password.casefold() in _COMMON_PASSWORDS or password.isspace():
        return False
    return estimated_entropy_bits(password) >= _MIN_ENTROPY_BITS
