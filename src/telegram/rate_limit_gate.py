"""Proactive, per-account Telegram operation rate limiting.

The limits are intentionally conservative only for ``dialogs``.  The other
categories are registered for future calibration, but use a broad default in
Phase 1 (see issue #1331).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from src.telegram.rate_limiter import ResolveRateLimiter


@dataclass(frozen=True)
class RateLimitSpec:
    max_calls: int
    window_sec: float
    jitter_sec: float = 0.0


class TelegramRateLimitedError(RuntimeError):
    """Raised when an operation is deferred before making a Telegram call."""

    def __init__(self, phone: str, category: str, retry_after_sec: float) -> None:
        super().__init__(f"Telegram {category} rate-limited for {phone}; retry in {retry_after_sec:.1f}s")
        self.phone = phone
        self.category = category
        self.retry_after_sec = retry_after_sec


_OPERATION_CATEGORIES = {
    "telegram_warm_dialog_cache": "dialogs",
    "telegram_stream_dialogs": "dialogs",
    "telegram_stream_messages": "history",
    "telegram_edit_admin": "admin_action",
    "telegram_edit_permissions": "admin_action",
    "telegram_kick_participant": "admin_action",
    "telegram_edit_folder": "admin_action",
    "telegram_send_message": "send",
    "telegram_edit_message": "send",
    "telegram_forward_messages": "send",
    "telegram_pin_message": "send",
    # Reactions retain their dedicated gate for now; do not double throttle.
    "telegram_send_reaction": "resolve",
    "telegram_create_channel": "channel_lifecycle",
    "telegram_join_channel": "channel_lifecycle",
    "telegram_delete_channel": "channel_lifecycle",
}


class TelegramRateLimitGate:
    """Registry of independent sliding-window buckets keyed by phone/category."""

    DEFAULT_SPEC = RateLimitSpec(max_calls=1000, window_sec=60.0)
    # #1330 showed repeated getDialogs floods even with multi-minute pauses.
    # Keep this deliberately low until production logs calibrate the value.
    DIALOGS_SPEC = RateLimitSpec(max_calls=1, window_sec=60.0)

    def __init__(
        self,
        *,
        category_limits: dict[str, RateLimitSpec] | None = None,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        specs = {
            "dialogs": self.DIALOGS_SPEC,
            "history": self.DEFAULT_SPEC,
            "admin_action": self.DEFAULT_SPEC,
            "send": self.DEFAULT_SPEC,
            "channel_lifecycle": self.DEFAULT_SPEC,
            "default": self.DEFAULT_SPEC,
        }
        specs.update(category_limits or {})
        self._limiters = {
            category: ResolveRateLimiter(
                max_calls=spec.max_calls,
                window_sec=spec.window_sec,
                jitter_sec=spec.jitter_sec,
                **({"time_func": time_func} if time_func is not None else {}),
            )
            for category, spec in specs.items()
        }

    @staticmethod
    def category_for(operation: str) -> str:
        # resolve is explicitly a no-op category: ResolveGuardMixin owns it.
        return _OPERATION_CATEGORIES.get(operation, "default")

    def try_acquire(self, phone: str, category: str) -> float:
        if category == "resolve":
            return 0.0
        return self._limiters.get(category, self._limiters["default"]).try_acquire(phone)

    def reset(self, phone: str | None = None, category: str | None = None) -> None:
        limiters = self._limiters.values() if category is None else [self._limiters[category]]
        for limiter in limiters:
            limiter.reset(phone)
