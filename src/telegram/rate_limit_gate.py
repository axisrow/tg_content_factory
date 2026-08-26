"""Proactive, per-account Telegram operation rate limiting.

The category values below are deliberately boring guardrails rather than a
claim that Telegram publishes quotas (it does not).  They are calibrated to
the observed production shape: history is a high-volume read path, while
admin and channel-lifecycle calls are sparse writes.  Keep them configurable
so a new production sample can be applied without changing call sites.
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
    # Live username resolution is already protected by ResolveGuardMixin.
    # Cached entity lookups share these operation tags, so generic throttling
    # must stay disabled for the whole transport operation rather than risk a
    # second, conflicting limiter on the live path.
    "telegram_resolve_entity": "resolve",
    "telegram_resolve_input_entity": "resolve",
    "telegram_stream_messages": "history",
    "telegram_edit_admin": "admin_action",
    "telegram_edit_permissions": "admin_action",
    "telegram_kick_participant": "admin_action",
    "telegram_edit_folder": "admin_action",
    "telegram_send_message": "send",
    "telegram_edit_message": "send",
    "telegram_forward_messages": "send",
    "telegram_pin_message": "send",
    # _ensure_reaction_can_run remains the sole reaction gate in Phase 1.
    "telegram_send_reaction": "reaction",
    "telegram_create_channel": "channel_lifecycle",
    "telegram_update_channel_username": "channel_lifecycle",
    "telegram_join_channel": "channel_lifecycle",
    "telegram_import_chat_invite": "channel_lifecycle",
    "telegram_delete_channel": "channel_lifecycle",
}


def _category_for_operation(operation: str) -> str:
    """Return a category for both canonical and decorated operation tags.

    Warm operations are decorated with the caller name (for example
    ``resolve_channel_warm_dialog_cache``) so that flood diagnostics retain
    their useful context.  Matching the stable suffix prevents those paths
    from silently falling back to the broad default bucket.
    """
    exact = _OPERATION_CATEGORIES.get(operation)
    if exact is not None:
        return exact
    if operation.endswith("_warm_dialog_cache") or operation.endswith("_stream_dialogs"):
        return "dialogs"
    return "default"


class TelegramRateLimitGate:
    """Registry of independent sliding-window buckets keyed by phone/category."""

    DEFAULT_SPEC = RateLimitSpec(max_calls=1000, window_sec=60.0)
    # #1330 showed repeated getDialogs floods even with multi-minute pauses.
    # Keep this deliberately low until production logs calibrate the value.
    DIALOGS_SPEC = RateLimitSpec(max_calls=1, window_sec=60.0)
    # Phase 2 calibration.  These are intentionally permissive for normal
    # workloads and should be revisited when a larger production sample is
    # available; they are not Telegram's documented quotas.
    HISTORY_SPEC = RateLimitSpec(max_calls=600, window_sec=60.0)
    ADMIN_ACTION_SPEC = RateLimitSpec(max_calls=10, window_sec=60.0)
    SEND_SPEC = RateLimitSpec(max_calls=30, window_sec=60.0)
    CHANNEL_LIFECYCLE_SPEC = RateLimitSpec(max_calls=3, window_sec=300.0)

    def __init__(
        self,
        *,
        category_limits: dict[str, RateLimitSpec] | None = None,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        specs = {
            "dialogs": self.DIALOGS_SPEC,
            "history": self.HISTORY_SPEC,
            "admin_action": self.ADMIN_ACTION_SPEC,
            "send": self.SEND_SPEC,
            "channel_lifecycle": self.CHANNEL_LIFECYCLE_SPEC,
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
        return _category_for_operation(operation)

    def try_acquire(self, phone: str, category: str, *, slots: int = 1) -> float:
        if category in {"resolve", "reaction"}:
            return 0.0
        return self._limiters.get(category, self._limiters["default"]).try_acquire_many(
            phone, slots
        )

    def reset(self, phone: str | None = None, category: str | None = None) -> None:
        limiters = self._limiters.values() if category is None else [self._limiters[category]]
        for limiter in limiters:
            limiter.reset(phone)
