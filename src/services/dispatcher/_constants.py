"""Shared constants for the Telegram command dispatcher and its mixins (#1047).

These live in their own module so both the facade
(``src.services.telegram_command_dispatcher``) and the per-domain mixins can
import them without a circular dependency. The facade re-exports them at module
level so historical patch points like ``mod.REACTION_MIN_INTERVAL_FLOOR_SEC``
keep resolving.
"""

from __future__ import annotations

COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC = 0.1
COMMAND_STATUS_UPDATE_BUSY_RETRY_MAX_SEC = 1.0

# Minimum spacing between reactions on the same phone. Configurable live via the
# DB setting below; a non-zero floor is enforced because Telegram rate-limits
# reactions server-side and zero spacing risks FLOOD_WAIT / account limiting.
REACTION_MIN_INTERVAL_SETTING = "reaction_min_interval_sec"
DEFAULT_REACTION_MIN_INTERVAL_SEC = 30.0
REACTION_MIN_INTERVAL_FLOOR_SEC = 1.0
REACTION_MIN_INTERVAL_CEILING_SEC = 300.0
