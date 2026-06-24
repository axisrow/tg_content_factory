"""Per-domain command handlers for :class:`TelegramCommandDispatcher` (#1047).

The dispatcher used to be a single 1162-line module. Handlers are now grouped
by command domain into mixin classes here; the facade in
``src.services.telegram_command_dispatcher`` composes them and keeps the loop
machinery, the public class, and every patch-sensitive symbol in one place.
"""

from __future__ import annotations
