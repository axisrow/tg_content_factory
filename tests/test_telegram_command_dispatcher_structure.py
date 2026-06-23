"""Characterization tests for the TelegramCommandDispatcher domain split (#1047).

These pin the *structure* contract that the 1162-line monolith was broken into
per-domain mixin modules **without changing behaviour**:

- the public class and error type still import from the historical module path;
- every command type the queue can carry still resolves to a callable handler
  via the unchanged ``getattr``-based dispatch;
- the per-domain mixins are real, independent classes in the new subpackage,
  and the dispatcher inherits all of them;
- the module-level symbols that the existing suite patches stay re-exported on
  the facade module so 150 patch points keep working.

Together with the existing behavioural suite (test_telegram_command_dispatcher.py)
this is the red→green anchor for the refactor: the behaviour suite proves the
handlers still *do* the same thing, this file proves they were actually moved.
"""

from __future__ import annotations

import inspect

import src.services.telegram_command_dispatcher as mod
from src.services.telegram_command_dispatcher import (
    DEFAULT_REACTION_MIN_INTERVAL_SEC,
    REACTION_MIN_INTERVAL_SETTING,
    TelegramCommandDispatcher,
    TelegramCommandRetryLaterError,
)

# Every command_type the dispatcher must still route. Derived from the queue
# contract (command_type -> _handle_<command_type with dots as underscores>).
EXPECTED_COMMAND_TYPES = [
    "auth.send_code",
    "auth.resend_code",
    "auth.verify_code",
    "scheduler.reconcile",
    "scheduler.trigger_warm",
    "collection.pause",
    "collection.resume",
    "dialogs.refresh",
    "dialogs.cache_clear",
    "dialogs.leave",
    "dialogs.send",
    "dialogs.join",
    "dialogs.resolve",
    "dialogs.edit_message",
    "dialogs.delete_message",
    "dialogs.forward_messages",
    "dialogs.pin_message",
    "dialogs.react",
    "dialogs.unpin_message",
    "dialogs.participants",
    "dialogs.broadcast_stats",
    "dialogs.archive",
    "dialogs.unarchive",
    "dialogs.mark_read",
    "dialogs.edit_admin",
    "dialogs.edit_permissions",
    "dialogs.kick",
    "dialogs.create_channel",
    "dialogs.download_media",
    "search.telegram",
    "agent.forum_topics_refresh",
    "channels.add_identifier",
    "channels.collect_stats",
    "channels.refresh_types",
    "channels.refresh_meta",
    "channels.import_batch",
    "accounts.connect",
    "accounts.toggle",
    "accounts.delete",
    "notifications.setup_bot",
    "notifications.delete_bot",
    "notifications.invalidate_cache",
    "notifications.test",
    "photo.send_now",
    "photo.schedule_send",
    "photo.run_due",
    "moderation.publish_run",
]


def test_public_api_imports_from_historical_module_path():
    """External consumers (web/bootstrap, cli/settings) import these names from
    src.services.telegram_command_dispatcher — the split must not move them."""
    assert TelegramCommandDispatcher.__name__ == "TelegramCommandDispatcher"
    assert issubclass(TelegramCommandRetryLaterError, RuntimeError)
    assert isinstance(DEFAULT_REACTION_MIN_INTERVAL_SEC, float)
    assert REACTION_MIN_INTERVAL_SETTING == "reaction_min_interval_sec"


def test_every_expected_command_type_resolves_to_a_handler():
    """The unchanged ``getattr`` dispatch must find a callable for every command
    type the queue can carry, regardless of which mixin now owns it."""
    for command_type in EXPECTED_COMMAND_TYPES:
        handler_name = f"_handle_{command_type.replace('.', '_')}"
        handler = getattr(TelegramCommandDispatcher, handler_name, None)
        assert callable(handler), f"missing handler for {command_type!r}: {handler_name}"


def test_dispatcher_is_composed_of_per_domain_mixins():
    """The monolith was split into independent per-domain mixin classes; the
    dispatcher must inherit all of them (so MRO keeps every handler on self)."""
    from src.services.dispatcher.accounts_mixin import AccountsCommandsMixin
    from src.services.dispatcher.auth_mixin import AuthCommandsMixin
    from src.services.dispatcher.channels_mixin import ChannelsCommandsMixin
    from src.services.dispatcher.dialogs_mixin import DialogsCommandsMixin
    from src.services.dispatcher.moderation_mixin import ModerationCommandsMixin
    from src.services.dispatcher.notifications_mixin import NotificationsCommandsMixin
    from src.services.dispatcher.photo_mixin import PhotoCommandsMixin
    from src.services.dispatcher.scheduler_mixin import SchedulerCommandsMixin
    from src.services.dispatcher.search_mixin import SearchCommandsMixin

    mixins = (
        AuthCommandsMixin,
        AccountsCommandsMixin,
        SchedulerCommandsMixin,
        DialogsCommandsMixin,
        ChannelsCommandsMixin,
        NotificationsCommandsMixin,
        PhotoCommandsMixin,
        SearchCommandsMixin,
        ModerationCommandsMixin,
    )
    for mixin in mixins:
        assert issubclass(TelegramCommandDispatcher, mixin), f"not a base: {mixin.__name__}"


def test_patch_sensitive_symbols_stay_on_facade_module():
    """150 existing tests patch these via the facade module namespace; they must
    remain bound there after the split or the patches silently no-op."""
    for name in ("TelegramActionService", "Notifier", "NotificationTargetService", "asyncio"):
        assert hasattr(mod, name), f"facade no longer re-exports {name!r}"


def test_patch_sensitive_handlers_are_defined_on_the_facade_class_body():
    """These handlers resolve a symbol patched via ``mod`` (TelegramActionService,
    Notifier, NotificationTargetService) or read ``mod.__file__`` — they must be
    defined in the facade class' own module so the patch reaches the call site."""
    facade_module = TelegramCommandDispatcher.__module__
    for handler_name in (
        "_handle_dialogs_join",
        "_handle_dialogs_download_media",
        "_handle_notifications_test",
        "_notification_target_service",
        "_run_loop",
        "_update_command_safely",
    ):
        method = getattr(TelegramCommandDispatcher, handler_name)
        owner_module = inspect.unwrap(method).__module__
        assert owner_module == facade_module, (
            f"{handler_name} must stay in {facade_module} (patch-sensitive), "
            f"found in {owner_module}"
        )
