"""`_HANDOFF_COMMANDS` must forward every field the worker handler actually reads.

Regression (Codex, PR #1324 round 3): three prior fix passes (round 1: edit-admin,
round 2: edit-permissions/mark-read/phone-resolve, round 3: this file) each closed
one instance of the same class of bug — a `_HANDOFF_COMMANDS` field tuple built by
copying argparse attribute names without cross-checking (a) the real Typer
parameter types and (b) every branch the worker's dispatcher handler reads. This
module programmatically audits the whole table instead of relying on another
reactive review round to catch the next instance.
"""

from __future__ import annotations

import inspect

import pytest

from src.cli.commands import dialogs as dialogs_cli
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher

# command_type -> the set of payload keys the worker handler actually reads via
# payload[...]/payload.get(...). Maintained by hand (there is no way to derive
# this from static analysis alone), audited against dialogs_mixin.py /
# telegram_command_dispatcher.py source as of PR #1324 round 3.
#
# `phone` is intentionally excluded everywhere — `_handoff_dialog_action` always
# sets it separately from `_HANDOFF_COMMANDS`' field tuple.
_WORKER_READS: dict[str, frozenset[str]] = {
    "dialogs.send": frozenset({"recipient", "text"}),
    "dialogs.join": frozenset({"target"}),
    "dialogs.resolve": frozenset({"identifier"}),
    "dialogs.edit_message": frozenset({"chat_id", "message_id", "text"}),
    "dialogs.delete_message": frozenset({"chat_id", "message_ids"}),
    "dialogs.forward_messages": frozenset({"from_chat", "to_chat", "message_ids"}),
    "dialogs.pin_message": frozenset({"chat_id", "message_id", "notify"}),
    "dialogs.unpin_message": frozenset({"chat_id", "message_id"}),
    "dialogs.react": frozenset({"chat_id", "message_id", "emoji", "clear"}),
    "dialogs.participants": frozenset({"chat_id", "limit", "search"}),
    "dialogs.edit_admin": frozenset({"chat_id", "user_id", "is_admin", "title"}),
    "dialogs.edit_permissions": frozenset(
        {"chat_id", "user_id", "until_date", "send_messages", "send_media"}
    ),
    "dialogs.kick": frozenset({"chat_id", "user_id"}),
    "dialogs.broadcast_stats": frozenset({"chat_id"}),
    "dialogs.archive": frozenset({"chat_id"}),
    "dialogs.unarchive": frozenset({"chat_id"}),
    "dialogs.mark_read": frozenset({"chat_id", "max_id"}),
    "dialogs.refresh": frozenset(),
    "dialogs.cache_clear": frozenset(),
}


def test_handoff_table_covers_every_field_the_worker_reads():
    """Every key `_WORKER_READS` lists for a command must be in its handoff tuple.

    A missing field here means the worker either KeyErrors (a required field) or
    silently falls back to a default that diverges from the in-process path (an
    optional field, e.g. mark-read's --max-id, PR #1324 round 2).
    """
    missing: dict[str, frozenset[str]] = {}
    for action, (command_type, fields) in dialogs_cli._HANDOFF_COMMANDS.items():
        expected = _WORKER_READS.get(command_type)
        if expected is None:
            continue
        gap = expected - set(fields)
        if gap:
            missing[action] = gap

    assert not missing, (
        f"_HANDOFF_COMMANDS is missing fields the worker handler reads: {missing}. "
        "Add them to the tuple in src/cli/commands/dialogs.py."
    )


def test_download_media_is_not_handed_off():
    """download-media must stay in-process: --output-dir names a local path.

    Regression (Codex, PR #1324 round 3 audit): the worker (possibly a
    different machine in a split deployment) always saves to its own
    data/downloads/ — the same contract the web UI already uses (it never
    sends output_dir either). If `download-media` is re-added to
    `_HANDOFF_COMMANDS`, `output_dir` must be included and the dispatcher
    handler must actually honor it (or the CLI must warn that a custom
    --output-dir is ignored while a worker is running).
    """
    assert "download-media" not in dialogs_cli._HANDOFF_COMMANDS


def test_handoff_table_has_no_stale_command_types():
    """Every `_HANDOFF_COMMANDS` command_type must have a `_WORKER_READS` entry.

    Catches a command_type typo or a worker handler rename that would otherwise
    make the audit above silently pass on nothing.
    """
    known = set(_WORKER_READS)
    used = {command_type for command_type, _fields in dialogs_cli._HANDOFF_COMMANDS.values()}
    stale = used - known

    assert not stale, (
        f"_HANDOFF_COMMANDS references command_type(s) not audited here: {sorted(stale)}. "
        "Add them to _WORKER_READS in this file (or remove them if dead)."
    )


@pytest.mark.parametrize("action", sorted(dialogs_cli._HANDOFF_COMMANDS))
def test_handoff_fields_are_real_typer_or_namespace_parameters(action: str):
    """Every field name in the handoff tuple must exist on the Namespace it reads.

    `_handoff_dialog_action` does `getattr(args, field, None)` — a typo in a
    field name silently drops that field into the "missing, treated as None"
    bucket instead of failing loudly. Cross-check against the corresponding
    Typer command's `_run_dialogs(...)` call, which is the only place that
    builds the real Namespace for this action.
    """
    _command_type, fields = dialogs_cli._HANDOFF_COMMANDS[action]
    source = inspect.getsource(dialogs_cli)
    # Find the `_run_dialogs(ctx, "<action>", ...)` call for this action and
    # extract its keyword argument names — those are exactly the Namespace
    # attributes available to `_handoff_dialog_action` for this action.
    marker = f'_run_dialogs(ctx, "{action}"'
    marker_multiline = f'_run_dialogs(\n        ctx, "{action}"'
    start = source.find(marker)
    if start == -1:
        start = source.find(marker_multiline)
    assert start != -1, f"Could not locate the _run_dialogs(...) call for action={action!r}"
    call_text = source[start : start + 400].split(")\n", 1)[0]
    for field in fields:
        assert f"{field}=" in call_text, (
            f"_HANDOFF_COMMANDS[{action!r}] declares field {field!r}, but the Typer "
            f"command's _run_dialogs(...) call does not pass it as a keyword — the "
            f"Namespace will never carry it, so the handoff payload silently drops it."
        )


def test_worker_reads_registry_matches_dispatcher_handlers():
    """Every `_WORKER_READS` command_type must resolve to a real dispatcher method.

    Guards the audit registry itself: a stale/renamed handler would let the
    field-coverage test above pass vacuously.
    """
    missing_handlers = []
    for command_type in _WORKER_READS:
        handler_name = "_handle_" + command_type.replace(".", "_")
        if not hasattr(TelegramCommandDispatcher, handler_name):
            missing_handlers.append((command_type, handler_name))

    assert not missing_handlers, (
        f"_WORKER_READS references command_type(s) with no matching dispatcher "
        f"handler: {missing_handlers}"
    )
