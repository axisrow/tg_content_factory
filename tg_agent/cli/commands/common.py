from __future__ import annotations

import signal
import sys
from collections.abc import Coroutine
from enum import Enum
from typing import Any, NoReturn, TypeVar

import typer

from tg_agent.cli.typer_app import apply_startup as _apply_startup
from tg_agent.cli.typer_app import run_async as _run_async
from tg_agent.utils.datetime import parse_date_bound, parse_datetime_bound

_T = TypeVar("_T")

#: Context settings for the commands whose first positional can be a *negative*
#: identifier — Telegram channel / chat ids (``-100123…``), ``@username`` /
#: dialog identifiers, search queries, etc. argparse accepted ``search -100500`` /
#: ``dialogs archive -100… --yes`` directly because no option in this CLI is a
#: negative number, so a ``-N`` token was unambiguously a value. Click instead
#: reads ``-100500`` as an unknown option and errors. ``ignore_unknown_options``
#: restores argparse's behaviour: a ``-N`` token falls through to the positional,
#: options and the negative interleave freely (``-100 --yes`` and ``--yes -100``
#: both work), and a genuinely unknown ``--option`` still errors with exit 2 *when
#: every positional slot is already filled*. This replaces the fragile
#: argv-``--``-insertion the #1125 review rejected (string-munging could not
#: reproduce argparse's free interleaving).
#:
#: **Applied narrowly (#1162 review):** only the ~34 commands whose first
#: positional is a Telegram-id / identifier / query (``channel_id`` / ``chat_id`` /
#: ``identifier`` / ``recipient`` / ``from_chat`` / ``query`` / ``message_id`` /
#: ``source`` / ``target``) carry this. Commands keyed on a *positive* DB primary
#: key (``pipeline <id>``, ``agent context <thread_id>``, ``search-query <id>``, …)
#: keep Click's strict option checking, so an ``--typo`` on those still errors.
#:
#: **Accepted trade-off (#1162 review):** on the negative-capable commands above,
#: ``ignore_unknown_options`` cannot distinguish a real negative value from an
#: unknown dash token, so an unknown option (``-x`` *or* ``--xyz``) that lands in
#: front of an *open* string positional is absorbed as that positional instead of
#: erroring — e.g. ``search --typo`` searches for the literal ``--typo`` rather
#: than exiting 2. This is strictly more permissive (no valid invocation breaks)
#: and the operator's negative-id workflow (Telegram ids are negative everywhere)
#: is the priority; the loss of strict ``--typo`` detection on this subset is the
#: accepted cost.
_NEG_ID_POSITIONAL = {"ignore_unknown_options": True}


class SearchMode(str, Enum):
    """``search --mode`` choices — mirrors the argparse ``choices=[…]`` set.

    Subclassing ``str`` keeps the value a plain string for the command bodies
    (which compare against ``"local"`` / ``"telegram"`` / … literals) while
    giving Typer the closed choice set argparse enforced, so an unknown ``--mode``
    is rejected on the Typer path too (not silently treated as ``local``).
    """

    local = "local"
    semantic = "semantic"
    hybrid = "hybrid"
    telegram = "telegram"
    #: Accepted shorthand for ``telegram`` (#1753); ``search_impl`` normalizes
    #: it to ``telegram`` before any mode dispatch, so no consumer sees "tg".
    tg = "tg"
    my_chats = "my_chats"
    channel = "channel"


class OutputFormat(str, Enum):
    """``messages read --format`` choices — mirrors the argparse ``choices=[…]``."""

    text = "text"
    json = "json"
    csv = "csv"


class PhotoMode(str, Enum):
    """``photo-loader … --mode`` choices — mirrors the argparse ``choices=[…]``.

    Subclasses ``str`` so the command body receives a plain ``"album"``/``"separate"``
    and ``.value`` round-trips cleanly into the *_impl bodies.
    """

    album = "album"
    separate = "separate"


class ExportFormat(str, Enum):
    """``export telegram --format`` choices — mirrors the argparse ``choices=[…]``."""

    json = "json"
    html = "html"
    both = "both"


class AnalyticsUseful(str, Enum):
    """``analytics channel-rating --useful`` choices — mirrors the argparse set."""

    useful = "useful"
    useless = "useless"


class AnalyticsGenre(str, Enum):
    """``analytics channel-rating --genre`` choices — mirrors the argparse set."""

    ad = "ad"
    infobiz = "infobiz"
    aggregator = "aggregator"
    copy = "copy"
    original = "original"


class PublishMode(str, Enum):
    """``pipeline add/edit --publish-mode`` choices."""

    auto = "auto"
    moderated = "moderated"


class GenerationBackend(str, Enum):
    """``pipeline add/edit --generation-backend`` choices."""

    chain = "chain"
    agent = "agent"
    deep_agents = "deep_agents"


class SinceUnit(str, Enum):
    """``pipeline add/dry-run-count --since-unit`` choices."""

    m = "m"
    h = "h"
    d = "d"


class TriBool(str, Enum):
    """``pipeline filter set --forwarded/--has-text`` choices (true/false)."""

    true = "true"
    false = "false"


def apply_startup(ctx: typer.Context) -> None:
    """Run CLI startup through ``typer_commands`` so existing patch paths still work."""
    from tg_agent.cli import typer_commands

    active = getattr(typer_commands, "apply_startup", _apply_startup)
    active(ctx)


def sync_prompt(text: str) -> str:
    """Read one line from stdin for a [y/N]-style confirmation gate.

    A bare ``input()`` inside an async command body blocks the event-loop thread.
    With ``asyncio.Runner``'s SIGINT handler active (any ``asyncio.run`` on
    Python ≥3.11), the first Ctrl-C only marks the main task for cancellation —
    the loop thread is stuck in the read, the interrupted read retries (PEP 475),
    and the process hangs with its aiosqlite threads open until a second Ctrl-C
    (which may then skip the ``finally: await db.close()`` teardown). Temporarily
    restoring the default handler makes the FIRST Ctrl-C raise
    ``KeyboardInterrupt`` right at the prompt, unwinding through the command's
    normal cleanup (#1590).
    """
    try:
        previous = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.default_int_handler)
    except ValueError:
        # Not the main thread — cannot (and need not) touch signal handlers.
        return input(text)
    try:
        return input(text)
    finally:
        signal.signal(signal.SIGINT, previous)


def run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run async command bodies through ``typer_commands`` for patch-path compatibility."""
    from tg_agent.cli import typer_commands

    active = getattr(typer_commands, "run_async", _run_async)
    return active(coro)


def cli_error(message: str, *, code: int = 1) -> NoReturn:
    """Report a failed command: *message* to stderr, non-zero exit (#1486).

    The automation-first contract makes exit codes the primary machine-readable
    outcome: consumers are programs, so a failed command must be distinguishable
    from a successful one. Errors go to stderr, stdout stays for results, and
    the process exits with *code*. Empty-but-valid results ("No messages
    found.") are not failures — they keep printing to stdout with exit 0.

    Usable from sync and ``async`` command bodies alike: ``typer.Exit`` raised
    inside the coroutine propagates through ``run_async``/``asyncio.run`` to
    the Typer runner, and ``main()`` turns it into ``SystemExit(code)``.
    """
    print(message, file=sys.stderr)
    raise typer.Exit(code)


def resolve_date_bounds(
    date_from: str | None, date_to: str | None
) -> tuple[str | None, str | None]:
    """Validate and normalize ``--date-from``/``--date-to`` for date-bounded commands.

    Relaxed ``YYYY-M-D`` canonicalizes to ``YYYY-MM-DD``, full ISO datetimes
    keep exact precision; anything else raises ``typer.BadParameter`` (usage
    error, exit 2) instead of letting the raw string reach a lexicographic SQL
    comparison and silently mis-window the query (#1593).
    """

    def bound(value: str | None) -> str | None:
        if not value:
            return None
        try:
            return parse_date_bound(value).isoformat()
        except ValueError:
            return parse_datetime_bound(value).isoformat()

    try:
        first = bound(date_from)
    except ValueError as exc:
        raise typer.BadParameter(f"--date-from: {exc}") from exc
    try:
        second = bound(date_to)
    except ValueError as exc:
        raise typer.BadParameter(f"--date-to: {exc}") from exc
    return first, second


def resolve_channel(channels: list, identifier: str):
    try:
        num = int(identifier)
        ch = next((c for c in channels if c.id == num), None)
        if ch:
            return ch
        return next((c for c in channels if c.channel_id == num), None)
    except ValueError:
        pass

    uname = identifier.lstrip("@").lower()
    return next((c for c in channels if (c.username or "").lower() == uname), None)
