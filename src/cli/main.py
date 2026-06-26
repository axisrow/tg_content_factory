from __future__ import annotations

import re
import sys
from typing import cast

import click

# Typer is the single CLI entry point (epic #959, Final — issue #1125). Every
# command is declared on ``src/cli/typer_app.py::app`` and registered by importing
# ``src.cli.typer_commands`` (its module body attaches the command groups to
# ``app``). The previous argparse framework — ``build_parser()`` + the dict
# dispatcher + the ``dispatch_via_typer`` bridge — was removed in #1125; ``app``
# now owns global option parsing (``--config`` / ``--version`` via
# ``main_callback``) and the per-command startup side effects run in
# ``apply_startup`` (exported ``TG_CONFIG_PATH`` / dotenv / logging / data dirs),
# which each command calls as its first line so a ``subcommand --help`` stays
# side-effect-free.
import src.cli.typer_commands  # noqa: F401  (import attaches commands to ``app``)
from src.cli.typer_app import app

# Typer vendors its *own* copy of Click under ``typer._click``, so the exception a
# Typer sub-group raises (``NoArgsIsHelpError`` / ``ClickException``) is NOT the
# same class as the one in the top-level ``click`` package — an ``except
# click.exceptions.ClickException`` would silently miss it. ``main`` must catch
# both. The vendored module is private; fall back gracefully to the public
# ``click`` types if a future Typer drops it, so the import can never crash the
# CLI.
try:  # pragma: no cover - exercised indirectly via main() bare-group tests
    from typer._click import exceptions as _typer_click_exc

    _CLICK_EXCEPTIONS: tuple[type[BaseException], ...] = (
        click.exceptions.ClickException,
        _typer_click_exc.ClickException,
    )
    _NO_ARGS_HELP_EXCEPTIONS: tuple[type[BaseException], ...] = (
        click.exceptions.NoArgsIsHelpError,
        _typer_click_exc.NoArgsIsHelpError,
    )
except ImportError:  # pragma: no cover - defensive fallback
    _CLICK_EXCEPTIONS = (click.exceptions.ClickException,)
    _NO_ARGS_HELP_EXCEPTIONS = (click.exceptions.NoArgsIsHelpError,)


#: A negative-number token (``-100123``, ``-1.5``). argparse treats such tokens as
#: positional *values* (no option in this CLI is a negative number); Click instead
#: tries to parse them as options and errors. ``_escape_negative_positionals`` adds
#: the ``--`` separator argparse made implicit (#1125 / #1162 Codex review).
_NEGATIVE_NUMBER = re.compile(r"^-\d+(\.\d+)?$|^-\.\d+$")


def _escape_negative_positionals(argv: list[str]) -> list[str]:
    """Insert ``--`` before the first negative-number *positional* in *argv*.

    Restores argparse parity for negative-id positionals (Telegram channel ids are
    negative): argparse accepted ``search -100500`` / ``collect sample -100123`` /
    ``analytics channel -100123456`` directly, but Click reads ``-100500`` as an
    unknown option unless a ``--`` separates options from positionals. The old
    ``_argv_from_namespace`` bridge inserted that ``--``; with the bridge gone we
    reproduce it on the raw argv here.

    A ``-N`` token is a positional value only when it does *not* immediately follow
    an option that consumes it (``--channel-id -100123`` is fine — Click takes the
    negative as the option's value). So we escape a ``-N`` token only when the
    preceding token is not itself a flag. We insert a single ``--`` (idempotent if
    one is already present) before the first such token; Click then treats every
    later token as a positional, exactly as the explicit separator would.
    """
    if "--" in argv:
        return argv
    for index, token in enumerate(argv):
        if not _NEGATIVE_NUMBER.match(token):
            continue
        prev = argv[index - 1] if index > 0 else ""
        # Preceded by an option flag → it's that option's value; leave it for Click.
        if prev.startswith("-") and prev != "--":
            continue
        return [*argv[:index], "--", *argv[index:]]
    return argv


def main() -> None:
    """CLI entry point — run the Typer ``app`` with argparse-identical exit codes.

    The app runs in non-standalone mode so we can reproduce the exact behaviour
    the old argparse dispatcher had (the ``dispatch_via_typer`` bridge, removed in
    #1125, did the same):

    * **Negative-number positionals** (``search -100500``) are escaped with ``--``
      via :func:`_escape_negative_positionals` so Click accepts them as values, the
      way argparse did natively.
    * A bare command group with ``no_args_is_help`` (e.g. ``messages`` with no
      ``read`` sub-command) raises :class:`click.exceptions.NoArgsIsHelpError`.
      argparse's old ``sub_attr`` fallback printed that group's ``--help`` and
      exited **0**; we reproduce that — render the help, exit 0.
    * The **root** with no command, by contrast, exited **1** under argparse
      (``command is None`` → ``print_help`` + ``sys.exit(1)``); we detect that case
      (the help error's context is the root app) and exit 1, not 0.
    * Any other usage error (unknown option, bad value) renders normally and exits
      with its own (non-zero) code, matching argparse's exit-2 on misuse.
    * A clean run / ``Exit(0)`` returns normally (exit 0).
    """
    argv = _escape_negative_positionals(sys.argv[1:])
    try:
        app(args=argv, standalone_mode=False)
    except _NO_ARGS_HELP_EXCEPTIONS as exc:
        # Bare group → show help and exit cleanly (argparse parity: exit 0). The
        # bare *root* (no command at all) is the exception: argparse exited 1 there,
        # so map the root help-error to exit 1. The root is identifiable as the
        # context whose command is the app itself (no parent). Must precede the
        # generic ClickException arm — NoArgsIsHelpError is a subclass of it.
        cast("click.ClickException", exc).show()
        ctx = getattr(exc, "ctx", None)
        is_root = ctx is not None and ctx.parent is None
        raise SystemExit(1 if is_root else 0) from None
    except _CLICK_EXCEPTIONS as exc:
        click_exc = cast("click.ClickException", exc)
        click_exc.show()
        raise SystemExit(click_exc.exit_code) from None
    except click.exceptions.Abort:
        # Ctrl-C / abort — Click prints "Aborted!" in standalone mode; match the
        # conventional non-zero exit so an interrupted CLI run is distinguishable.
        click.echo("Aborted!", err=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
