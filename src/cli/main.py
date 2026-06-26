from __future__ import annotations

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


def main() -> None:
    """CLI entry point — run the Typer ``app`` with argparse-identical exit codes.

    The app runs in non-standalone mode so we can reproduce the exact exit-code
    behaviour the old argparse dispatcher had (the ``dispatch_via_typer`` bridge,
    removed in #1125, did the same):

    * A bare command group with ``no_args_is_help`` (e.g. ``messages`` with no
      ``read`` sub-command) raises :class:`click.exceptions.NoArgsIsHelpError`.
      argparse's old ``sub_attr`` fallback printed that group's ``--help`` and
      exited **0**; we reproduce that — render the help, exit 0 — so the user sees
      usage, not a ``NoArgsIsHelpError`` traceback with a non-zero exit.
    * Any other usage error (unknown option, bad value) renders normally and
      exits with its own (non-zero) code, matching argparse's exit-2 on misuse.
    * A clean run / ``Exit(0)`` returns normally (exit 0).
    """
    try:
        app(standalone_mode=False)
    except _NO_ARGS_HELP_EXCEPTIONS as exc:
        # Bare group → show help and exit cleanly (argparse parity: exit 0).
        # Must precede the generic ClickException arm — NoArgsIsHelpError is a
        # subclass of it but argparse exited 0 here, not 2. ``exc`` is a (vendored
        # or stdlib) ClickException; both share ``.show()`` — cast for the checker.
        cast("click.ClickException", exc).show()
        raise SystemExit(0) from None
    except _CLICK_EXCEPTIONS as exc:
        click_exc = cast("click.ClickException", exc)
        click_exc.show()
        raise SystemExit(click_exc.exit_code) from None
    except click.exceptions.Abort:
        # Ctrl-C / abort — Click prints "Aborted!" in standalone mode; match the
        # conventional 130 exit so an interrupted CLI run is distinguishable.
        click.echo("Aborted!", err=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
