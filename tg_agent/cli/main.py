from __future__ import annotations

import importlib
from typing import Any, cast

import click
from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitedError

try:
    from typer.exceptions import Abort as TyperAbort
except ImportError:  # pragma: no cover - defensive fallback, mirrors _CLICK_EXCEPTIONS below
    # Older Typer versions (>=0.12 floor in pyproject) may not ship the module;
    # there ``typer.Abort`` IS ``click.exceptions.Abort``, which the first
    # tuple element below already catches.
    TyperAbort = click.exceptions.Abort

# Typer is the single CLI entry point (epic #959, Final — issue #1125). Every
# command is declared on ``tg_agent/cli/typer_app.py::app`` and registered by importing
# ``tg_agent.cli.typer_commands`` (its module body attaches the command groups to
# ``app``). The previous argparse framework — ``build_parser()`` + the dict
# dispatcher + the ``dispatch_via_typer`` bridge — was removed in #1125; ``app``
# now owns global option parsing (``--config`` / ``--version`` via
# ``main_callback``) and the per-command startup side effects run in
# ``apply_startup`` (exported ``TG_CONFIG_PATH`` / dotenv / logging / data dirs),
# which each command calls as its first line so a ``subcommand --help`` stays
# side-effect-free.
import tg_agent.cli.typer_commands  # noqa: F401  (import attaches commands to ``app``)
from tg_agent.cli.typer_app import app

# Typer vendors its *own* copy of Click under ``typer._click``, so the exception a
# Typer sub-group raises (``NoArgsIsHelpError`` / ``ClickException``) is NOT the
# same class as the one in the top-level ``click`` package — an ``except
# click.exceptions.ClickException`` would silently miss it. ``main`` must catch
# both. The vendored module is private; fall back gracefully to the public
# ``click`` types if a future Typer drops it, so the import can never crash the
# CLI.
try:  # pragma: no cover - exercised indirectly via main() bare-group tests
    _typer_click_exc = cast(Any, importlib.import_module("typer._click.exceptions"))

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

    The app runs in non-standalone mode so we can reproduce the exact behaviour
    the old argparse dispatcher had (the ``dispatch_via_typer`` bridge, removed in
    #1125, did the same):

    * A bare command group with ``no_args_is_help`` (e.g. ``messages`` with no
      ``read`` sub-command) raises :class:`click.exceptions.NoArgsIsHelpError`.
      argparse's old ``sub_attr`` fallback printed that group's ``--help`` and
      exited **0**; we reproduce that — render the help, exit 0.
    * The **root** with no command, by contrast, exited **1** under argparse
      (``command is None`` → ``print_help`` + ``sys.exit(1)``); we detect that case
      (the help error's context is the root app) and exit 1, not 0.
    * Any other usage error (unknown option, bad value) renders normally and exits
      with its own (non-zero) code, matching argparse's exit-2 on misuse.
    * A clean run / ``Exit(0)`` returns normally (exit 0); a non-zero exit
      returned by Typer is propagated to the process, including Ctrl-C (130).

    Negative-number positionals (Telegram channel/chat ids: ``search -100500``)
    are handled at the *command* level via ``ignore_unknown_options`` (see
    ``_NEG_ID_POSITIONAL`` in ``typer_commands``), not by rewriting argv here — that
    reproduces argparse's free option/positional interleaving, which a flat ``--``
    insertion could not (#1125 / #1162 review).
    """
    try:
        exit_code = app(standalone_mode=False)
        # Non-standalone Typer returns Exit codes instead of raising SystemExit.
        # Both console-script and module entrypoints must preserve those codes.
        if isinstance(exit_code, int) and exit_code != 0:
            raise SystemExit(exit_code)
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
    except (HandledFloodWaitError, TelegramRateLimitedError) as exc:
        # A refusal that no command caught closer to the call — a transfer the
        # flood gate deferred, a spent history hour, a Telegram FloodWait — is
        # a reason, not a crash: the curated line `dialogs send` prints, and
        # exit 1 (#1985: a deferred upload left `photo-loader send` as a
        # traceback).
        from tg_agent.telegram.rpc_guard import describe_rpc_failure

        click.echo(f"Refused: {describe_rpc_failure(exc)}", err=True)
        raise SystemExit(1) from None
    except (click.exceptions.Abort, TyperAbort):
        # Ctrl-C / abort — Click prints "Aborted!" in standalone mode; match the
        # conventional non-zero exit so an interrupted CLI run is distinguishable.
        # Typer's Abort is a separate class (RuntimeError, not a
        # click.exceptions.Abort): EOF at a confirmation prompt (agent run with a
        # closed stdin) surfaces as ``typer.exceptions.Abort`` in non-standalone
        # mode and must refuse just as cleanly (#1590). On Typer versions without
        # the module, ``TyperAbort`` aliases click's own Abort (harmless duplicate
        # in the tuple).
        click.echo("Aborted!", err=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
