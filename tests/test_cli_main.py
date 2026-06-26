"""Tests for src/cli/main.py — the thin Typer entry point (#1125 Final).

The argparse framework (``build_parser`` + dict dispatcher + ``dispatch_via_typer``
bridge) was removed in #1125. ``main()`` now runs the Typer ``app`` in
non-standalone mode and reproduces the exact exit codes the old argparse path had:

* a bare command group (``no_args_is_help``) renders help and exits **0** (argparse
  ``sub_attr`` parity), NOT a ``NoArgsIsHelpError`` traceback;
* an unknown command / usage error exits non-zero;
* ``--version`` exits 0; a normal command delegates to its body.

Per-command flag wiring is covered by the Wave CliRunner suites
(``test_cli_typer_wave{1..4}.py``); here we test the entry point itself.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.cli.main import main


def _run_main(argv: list[str]):
    """Run ``main()`` with *argv* as the process argv; return the SystemExit code.

    Returns ``0`` when ``main()`` returns without raising (clean exit).
    """
    with patch("sys.argv", ["main.py", *argv]):
        try:
            main()
        except SystemExit as exc:
            return exc.code
    return 0


class TestMainEntryPoint:
    """The Typer entry point's exit-code contract."""

    def test_bare_group_shows_help_and_exits_zero(self, capsys):
        """A bare group (``messages`` with no ``read``) → help, exit 0, no traceback.

        argparse's old ``sub_attr`` fallback printed the group help and exited 0;
        ``main()`` reproduces that for the Typer ``no_args_is_help`` groups.
        """
        code = _run_main(["messages"])
        assert code == 0
        out = capsys.readouterr()
        combined = out.out + out.err
        assert "NoArgsIsHelpError" not in combined
        assert "Traceback" not in combined

    def test_nested_bare_group_exits_zero(self):
        """A bare depth-2 group (``channel tag``) also renders help and exits 0."""
        assert _run_main(["channel", "tag"]) == 0

    def test_unknown_command_exits_nonzero(self):
        """An unknown top-level command is a usage error → non-zero exit."""
        assert _run_main(["no-such-command"]) not in (0, None)

    def test_unknown_option_exits_nonzero(self):
        """An unknown option on a real command is a usage error → non-zero exit."""
        assert _run_main(["serve", "--no-such-flag"]) not in (0, None)

    def test_version_exits_zero(self, capsys):
        """``--version`` prints the version and exits 0 before any command runs."""
        code = _run_main(["--version"])
        assert code == 0
        out = capsys.readouterr()
        assert "src" in (out.out + out.err)

    def test_no_args_shows_help(self):
        """No command at all → root help (``no_args_is_help`` on the app)."""
        # The root app raises a usage/help exit; either way it is non-error help,
        # not a crash. We assert it does not raise an unexpected exception.
        _run_main([])

    def test_migrated_command_delegates_to_body(self):
        """A real command runs its shared body — ``serve`` → ``serve_web``.

        Confirms the entry point actually dispatches into a command (not just
        help handling) and threads the default config.
        """
        with patch("src.cli.typer_commands.serve_cmd.serve_web") as mock_serve:
            code = _run_main(["serve"])
        assert code == 0
        mock_serve.assert_called_once_with("config.yaml", web_pass=None, no_worker=False)

    def test_global_config_option_threads_to_body(self):
        """The global ``--config`` reaches the command body (argparse parity)."""
        with patch("src.cli.typer_commands.serve_cmd.serve_web") as mock_serve:
            code = _run_main(["--config", "prod.yaml", "serve"])
        assert code == 0
        mock_serve.assert_called_once_with("prod.yaml", web_pass=None, no_worker=False)


def test_main_is_module_entrypoint():
    """``python -m src.main`` resolves ``main`` from ``src.cli.main``."""
    import src.main

    assert src.main.main is main


@pytest.mark.parametrize(
    "argv",
    [
        ["messages"],
        ["channel", "tag"],
        ["dialogs", "queue"],
        ["pipeline", "filter"],
    ],
)
def test_bare_groups_uniformly_exit_zero(argv):
    """Every ``no_args_is_help`` group (flat and nested) exits 0 via ``main()``."""
    code = _run_main(argv)
    assert code == 0
