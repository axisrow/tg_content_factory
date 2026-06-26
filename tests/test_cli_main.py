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

    def test_root_no_args_shows_help_and_exits_one(self, capsys):
        """No command at all → root help, exit **1** (argparse parity, #1162).

        argparse's old ``main([])`` resolved ``command=None``, printed the root
        help and called ``sys.exit(1)``. The Typer entry must match: the *root*
        no-args help error maps to exit 1, distinct from a bare *subgroup* (exit
        0). A regression here previously slipped because the test did not assert
        the exit code (Codex cycle-review).
        """
        code = _run_main([])
        assert code == 1
        combined = "".join(capsys.readouterr())
        assert "Traceback" not in combined

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


class TestNegativeIdPositionalParity:
    """Negative-id positionals work without an explicit ``--`` (argparse parity).

    Telegram channel/chat ids are negative, so ``search -100500`` /
    ``collect sample -100123`` / ``analytics channel -100123456`` were everyday
    invocations argparse accepted directly. Click would read ``-100500`` as an
    unknown option; the affected commands carry ``ignore_unknown_options`` (see
    ``_NEG_ID_POSITIONAL`` in ``typer_commands``) which lets the ``-N`` token fall
    through to the positional — reproducing argparse's free option/positional
    interleaving, which a flat ``--`` insertion could not (#1162 cycle-review).
    """

    def test_search_negative_query(self):
        with patch("src.cli.typer_commands.search_cmd.search_impl") as mock_impl:
            code = _run_main(["search", "-100500"])
        assert code == 0
        assert mock_impl.call_args.kwargs["query"] == "-100500"

    def test_collect_sample_negative_channel_id(self):
        with patch("src.cli.typer_commands.collect_cmd.collect_sample_impl") as mock_impl:
            code = _run_main(["collect", "sample", "-100123"])
        assert code == 0
        assert mock_impl.call_args.kwargs["channel_id"] == -100123

    def test_messages_read_negative_identifier(self):
        with patch("src.cli.typer_commands.messages_cmd.messages_read_impl") as mock_impl:
            code = _run_main(["messages", "read", "-100500"])
        assert code == 0
        assert mock_impl.call_args.kwargs["identifier"] == "-100500"

    def test_analytics_channel_negative_id(self):
        with patch("src.cli.typer_commands.analytics_cmd.channel_impl") as mock_impl:
            code = _run_main(["analytics", "channel", "-100123456"])
        assert code == 0
        assert mock_impl.call_args.kwargs["channel_id"] == -100123456

    def test_negative_value_as_option_value_is_untouched(self):
        """``--channel-id -100123`` — the negative is the *option's* value, parsed
        natively (collect carries no positional / context override either way)."""
        with patch("src.cli.typer_commands.collect_cmd.collect_impl") as mock_impl:
            code = _run_main(["collect", "--channel-id", "-100123"])
        assert code == 0
        assert mock_impl.call_args.kwargs["channel_id"] == -100123

    def test_negative_id_interleaves_with_flag_after(self):
        """``analytics channel -100123456 --days 14`` — option AFTER the negative
        positional (free interleaving, the case a flat ``--`` insertion broke)."""
        with patch("src.cli.typer_commands.analytics_cmd.channel_impl") as mock_impl:
            code = _run_main(["analytics", "channel", "-100123456", "--days", "14"])
        assert code == 0
        assert mock_impl.call_args.kwargs["channel_id"] == -100123456
        assert mock_impl.call_args.kwargs["days"] == 14

    def test_negative_id_interleaves_with_flag_before(self):
        """``analytics channel --days 14 -100123456`` — option BEFORE the negative."""
        with patch("src.cli.typer_commands.analytics_cmd.channel_impl") as mock_impl:
            code = _run_main(["analytics", "channel", "--days", "14", "-100123456"])
        assert code == 0
        assert mock_impl.call_args.kwargs["channel_id"] == -100123456
        assert mock_impl.call_args.kwargs["days"] == 14

    def test_unknown_option_errors_when_positional_slot_is_filled(self):
        """On a neg-capable command, an unknown ``--option`` still exits non-zero
        when every positional slot is already filled (no slot to absorb it)."""
        # channel_id is an int positional — once ``-100123456`` fills it, the
        # trailing ``--no-such-flag`` has nowhere to go and Click errors.
        code = _run_main(["analytics", "channel", "-100123456", "--no-such-flag"])
        assert code not in (0, None)


class TestNegIdScopeIsNarrow:
    """``ignore_unknown_options`` is applied only to negative-capable commands.

    Telegram-id / identifier / query positionals need it (their value can start
    with ``-``); commands keyed on a *positive* DB primary key keep Click's strict
    option checking so a typo'd ``--option`` still errors (#1162 narrowing).
    """

    def test_positive_pk_command_rejects_unknown_option(self):
        """``pipeline show --badtypo`` (id is a positive pk) → strict error, exit 2.

        This is the case the cycle-review flagged: without the narrowing, every
        command absorbed ``--badtypo`` into an open positional. ``pipeline show``
        must NOT carry ``ignore_unknown_options``.
        """
        code = _run_main(["pipeline", "show", "--badtypo"])
        assert code not in (0, None)

    def test_positive_pk_multi_positional_rejects_unknown_option(self):
        """``pipeline node replace 5 --badtypo spec`` → strict error (no masking)."""
        code = _run_main(["pipeline", "node", "replace", "5", "--badtypo", "spec"])
        assert code not in (0, None)

    def test_agent_context_rejects_unknown_option(self):
        """``agent context 5 --badtypo`` (thread_id positive pk) → strict error."""
        code = _run_main(["agent", "context", "5", "--badtypo"])
        assert code not in (0, None)

    def test_neg_capable_command_absorbs_unknown_into_open_slot(self):
        """Accepted trade-off (#1162): on a neg-capable command an unknown dash
        token in front of an *open* string positional is absorbed as that
        positional rather than erroring — the cost of ``ignore_unknown_options``.

        This test pins the *known, accepted* behaviour so a reviewer sees it is
        intentional, not an oversight: ``search --typo`` searches for ``--typo``.
        """
        with patch("src.cli.typer_commands.search_cmd.search_impl") as mock_impl:
            code = _run_main(["search", "--typo"])
        assert code == 0
        assert mock_impl.call_args.kwargs["query"] == "--typo"
