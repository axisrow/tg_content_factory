"""Tests for src/cli/main.py — dispatch logic and argument parsing."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.main import main
from src.cli.parser import build_parser


class TestParser:
    """Test argument parser construction and parsing."""

    def test_parser_version_flag(self, capsys):
        """--version flag prints version and exits."""
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])
        assert exc_info.value.code == 0

    def test_parser_serve_subcommand(self):
        """serve subcommand parses --web-pass."""
        parser = build_parser()
        args = parser.parse_args(["serve", "--web-pass", "secret123"])
        assert args.command == "serve"
        assert args.web_pass == "secret123"

    def test_parser_serve_default(self):
        """serve subcommand has no default web-pass."""
        parser = build_parser()
        args = parser.parse_args(["serve"])
        assert args.command == "serve"
        assert args.web_pass is None

    def test_parser_stop_subcommand(self):
        """stop subcommand parses correctly."""
        parser = build_parser()
        args = parser.parse_args(["stop"])
        assert args.command == "stop"

    def test_parser_restart_subcommand(self):
        """restart subcommand parses --web-pass."""
        parser = build_parser()
        args = parser.parse_args(["restart", "--web-pass", "pw"])
        assert args.command == "restart"
        assert args.web_pass == "pw"

    def test_parser_collect_subcommand(self):
        """collect subcommand parses --channel-id."""
        parser = build_parser()
        args = parser.parse_args(["collect", "--channel-id", "12345"])
        assert args.command == "collect"
        assert args.channel_id == 12345

    def test_parser_collect_default(self):
        """collect subcommand has no default channel-id."""
        parser = build_parser()
        args = parser.parse_args(["collect"])
        assert args.command == "collect"
        assert args.channel_id is None

    def test_parser_search_subcommand(self):
        """search subcommand parses query and options."""
        parser = build_parser()
        args = parser.parse_args(["search", "test query", "--limit", "5", "--mode", "local"])
        assert args.command == "search"
        assert args.query == "test query"
        assert args.limit == 5
        assert args.mode == "local"

    def test_parser_search_defaults(self):
        """search subcommand has proper defaults."""
        parser = build_parser()
        args = parser.parse_args(["search"])
        assert args.command == "search"
        assert args.query == ""
        assert args.limit == 20
        assert args.mode == "local"

    def test_parser_messages_read(self):
        """messages read subcommand parses identifier and options."""
        parser = build_parser()
        args = parser.parse_args(["messages", "read", "test_channel", "--limit", "10"])
        assert args.command == "messages"
        assert args.messages_action == "read"
        assert args.identifier == "test_channel"
        assert args.limit == 10

    def test_parser_channel_list(self):
        """channel list subcommand."""
        parser = build_parser()
        args = parser.parse_args(["channel", "list"])
        assert args.command == "channel"
        assert args.channel_action == "list"

    def test_parser_channel_add(self):
        """channel add subcommand parses identifier."""
        parser = build_parser()
        args = parser.parse_args(["channel", "add", "@testchannel"])
        assert args.command == "channel"
        assert args.channel_action == "add"
        assert args.identifier == "@testchannel"

    def test_parser_pipeline_list(self):
        """pipeline list subcommand."""
        parser = build_parser()
        args = parser.parse_args(["pipeline", "list"])
        assert args.command == "pipeline"
        assert args.pipeline_action == "list"

    def test_parser_pipeline_add(self):
        """pipeline add subcommand parses name and required flags."""
        parser = build_parser()
        args = parser.parse_args([
            "pipeline", "add", "MyPipeline",
            "--prompt-template", "Summarize",
            "--source", "100",
            "--target", "+100|77",
        ])
        assert args.command == "pipeline"
        assert args.pipeline_action == "add"
        assert args.name == "MyPipeline"
        assert args.prompt_template == "Summarize"
        assert args.source == [100]
        assert args.target == ["+100|77"]

    def test_parser_pipeline_export(self):
        """pipeline export subcommand parses id and --output."""
        parser = build_parser()
        args = parser.parse_args(["pipeline", "export", "5", "--output", "/tmp/out.json"])
        assert args.command == "pipeline"
        assert args.pipeline_action == "export"
        assert args.id == 5
        assert args.output == "/tmp/out.json"

    def test_parser_pipeline_refinement_steps(self):
        """pipeline refinement-steps subcommand parses id and --set."""
        parser = build_parser()
        args = parser.parse_args(["pipeline", "refinement-steps", "3", "--set", '[{"type":"llm_refine"}]'])
        assert args.command == "pipeline"
        assert args.pipeline_action == "refinement-steps"
        assert args.id == 3
        assert args.steps_json == '[{"type":"llm_refine"}]'

    def test_parser_pipeline_from_template(self):
        """pipeline from-template subcommand parses template_id and name."""
        parser = build_parser()
        args = parser.parse_args(["pipeline", "from-template", "1", "NewPipeline"])
        assert args.pipeline_action == "from-template"
        assert args.template_id == 1
        assert args.name == "NewPipeline"

    def test_parser_pipeline_ai_edit(self):
        """pipeline ai-edit subcommand parses id, instruction, and --show."""
        parser = build_parser()
        args = parser.parse_args(["pipeline", "ai-edit", "5", "Add image node", "--show"])
        assert args.pipeline_action == "ai-edit"
        assert args.id == 5
        assert args.instruction == "Add image node"
        assert args.show is True

    def test_parser_account_list(self):
        """account list subcommand."""
        parser = build_parser()
        args = parser.parse_args(["account", "list"])
        assert args.command == "account"
        assert args.account_action == "list"

    def test_parser_scheduler_start(self):
        """scheduler start subcommand."""
        parser = build_parser()
        args = parser.parse_args(["scheduler", "start"])
        assert args.command == "scheduler"
        assert args.scheduler_action == "start"

    def test_parser_analytics_top(self):
        """analytics top subcommand."""
        parser = build_parser()
        args = parser.parse_args(["analytics", "top", "--limit", "10"])
        assert args.command == "analytics"
        assert args.analytics_action == "top"
        assert args.limit == 10

    def test_parser_translate_stats(self):
        """translate stats subcommand."""
        parser = build_parser()
        args = parser.parse_args(["translate", "stats"])
        assert args.command == "translate"
        assert args.translate_action == "stats"

    def test_parser_export_json(self):
        """export json subcommand."""
        parser = build_parser()
        args = parser.parse_args(["export", "json", "--limit", "50"])
        assert args.command == "export"
        assert args.export_action == "json"
        assert args.limit == 50

    def test_parser_settings_get(self):
        """settings get subcommand."""
        parser = build_parser()
        args = parser.parse_args(["settings", "get", "--key", "some_key"])
        assert args.command == "settings"
        assert args.settings_action == "get"
        assert args.key == "some_key"

    def test_parser_settings_set(self):
        """settings set subcommand."""
        parser = build_parser()
        args = parser.parse_args(["settings", "set", "my_key", "my_value"])
        assert args.command == "settings"
        assert args.settings_action == "set"
        assert args.key == "my_key"
        assert args.value == "my_value"

    def test_parser_debug_logs(self):
        """debug logs subcommand."""
        parser = build_parser()
        args = parser.parse_args(["debug", "logs", "--limit", "100"])
        assert args.command == "debug"
        assert args.debug_action == "logs"
        assert args.limit == 100

    def test_parser_provider_list(self):
        """provider list subcommand."""
        parser = build_parser()
        args = parser.parse_args(["provider", "list"])
        assert args.command == "provider"
        assert args.provider_action == "list"

    def test_parser_no_command(self):
        """No command returns command=None."""
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

    def test_parser_dialogs_list(self):
        """dialogs list subcommand."""
        parser = build_parser()
        args = parser.parse_args(["dialogs", "list"])
        assert args.command == "dialogs"
        assert args.dialogs_action == "list"

    def test_parser_my_telegram_alias(self):
        """my-telegram is an alias for dialogs."""
        parser = build_parser()
        args = parser.parse_args(["my-telegram", "list"])
        assert args.command == "my-telegram"
        assert args.dialogs_action == "list"

    def test_parser_filter_analyze(self):
        """filter analyze subcommand."""
        parser = build_parser()
        args = parser.parse_args(["filter", "analyze"])
        assert args.command == "filter"
        assert args.filter_action == "analyze"

    def test_parser_search_query_list(self):
        """search-query list subcommand."""
        parser = build_parser()
        args = parser.parse_args(["search-query", "list"])
        assert args.command == "search-query"
        assert args.search_query_action == "list"


class TestMainDispatch:
    """Test the main() dispatch logic."""

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_unknown_command_exits(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() exits with code 1 for unknown commands."""
        parser = MagicMock()
        args = MagicMock()
        args.command = None
        parser.parse_args.return_value = args
        mock_build.return_value = parser

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_dispatches_to_handler(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() calls the correct handler for a known command."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "stop"
        parser.parse_args.return_value = args
        mock_build.return_value = parser

        mock_handler = MagicMock()
        with patch("src.cli.main.server_control") as mock_mod:
            mock_mod.run_stop = mock_handler
            main()

        mock_handler.assert_called_once_with(args)

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_missing_subaction_prints_help(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() prints help when subcommand action is missing for commands that require it."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "pipeline"
        args.pipeline_action = None
        # First call returns args, second call (with --help) raises SystemExit
        parser.parse_args.side_effect = [args, SystemExit(0)]
        mock_build.return_value = parser

        with pytest.raises(SystemExit):
            main()

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_serve_dispatch(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() dispatches 'serve' command."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "serve"
        parser.parse_args.return_value = args
        mock_build.return_value = parser

        with patch("src.cli.main.serve") as mock_mod:
            mock_mod.run = MagicMock()
            main()
            mock_mod.run.assert_called_once_with(args)

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_collect_dispatch(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() dispatches 'collect' command."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "collect"
        args.collect_action = "sample"
        args.channel_id = 100
        parser.parse_args.return_value = args
        mock_build.return_value = parser

        with patch("src.cli.main.collect") as mock_mod:
            mock_mod.run = MagicMock()
            main()
            mock_mod.run.assert_called_once_with(args)

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_my_telegram_alias_dispatch(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() dispatches 'my-telegram' to the dialogs handler."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "my-telegram"
        args.dialogs_action = "list"
        parser.parse_args.return_value = args
        mock_build.return_value = parser

        with patch("src.cli.main.dialogs_cmd") as mock_mod:
            mock_mod.run = MagicMock()
            main()
            mock_mod.run.assert_called_once_with(args)

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_account_missing_action_prints_help(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() prints help when account_action is missing."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "account"
        args.account_action = None
        # First call returns args, second call (with --help) raises SystemExit
        parser.parse_args.side_effect = [args, SystemExit(0)]
        mock_build.return_value = parser

        with pytest.raises(SystemExit):
            main()

    @patch("src.cli.main.load_dotenv")
    @patch("src.cli.main.setup_logging")
    @patch("src.cli.main.ensure_data_dirs")
    @patch("src.cli.main.build_parser")
    def test_main_test_dispatch(self, mock_build, mock_dirs, mock_log, mock_dotenv):
        """main() dispatches 'test' command."""
        parser = MagicMock()
        args = MagicMock()
        args.command = "test"
        args.test_action = "all"
        parser.parse_args.return_value = args
        mock_build.return_value = parser

        with patch("src.cli.main.test_cmd") as mock_mod:
            mock_mod.run = MagicMock()
            main()
            mock_mod.run.assert_called_once_with(args)
