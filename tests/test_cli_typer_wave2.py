"""CliRunner tests for the Wave-2 Typer command groups (epic #959 — issue #1122).

Wave 2 migrates the flat, depth-1 command groups off the argparse dispatcher onto
the Typer ``app``:

    debug · export · translate · image · provider · notification

These tests drive the production ``app`` through ``typer.testing.CliRunner`` and
assert each sub-command:

* exposes the *same* flags / arguments / sub-command names the argparse parser did
  (the hard invariant of the migration), and
* delegates to the shared ``*_impl`` body with the flags mapped to exactly the
  right keyword arguments.

The shared bodies are stubbed (and ``run_async`` is patched to capture rather than
execute the coroutine) so no real DB / Telegram / provider work happens — the
wiring from CLI tokens to the body is what is under test.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from src.cli.typer_app import app

runner = CliRunner()


# --------------------------------------------------------------------------- #
# debug → logs / memory / timing
# --------------------------------------------------------------------------- #


def test_debug_logs_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.debug_cmd.logs_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["debug", "logs"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", limit=50)


def test_debug_logs_limit():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.debug_cmd.logs_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["debug", "logs", "--limit", "5"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", limit=5)


def test_debug_memory_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.debug_cmd.memory_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["debug", "memory"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_debug_timing_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.debug_cmd.timing_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["debug", "timing"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_debug_bare_group_shows_help():
    result = runner.invoke(app, ["debug"])
    assert result.exit_code != 0  # no_args_is_help → non-zero
    assert "logs" in result.output and "memory" in result.output


# --------------------------------------------------------------------------- #
# export → json / csv / rss / telegram
# --------------------------------------------------------------------------- #


def test_export_json_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.export_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["export", "json"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", fmt="json", channel_id=None, limit=200, output=None
    )


def test_export_csv_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.export_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["export", "csv", "--channel-id", "42", "--limit", "10", "-o", "out.csv"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", fmt="csv", channel_id=42, limit=10, output="out.csv"
    )


def test_export_rss_delegates_with_fmt():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.export_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["export", "rss"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["fmt"] == "rss"


def test_export_telegram_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.telegram_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["export", "telegram", "--channel-id", "100"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        channel_id=100,
        export_format="json",
        with_media=False,
        wait=False,
        max_file_size=None,
        date_from=None,
        date_to=None,
        limit=5000,
        output=None,
    )


def test_export_telegram_full_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.telegram_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "export", "telegram", "--channel-id", "100",
                "--format", "both", "--with-media", "--wait",
                "--max-file-size", "5", "--date-from", "2025-01-01",
                "--date-to", "2025-12-31", "--limit", "9", "--output", "/tmp/exp",
            ],
        )
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["export_format"] == "both"
    assert kwargs["with_media"] is True
    assert kwargs["wait"] is True
    assert kwargs["max_file_size"] == 5
    assert kwargs["date_from"] == "2025-01-01"
    assert kwargs["date_to"] == "2025-12-31"
    assert kwargs["limit"] == 9
    assert kwargs["output"] == "/tmp/exp"


def test_export_telegram_rejects_unknown_format():
    """--format keeps argparse's json/html/both choice set."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.telegram_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["export", "telegram", "--channel-id", "1", "--format", "bogus"])
    assert result.exit_code == 2
    mock_impl.assert_not_called()


# --------------------------------------------------------------------------- #
# translate → stats / detect / run / message
# --------------------------------------------------------------------------- #


def test_translate_stats_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.translate_cmd.stats_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["translate", "stats"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_translate_detect_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.translate_cmd.detect_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["translate", "detect"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", batch_size=5000)


def test_translate_detect_batch_size():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.translate_cmd.detect_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["translate", "detect", "--batch-size", "100"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", batch_size=100)


def test_translate_run_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.translate_cmd.run_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["translate", "run"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", target="en", source_filter="", limit=100
    )


def test_translate_run_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.translate_cmd.run_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["translate", "run", "--target", "ru", "--source-filter", "en,de", "--limit", "5"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", target="ru", source_filter="en,de", limit=5
    )


def test_translate_message_positional_and_target():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.translate_cmd.message_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["translate", "message", "777", "--target", "fr"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", message_id=777, target="fr")


# --------------------------------------------------------------------------- #
# image → generate / models / providers / generated
# --------------------------------------------------------------------------- #


def test_image_generate_prompt_only():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.generate_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["image", "generate", "a red cat"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", prompt="a red cat", model=None)


def test_image_generate_with_model():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.generate_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["image", "generate", "sky", "--model", "replicate:flux-schnell"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", prompt="sky", model="replicate:flux-schnell")


def test_image_models_requires_provider():
    """--provider is required (argparse required=True parity)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.models_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["image", "models"])
    assert result.exit_code == 2
    mock_impl.assert_not_called()


def test_image_models_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.models_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["image", "models", "--provider", "openai", "--query", "dall", "--refresh"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", provider="openai", query="dall", refresh=True
    )


def test_image_providers_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.providers_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["image", "providers"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_image_generated_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.generated_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["image", "generated"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", limit=20)


def test_image_generated_limit():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.image_cmd.generated_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["image", "generated", "--limit", "3"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", limit=3)


# --------------------------------------------------------------------------- #
# provider → list / add / delete / probe / refresh / test-all
# --------------------------------------------------------------------------- #


def test_provider_list_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.list_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "list"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_provider_add_requires_api_key():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "add", "openai"])
    assert result.exit_code == 2
    mock_impl.assert_not_called()


def test_provider_add_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["provider", "add", "openai", "--api-key", "sk-xxx", "--base-url", "http://x"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", name="openai", api_key="sk-xxx", base_url="http://x"
    )


def test_provider_delete_positional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.delete_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "delete", "groq"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", name="groq")


def test_provider_probe_positional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.probe_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "probe", "cohere"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", name="cohere")


def test_provider_refresh_no_name():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.refresh_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "refresh"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", name=None)


def test_provider_refresh_with_name():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.refresh_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "refresh", "openai"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", name="openai")


def test_provider_test_all_delegates():
    """The ``test-all`` sub-command name (with the dash) is preserved."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.provider_cmd.test_all_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["provider", "test-all"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


# --------------------------------------------------------------------------- #
# notification → setup / status / delete / test / dry-run / set-account
# --------------------------------------------------------------------------- #


def test_notification_setup_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.setup_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "setup"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_notification_status_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.status_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "status"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_notification_delete_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.delete_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "delete"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_notification_test_default_message():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.test_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "test"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", message="Тестовое уведомление")


def test_notification_test_custom_message():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.test_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "test", "--message", "hi"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", message="hi")


def test_notification_dry_run_delegates():
    """The ``dry-run`` sub-command name (with the dash) is preserved."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.dry_run_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "dry-run"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_notification_set_account_requires_phone():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.set_account_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "set-account"])
    assert result.exit_code == 2
    mock_impl.assert_not_called()


def test_notification_set_account_phone():
    """The ``set-account`` sub-command name (with the dash) is preserved."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.notification_cmd.set_account_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["notification", "set-account", "--phone", "+15550001111"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", phone="+15550001111")


# --------------------------------------------------------------------------- #
# Global --config threads through the migrated groups
# --------------------------------------------------------------------------- #


def test_global_config_threads_into_group():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.debug_cmd.timing_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["--config", "prod.yaml", "debug", "timing"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("prod.yaml")


# --------------------------------------------------------------------------- #
# Bare-group help: a group invoked with no sub-command renders its help.
# These ``no_args_is_help`` groups list their sub-commands and exit non-zero;
# crucially they never leak a NoArgsIsHelpError traceback.
# --------------------------------------------------------------------------- #


def test_provider_without_subcommand_shows_help():
    result = runner.invoke(app, ["provider"])
    assert result.exit_code != 0  # no_args_is_help → non-zero
    assert "list" in result.output and "add" in result.output


def test_image_without_subcommand_shows_help_no_traceback():
    """A bare ``image`` group prints help listing its sub-commands, no traceback."""
    result = runner.invoke(app, ["image"])
    assert result.exit_code != 0
    combined = result.output
    assert "NoArgsIsHelpError" not in combined
    assert "Traceback" not in combined
    assert "generate" in combined  # the help lists the sub-commands
