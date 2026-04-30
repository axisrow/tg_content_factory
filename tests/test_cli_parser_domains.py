from __future__ import annotations

import argparse

import pytest

from src.cli.parser import build_parser


def _subparser_choices(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices
    raise AssertionError("parser has no subparsers")


def test_cli_parser_registers_all_top_level_domains() -> None:
    choices = _subparser_choices(build_parser())

    assert set(choices) >= {
        "account",
        "agent",
        "analytics",
        "channel",
        "collect",
        "debug",
        "dialogs",
        "export",
        "filter",
        "image",
        "messages",
        "my-telegram",
        "notification",
        "photo-loader",
        "pipeline",
        "provider",
        "restart",
        "scheduler",
        "search",
        "search-query",
        "serve",
        "settings",
        "stop",
        "test",
        "translate",
        "worker",
    }


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (
            ["serve", "--no-worker", "--web-pass", "secret"],
            {"command": "serve", "no_worker": True, "web_pass": "secret"},
        ),
        (
            ["collect", "sample", "123", "--limit", "5"],
            {"command": "collect", "collect_action": "sample", "channel_id": 123, "limit": 5},
        ),
        (
            ["channel", "tag", "set", "7", "news,ai"],
            {"command": "channel", "channel_action": "tag", "tag_action": "set", "pk": 7, "tags": "news,ai"},
        ),
        (
            ["pipeline", "filter", "set", "3", "--message-kind", "text", "--has-text", "true"],
            {
                "command": "pipeline",
                "pipeline_action": "filter",
                "filter_action": "set",
                "id": 3,
                "message_kinds": ["text"],
                "has_text": "true",
            },
        ),
        (
            ["pipeline", "node", "add", "2", "source:channel_id=123"],
            {
                "command": "pipeline",
                "pipeline_action": "node",
                "node_action": "add",
                "pipeline_id": 2,
                "node_spec": "source:channel_id=123",
            },
        ),
        (
            ["photo-loader", "send", "--phone", "+1", "--target", "42", "--files", "a.jpg", "b.jpg"],
            {
                "command": "photo-loader",
                "photo_loader_action": "send",
                "phone": "+1",
                "target": "42",
                "files": ["a.jpg", "b.jpg"],
            },
        ),
        (
            ["settings", "agent", "--backend", "deepagents", "--prompt-template", "default"],
            {
                "command": "settings",
                "settings_action": "agent",
                "backend": "deepagents",
                "prompt_template": "default",
            },
        ),
    ],
)
def test_cli_parser_domain_regression(argv: list[str], expected: dict[str, object]) -> None:
    args = vars(build_parser().parse_args(argv))

    for key, value in expected.items():
        assert args[key] == value


@pytest.mark.parametrize(
    ("argv", "expected_text"),
    [
        (["channel", "--help"], "Manage channel tags"),
        (["pipeline", "--help"], "Manage semantic filters on pipeline DAGs"),
        (["photo-loader", "--help"], "Run due photo items and auto jobs now"),
        (["settings", "--help"], "Configure semantic search"),
    ],
)
def test_cli_parser_domain_help_smoke(
    argv: list[str],
    expected_text: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(argv)

    assert exc.value.code == 0
    assert expected_text in capsys.readouterr().out
