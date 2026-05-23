from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.conftest import (
    CLI_REAL_TG_LIVE_FIXTURE,
    REAL_TG_LIVE_FIXTURE,
    REAL_TG_LIVE_FIXTURES,
    REAL_TG_MANUAL_GATE_ENV,
    REAL_TG_MANUAL_MARK,
    REAL_TG_NEVER_MARK,
    REAL_TG_SAFE_GATE_ENV,
    REAL_TG_SAFE_MARK,
    _build_real_telegram_sandbox_config,
    _evaluate_real_tg_policy,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_TESTS_DIR = _REPO_ROOT / "tests"
_CLI_REAL_TG_DIR = _TESTS_DIR / "cli_real_tg_integration"
_MUTATING_PATTERNS = (
    "send_message(",
    "send_file(",
    "setup_bot(",
    "teardown_bot(",
    "leave_channels(",
    "delete_dialog(",
    "send_code(",
    "resend_code(",
    "verify_code(",
    "search_telegram(",
    "check_search_quota(",
)
_SAFE_MARKER_USAGES = (
    "@pytest.mark.real_tg_safe",
    "pytestmark = pytest.mark.real_tg_safe",
)
_MANUAL_MARKER_USAGES = (
    "@pytest.mark.real_tg_manual",
    "pytestmark = pytest.mark.real_tg_manual",
)
_NEVER_MARKER_USAGES = (
    "@pytest.mark.real_tg_never",
    "pytestmark = pytest.mark.real_tg_never",
)
_AUDIT_EXCLUDED_FILES = {"test_real_telegram_policy.py"}
_SAFE_RO_CLI_COMMAND_PREFIXES = {
    ("account", "flood-status"),
    ("account", "info"),
    ("account", "list"),
    ("agent", "messages"),
    ("agent", "threads"),
    ("analytics", "calendar"),
    ("analytics", "channel"),
    ("analytics", "content-types"),
    ("analytics", "daily"),
    ("analytics", "hourly"),
    ("analytics", "peak-hours"),
    ("analytics", "pipeline-stats"),
    ("analytics", "summary"),
    ("analytics", "top"),
    ("analytics", "trending-channels"),
    ("analytics", "trending-emojis"),
    ("analytics", "trending-topics"),
    ("analytics", "velocity"),
    ("channel", "list"),
    ("channel", "stats"),
    ("channel", "tag", "get"),
    ("channel", "tag", "list"),
    ("debug", "logs"),
    ("debug", "memory"),
    ("debug", "timing"),
    ("dialogs", "broadcast-stats"),
    ("dialogs", "cache-status"),
    ("dialogs", "list"),
    ("dialogs", "resolve"),
    ("dialogs", "topics"),
    ("export", "csv"),
    ("export", "json"),
    ("export", "rss"),
    ("filter", "analyze"),
    ("filter", "precheck"),
    ("image", "generated"),
    ("image", "providers"),
    ("messages", "read"),
    ("notification", "dry-run"),
    ("notification", "status"),
    ("photo-loader", "dialogs"),
    ("pipeline", "dry-run"),
    ("pipeline", "dry-run-count"),
    ("pipeline", "filter", "show"),
    ("pipeline", "graph"),
    ("pipeline", "list"),
    ("pipeline", "moderation-list"),
    ("pipeline", "moderation-view"),
    ("pipeline", "queue"),
    ("pipeline", "run-show"),
    ("pipeline", "runs"),
    ("pipeline", "show"),
    ("pipeline", "templates"),
    ("provider", "list"),
    ("scheduler", "status"),
    ("search", "test"),
    ("search-query", "get"),
    ("search-query", "list"),
    ("search-query", "stats"),
    ("settings", "get"),
    ("settings", "info"),
    ("test", "read"),
    ("translate", "stats"),
}
_SAFE_WRITE_CLI_COMMAND_PREFIXES = {
    ("agent", "chat"),
    ("agent", "threads"),
    ("channel", "add"),
    ("dialogs", "download-media"),
    ("pipeline", "export"),
    ("test", "all"),
    ("test", "benchmark"),
}
_MUTATING_CLI_COMMAND_PREFIXES = {
    ("channel", "refresh-meta"),
    ("scheduler", "trigger"),
}
_DESTRUCTIVE_CLI_COMMAND_PREFIXES = {
    ("restart",),
    ("stop",),
}
_HEAVY_CLI_COMMAND_PREFIXES = {
    ("channel", "collect"),
    ("channel", "refresh-meta", "--all"),
    ("channel", "refresh-types"),
    ("channel", "stats", "--all"),
    ("dialogs", "refresh"),
}


def test_real_tg_policy_rejects_live_fixture_without_policy_marker():
    action, message = _evaluate_real_tg_policy(
        mode=None,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
    )

    assert action == "fail"
    assert REAL_TG_LIVE_FIXTURE in message


def test_real_tg_policy_requires_live_fixture_for_safe_mode():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(),
        environ={REAL_TG_SAFE_GATE_ENV: "1"},
    )

    assert action == "fail"
    assert REAL_TG_LIVE_FIXTURE in message


def test_real_tg_policy_rejects_safe_mode_without_fixture_for_cli_integration():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(),
        environ={REAL_TG_SAFE_GATE_ENV: "1"},
        is_cli_integration=True,
    )

    assert action == "fail"
    assert CLI_REAL_TG_LIVE_FIXTURE in message


def test_real_tg_policy_allows_cli_sandbox_fixture_for_cli_integration():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(CLI_REAL_TG_LIVE_FIXTURE,),
        environ={REAL_TG_SAFE_GATE_ENV: "1"},
        is_cli_integration=True,
    )

    assert action is None
    assert message is None


def test_real_tg_policy_rejects_manual_mode_without_fixture_for_cli_integration():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MANUAL_MARK,
        fixturenames=(),
        environ={REAL_TG_MANUAL_GATE_ENV: "1"},
        is_cli_integration=True,
    )

    assert action == "fail"
    assert CLI_REAL_TG_LIVE_FIXTURE in message


def test_real_tg_policy_skips_safe_mode_without_gate():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
    )

    assert action == "skip"
    assert REAL_TG_SAFE_GATE_ENV in message


def test_real_tg_policy_skips_manual_mode_without_gate():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MANUAL_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
    )

    assert action == "skip"
    assert REAL_TG_MANUAL_GATE_ENV in message


def test_real_tg_policy_rejects_never_mode_with_live_fixture():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_NEVER_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
    )

    assert action == "fail"
    assert REAL_TG_NEVER_MARK in message


def test_real_tg_policy_allows_gated_safe_mode():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={REAL_TG_SAFE_GATE_ENV: "1"},
    )

    assert action is None
    assert message is None


def test_real_tg_sandbox_config_requires_dedicated_real_tg_env():
    with pytest.raises(RuntimeError, match="REAL_TG_API_ID"):
        _build_real_telegram_sandbox_config(
            {
                "TG_API_ID": "1",
                "TG_API_HASH": "hash",
                "TG_SESSION": "session",
            }
        )


def test_real_tg_sandbox_config_parses_required_and_optional_fields():
    cfg = _build_real_telegram_sandbox_config(
        {
            "REAL_TG_API_ID": "1",
            "REAL_TG_API_HASH": "hash",
            "REAL_TG_PHONE": "+70000000000",
            "REAL_TG_SESSION": "session",
            "REAL_TG_READ_CHANNEL_USERNAME": "sandbox_channel",
            "REAL_TG_READ_CHANNEL_ID": "-100123",
            "REAL_TG_PRIVATE_CHAT_ID": "123456",
            "REAL_TG_BOT_USERNAME": "sandbox_bot",
        }
    )

    assert cfg.api_id == 1
    assert cfg.api_hash == "hash"
    assert cfg.phone == "+70000000000"
    assert cfg.session_string == "session"
    assert cfg.read_channel_username == "sandbox_channel"
    assert cfg.read_channel_id == -100123
    assert cfg.private_chat_id == 123456
    assert cfg.bot_username == "sandbox_bot"


def test_real_tg_safe_marker_is_not_used_in_mutating_test_files():
    violations: list[str] = []

    for path in _TESTS_DIR.rglob("test_*.py"):
        if path.name in _AUDIT_EXCLUDED_FILES:
            continue
        content = path.read_text(encoding="utf-8")
        if not any(marker in content for marker in _SAFE_MARKER_USAGES):
            continue
        for pattern in _MUTATING_PATTERNS:
            if pattern in content:
                violations.append(f"{path.name}: {pattern}")

    assert violations == []


def test_real_tg_never_marker_does_not_request_live_fixture():
    violations: list[str] = []

    for path in _TESTS_DIR.rglob("test_*.py"):
        if path.name in _AUDIT_EXCLUDED_FILES:
            continue
        content = path.read_text(encoding="utf-8")
        if not any(marker in content for marker in _NEVER_MARKER_USAGES):
            continue
        if REAL_TG_LIVE_FIXTURE in content:
            violations.append(path.name)

    assert violations == []


def test_live_fixture_is_not_used_without_real_tg_policy_marker():
    violations: list[str] = []

    for path in _TESTS_DIR.rglob("test_*.py"):
        if path.name in _AUDIT_EXCLUDED_FILES:
            continue
        content = path.read_text(encoding="utf-8")
        if not any(fixture in content for fixture in REAL_TG_LIVE_FIXTURES):
            continue
        if not any(marker in content for marker in _SAFE_MARKER_USAGES + _MANUAL_MARKER_USAGES):
            violations.append(path.name)

    assert violations == []


def _literal_run_cli_prefixes(path: Path) -> list[tuple[str, ...]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    prefixes: list[tuple[str, ...]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "run_cli":
            continue
        prefix: list[str] = []
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                prefix.append(arg.value)
                continue
            break
        prefixes.append(tuple(prefix))
    return prefixes


def _has_prefix(command: tuple[str, ...], allowed: set[tuple[str, ...]]) -> bool:
    return any(command[: len(prefix)] == prefix for prefix in allowed)


def test_cli_real_tg_safe_commands_are_explicitly_allowlisted():
    violations: list[str] = []

    for path in _CLI_REAL_TG_DIR.rglob("test_*.py"):
        content = path.read_text(encoding="utf-8")
        if not any(marker in content for marker in _SAFE_MARKER_USAGES):
            continue
        relative_parts = path.relative_to(_CLI_REAL_TG_DIR).parts
        category = relative_parts[0] if len(relative_parts) > 1 else "safe_ro"
        allowed_by_category = {
            "safe_ro": _SAFE_RO_CLI_COMMAND_PREFIXES,
            "safe_write": _SAFE_WRITE_CLI_COMMAND_PREFIXES,
            "mutating": _MUTATING_CLI_COMMAND_PREFIXES,
            "destructive": _DESTRUCTIVE_CLI_COMMAND_PREFIXES,
            "heavy": _HEAVY_CLI_COMMAND_PREFIXES,
        }
        allowed = allowed_by_category.get(category, _SAFE_RO_CLI_COMMAND_PREFIXES)
        for command in _literal_run_cli_prefixes(path):
            if not command:
                violations.append(f"{path.relative_to(_REPO_ROOT)}: dynamic run_cli command")
                continue
            if not _has_prefix(command, allowed):
                violations.append(f"{path.relative_to(_REPO_ROOT)}: {command!r} is not {category}-allowlisted")

    assert violations == []


def test_cli_real_tg_tests_use_sandbox_fixture_not_repo_config_or_db():
    content = (_CLI_REAL_TG_DIR / "conftest.py").read_text(encoding="utf-8")

    assert CLI_REAL_TG_LIVE_FIXTURE in content
    assert '"--config"' in content
    assert "tmp_path_factory.mktemp" in content
    assert "return self.work_dir" in content
    assert "_detect_repo_root" not in content
