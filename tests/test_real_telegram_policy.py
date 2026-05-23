from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from src.cli.dotenv import load_cli_dotenv
from src.config import load_config
from tests.cli_real_tg_integration.command_manifest import (
    CLI_REAL_TG_CLEANUP_COMMAND_CASES,
    CLI_REAL_TG_COMMAND_CASES_BY_CATEGORY,
    CLI_REAL_TG_MANUAL_OR_EXCLUDED_COMMANDS,
)
from tests.cli_real_tg_integration.conftest import (
    LIVE_CLI_DEFAULT_PYTEST_TIMEOUT_SECONDS,
    RUN_CLI_DEFAULT_TIMEOUT_SECONDS,
    _assert_cli_result_ok,
)
from tests.conftest import (
    CLI_REAL_TG_LIVE_FIXTURE,
    REAL_TG_LIVE_FIXTURE,
    REAL_TG_LIVE_FIXTURES,
    REAL_TG_MANUAL_GATE_ENV,
    REAL_TG_MANUAL_MARK,
    REAL_TG_MUTATION_SAFE_GATE_ENV,
    REAL_TG_MUTATION_SAFE_MARK,
    REAL_TG_NEVER_MARK,
    REAL_TG_SAFE_GATE_ENV,
    REAL_TG_SAFE_MARK,
    _build_real_telegram_sandbox_config,
    _evaluate_real_tg_policy,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_TESTS_DIR = _REPO_ROOT / "tests"
_CLI_REAL_TG_DIR = _TESTS_DIR / "cli_real_tg_integration"
_RUN_CLI_HELPERS = {"run_cli", "run_cli_popen", "cli_run_direct"}
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
_MUTATION_SAFE_MARKER_USAGES = (
    "@pytest.mark.real_tg_mutation_safe",
    "pytestmark = pytest.mark.real_tg_mutation_safe",
)
_MANUAL_MARKER_USAGES = (
    "@pytest.mark.real_tg_manual",
    "pytestmark = pytest.mark.real_tg_manual",
)
_NEVER_MARKER_USAGES = (
    "@pytest.mark.real_tg_never",
    "pytestmark = pytest.mark.real_tg_never",
)
_OBSOLETE_CLI_LIVE_FIXTURE_NAMES = (
    "discover_first_channel",
    "discover_first_dialog_username",
    "discover_first_phone",
)
_LIVE_POLICY_MARKER_USAGES = (
    _SAFE_MARKER_USAGES
    + _MUTATION_SAFE_MARKER_USAGES
    + _MANUAL_MARKER_USAGES
)
_CLI_CATEGORY_REQUIRED_MARKERS = {
    "safe_ro": _SAFE_MARKER_USAGES,
    "safe_write": _SAFE_MARKER_USAGES,
    "heavy": _SAFE_MARKER_USAGES,
    "mutating": _SAFE_MARKER_USAGES,
    "mutation_safe": _MUTATION_SAFE_MARKER_USAGES,
    "destructive": _MANUAL_MARKER_USAGES,
    "manual": _MANUAL_MARKER_USAGES,
}
_AUDIT_EXCLUDED_FILES = {"test_real_telegram_policy.py"}


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


def test_real_tg_policy_rejects_safe_mode_without_fixture():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(),
        environ={REAL_TG_SAFE_GATE_ENV: "1"},
    )

    assert action == "fail"
    assert CLI_REAL_TG_LIVE_FIXTURE in message


def test_real_tg_policy_allows_cli_live_fixture():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(CLI_REAL_TG_LIVE_FIXTURE,),
        environ={REAL_TG_SAFE_GATE_ENV: "1"},
    )

    assert action is None
    assert message is None


def test_real_tg_policy_requires_live_fixture_for_mutation_safe_mode():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MUTATION_SAFE_MARK,
        fixturenames=(),
        environ={REAL_TG_MUTATION_SAFE_GATE_ENV: "1"},
    )

    assert action == "fail"
    assert REAL_TG_LIVE_FIXTURE in message


def test_real_tg_policy_rejects_manual_mode_without_fixture():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MANUAL_MARK,
        fixturenames=(),
        environ={REAL_TG_MANUAL_GATE_ENV: "1"},
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


def test_real_tg_policy_skips_mutation_safe_mode_without_gate():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MUTATION_SAFE_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
    )

    assert action == "skip"
    assert REAL_TG_MUTATION_SAFE_GATE_ENV in message


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


def test_real_tg_policy_allows_gated_mutation_safe_mode():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MUTATION_SAFE_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={REAL_TG_MUTATION_SAFE_GATE_ENV: "1"},
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
        if any(fixture in content for fixture in REAL_TG_LIVE_FIXTURES):
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
        if not any(marker in content for marker in _LIVE_POLICY_MARKER_USAGES):
            violations.append(path.name)

    assert violations == []


def _cli_live_policy_paths() -> list[Path]:
    paths = sorted(_CLI_REAL_TG_DIR.rglob("test_*.py"))
    paths.append(_CLI_REAL_TG_DIR / "conftest.py")
    return paths


def _cli_leaf_commands() -> set[tuple[str, ...]]:
    from src.cli.parser import build_parser

    leafs: set[tuple[str, ...]] = set()

    def walk(parser: argparse.ArgumentParser, prefix: tuple[str, ...]) -> None:
        subparser_actions = [
            action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
        ]
        if not subparser_actions:
            if prefix:
                leafs.add(prefix)
            return

        has_own_arguments = any(
            not isinstance(action, (argparse._HelpAction, argparse._SubParsersAction))
            for action in parser._actions
        )
        if prefix and has_own_arguments:
            leafs.add(prefix)

        for action in subparser_actions:
            for name, subparser in action.choices.items():
                walk(subparser, (*prefix, name))

    walk(build_parser(), ())
    return leafs


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return None


def _pytest_global_timeout_seconds() -> float:
    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return float(pyproject["tool"]["pytest"]["ini_options"]["timeout"])


def _has_pytest_timeout_marker(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for decorator in node.decorator_list:
        marker = decorator.func if isinstance(decorator, ast.Call) else decorator
        parts: list[str] = []
        while isinstance(marker, ast.Attribute):
            parts.append(marker.attr)
            marker = marker.value
        if isinstance(marker, ast.Name):
            parts.append(marker.id)
        dotted = ".".join(reversed(parts))
        if dotted in {"pytest.mark.timeout", "mark.timeout"}:
            return True
    return False


def _module_has_timeout_marker(tree: ast.Module) -> bool:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets):
            continue
        if "timeout" in ast.unparse(node.value):
            return True
    return False


def _live_cli_default_timeout_marker_seconds() -> float:
    return float(LIVE_CLI_DEFAULT_PYTEST_TIMEOUT_SECONDS)


def _literal_cli_calls(path: Path) -> list[tuple[str, tuple[str, ...], int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    calls: list[tuple[str, tuple[str, ...], int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        helper = _call_name(node.func)
        if helper not in _RUN_CLI_HELPERS:
            continue

        args = node.args[1:] if helper == "cli_run_direct" else node.args
        prefix: list[str] = []
        for arg in args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                prefix.append(arg.value)
                continue
            break
        calls.append((helper, tuple(prefix), node.lineno))
    return calls


def _normalize_cli_command_case(
    command: tuple[str, ...],
    leafs: set[tuple[str, ...]],
) -> tuple[str, ...] | None:
    for leaf in sorted(leafs, key=len, reverse=True):
        if command[: len(leaf)] != leaf:
            continue
        if leaf in {("channel", "refresh-meta"), ("channel", "stats")} and "--all" in command[len(leaf) :]:
            return (*leaf, "--all")
        return leaf
    return None


def _cli_real_tg_category(path: Path) -> str:
    relative = path.relative_to(_CLI_REAL_TG_DIR)
    if relative.parts == ("conftest.py",):
        return "safe_ro"
    return relative.parts[0] if len(relative.parts) > 1 else ""


def _covered_cli_leaf(command_case: tuple[str, ...], leafs: set[tuple[str, ...]]) -> tuple[str, ...] | None:
    if command_case in {("channel", "refresh-meta", "--all"), ("channel", "stats", "--all")}:
        command_case = command_case[:-1]
    return command_case if command_case in leafs else None


def test_cli_real_tg_marked_commands_are_explicitly_allowlisted():
    violations: list[str] = []
    leafs = _cli_leaf_commands()

    for path in _cli_live_policy_paths():
        content = path.read_text(encoding="utf-8")
        if path.name != "conftest.py" and not any(
            marker in content for marker in _LIVE_POLICY_MARKER_USAGES
        ):
            continue
        category = _cli_real_tg_category(path)
        allowed = CLI_REAL_TG_COMMAND_CASES_BY_CATEGORY.get(category)
        if allowed is None:
            violations.append(f"{path.relative_to(_REPO_ROOT)}: unknown CLI live category {category!r}")
            continue
        for helper, command, lineno in _literal_cli_calls(path):
            if not command:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: dynamic {helper} command")
                continue
            command_case = _normalize_cli_command_case(command, leafs)
            if command_case is None:
                violations.append(
                    f"{path.relative_to(_REPO_ROOT)}:{lineno}: {command!r} is not a parser leaf command"
                )
                continue
            if helper == "cli_run_direct":
                if command_case not in CLI_REAL_TG_CLEANUP_COMMAND_CASES:
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}:{lineno}: "
                        f"{command_case!r} is not cleanup-helper-allowlisted"
                    )
                continue
            if command_case not in allowed:
                violations.append(
                    f"{path.relative_to(_REPO_ROOT)}:{lineno}: {command_case!r} is not {category}-allowlisted"
                )

    assert violations == []


def test_cli_real_tg_folder_markers_match_risk_category():
    violations: list[str] = []

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        category = _cli_real_tg_category(path)
        required_markers = _CLI_CATEGORY_REQUIRED_MARKERS.get(category)
        if required_markers is None:
            violations.append(f"{path.relative_to(_REPO_ROOT)}: unknown CLI live category {category!r}")
            continue

        content = path.read_text(encoding="utf-8")
        if not any(marker in content for marker in required_markers):
            violations.append(f"{path.relative_to(_REPO_ROOT)}: missing expected marker for {category!r}")
            continue

        forbidden_markers = tuple(marker for marker in _LIVE_POLICY_MARKER_USAGES if marker not in required_markers)
        if any(marker in content for marker in forbidden_markers):
            violations.append(f"{path.relative_to(_REPO_ROOT)}: mixed real Telegram risk markers")

    assert violations == []


def test_cli_real_tg_inventory_uses_live_cli_runner_fixture():
    violations: list[str] = []

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        content = path.read_text(encoding="utf-8")
        if not any(marker in content for marker in _LIVE_POLICY_MARKER_USAGES):
            violations.append(f"{path.relative_to(_REPO_ROOT)}: missing real Telegram marker")
            continue
        if not _literal_cli_calls(path):
            violations.append(f"{path.relative_to(_REPO_ROOT)}: no run_cli/run_cli_popen/cli_run_direct call")

    assert violations == []


def test_cli_real_tg_inventory_does_not_reference_removed_discovery_fixtures():
    violations: list[str] = []

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        content = path.read_text(encoding="utf-8")
        for fixture_name in _OBSOLETE_CLI_LIVE_FIXTURE_NAMES:
            if fixture_name in content:
                violations.append(f"{path.relative_to(_REPO_ROOT)}: {fixture_name}")

    assert violations == []


def test_cli_real_tg_inventory_does_not_disable_all_failure_text_checks():
    violations: list[str] = []

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg != "allow_error_text":
                    continue
                if isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{node.lineno}")

    assert violations == []


def test_cli_real_tg_conftest_adds_default_pytest_timeout_for_live_cli_tests():
    assert _live_cli_default_timeout_marker_seconds() > RUN_CLI_DEFAULT_TIMEOUT_SECONDS
    assert _live_cli_default_timeout_marker_seconds() > _pytest_global_timeout_seconds()


def test_cli_real_tg_subprocess_timeouts_have_pytest_timeout_marker():
    global_timeout = _pytest_global_timeout_seconds()
    default_marker_timeout = _live_cli_default_timeout_marker_seconds()
    violations: list[str] = []

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_has_timeout = _module_has_timeout_marker(tree)
        test_functions = (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")
        )
        for test_function in test_functions:
            if module_has_timeout or _has_pytest_timeout_marker(test_function):
                continue
            for call in ast.walk(test_function):
                if not isinstance(call, ast.Call):
                    continue
                helper = _call_name(call.func)
                if helper not in _RUN_CLI_HELPERS:
                    continue
                timeout = None
                for keyword in call.keywords:
                    if keyword.arg != "timeout":
                        continue
                    if not isinstance(keyword.value, ast.Constant) or not isinstance(keyword.value.value, (int, float)):
                        continue
                    timeout = float(keyword.value.value)
                if timeout is None and helper == "run_cli":
                    timeout = float(RUN_CLI_DEFAULT_TIMEOUT_SECONDS)
                if timeout is None:
                    continue
                if timeout > global_timeout and default_marker_timeout <= timeout:
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}:{call.lineno}: "
                        f"{helper} timeout={timeout:g} exceeds pytest timeout={global_timeout:g} "
                        "without @pytest.mark.timeout or live CLI default timeout marker"
                    )

    assert violations == []


def test_cli_assert_ok_allows_only_named_failure_texts():
    allowed_result = subprocess.CompletedProcess(
        args=("src.main", "scheduler", "trigger"),
        returncode=0,
        stdout="No connected accounts.",
        stderr="",
    )
    _assert_cli_result_ok(allowed_result, allow_error_text=("No connected accounts",))

    mixed_failure_result = subprocess.CompletedProcess(
        args=("src.main", "scheduler", "trigger"),
        returncode=0,
        stdout="No connected accounts.\nTraceback (most recent call last):",
        stderr="",
    )
    with pytest.raises(pytest.fail.Exception, match="Traceback"):
        _assert_cli_result_ok(
            mixed_failure_result,
            allow_error_text=("No connected accounts",),
        )


def test_cli_real_tg_live_dotenv_is_loaded_from_config_root(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_API_ID", "999999")
    monkeypatch.delenv("TG_API_HASH", raising=False)

    live_root = tmp_path
    (live_root / ".env").write_text(
        "TG_API_ID=123456\nTG_API_HASH=abcdef0123456789abcdef0123456789\n",
        encoding="utf-8",
    )
    config_path = live_root / "config.yaml"
    config_path.write_text(
        """
telegram:
  api_id: ${TG_API_ID}
  api_hash: ${TG_API_HASH}
database:
  path: data/tg_search.db
""",
        encoding="utf-8",
    )

    load_cli_dotenv(config_path)
    config = load_config(config_path)

    assert config.telegram.api_id == 999999
    assert config.telegram.api_hash == "abcdef0123456789abcdef0123456789"


def test_cli_entrypoint_loads_dotenv_from_config_root(tmp_path, monkeypatch):
    source_root = _REPO_ROOT
    live_root = tmp_path / "live"
    unrelated_cwd = tmp_path / "cwd"
    live_root.mkdir()
    unrelated_cwd.mkdir()
    db_path = live_root / "cli-dotenv-test.db"
    config_path = live_root / "config.yaml"

    (live_root / ".env").write_text(
        f"CLI_DOTENV_TEST_DB={db_path}\n",
        encoding="utf-8",
    )
    config_path.write_text(
        """
database:
  path: ${CLI_DOTENV_TEST_DB}
""",
        encoding="utf-8",
    )
    monkeypatch.delenv("CLI_DOTENV_TEST_DB", raising=False)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.main",
            "--config",
            str(config_path),
            "settings",
            "info",
        ],
        cwd=unrelated_cwd,
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(source_root),
            "PYTHONSAFEPATH": "1",
        },
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert db_path.exists()
    assert not (unrelated_cwd / "data" / "tg_search.db").exists()


def test_cli_real_tg_parser_leaf_commands_are_covered_or_manifested():
    leafs = _cli_leaf_commands()
    covered: set[tuple[str, ...]] = set()
    violations: list[str] = []
    manifested = set(CLI_REAL_TG_MANUAL_OR_EXCLUDED_COMMANDS)

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        for helper, command, lineno in _literal_cli_calls(path):
            if helper == "cli_run_direct":
                continue
            if not command:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: dynamic CLI command")
                continue
            command_case = _normalize_cli_command_case(command, leafs)
            if command_case is None:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: unknown CLI command {command!r}")
                continue
            covered_leaf = _covered_cli_leaf(command_case, leafs)
            if covered_leaf is not None:
                covered.add(covered_leaf)
                if covered_leaf in manifested:
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}:{lineno}: "
                        f"{covered_leaf!r} is in CLI_REAL_TG_MANUAL_OR_EXCLUDED_COMMANDS"
                    )

    missing = sorted(leafs - covered - manifested)
    stale_manifest = sorted(manifested - leafs)

    assert violations == []
    assert missing == []
    assert stale_manifest == []


def test_cli_real_tg_tests_use_live_fixture_and_real_config_contract():
    content = (_CLI_REAL_TG_DIR / "conftest.py").read_text(encoding="utf-8")

    assert CLI_REAL_TG_LIVE_FIXTURE in content
    assert '"--config"' in content
    assert "RUN_CLI_REAL_TG_LIVE" in content
    assert "CLI_REAL_TG_CONFIG" in content
    assert "load_config(config_path)" in content
    assert "config.database.path" in content
    assert "tmp_path_factory" not in content
    assert "_build_real_telegram_sandbox_config" not in content
    assert "REAL_TG_SESSION" not in content
