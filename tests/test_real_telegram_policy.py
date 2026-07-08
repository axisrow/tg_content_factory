from __future__ import annotations

import ast
import functools
import json
import os
import sqlite3
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
    CLI_REAL_TG_PHONE_ENV,
    LIVE_CLI_DEFAULT_PYTEST_TIMEOUT_SECONDS,
    RUN_CLI_DEFAULT_TIMEOUT_SECONDS,
    CliRealCliEnv,
    LiveCliAccountReadinessError,
    LiveCliAccountWaitTimeoutError,
    _assert_cli_result_ok,
    _fetch_live_accounts,
    _first_premium_phone,
    account_info_probe_failure,
    wait_for_phone_flood_clear,
    wait_for_ready_live_cli_accounts,
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
_OBSOLETE_MUTATION_SAFE_TARGET_ENV_NAMES = (
    "CLI_REAL_TG_ARCHIVE_CHAT_ID",
    "CLI_REAL_TG_ARCHIVE_PHONE",
    "CLI_REAL_TG_MARK_READ_CHAT_ID",
    "CLI_REAL_TG_MARK_READ_MAX_ID",
    "CLI_REAL_TG_MARK_READ_PHONE",
    "CLI_REAL_TG_PIN_CHAT_ID",
    "CLI_REAL_TG_PIN_MESSAGE_ID",
    "CLI_REAL_TG_PIN_PHONE",
    "CLI_REAL_TG_REACT_CHAT_ID",
    "CLI_REAL_TG_REACT_MESSAGE_ID",
    "CLI_REAL_TG_REACT_PHONE",
    "CLI_REAL_TG_UNARCHIVE_CHAT_ID",
    "CLI_REAL_TG_UNARCHIVE_PHONE",
    "CLI_REAL_TG_UNPIN_CHAT_ID",
    "CLI_REAL_TG_UNPIN_MESSAGE_ID",
    "CLI_REAL_TG_UNPIN_PHONE",
)
_OBSOLETE_MUTATION_SAFE_TARGET_FIXTURE_NAMES = (
    "live_mutation_dialog",
    "live_mutation_message",
    "live_owned_mutation_message",
    "live_pin_mutation_message",
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
    "process_control": _MANUAL_MARKER_USAGES,
    "manual": _MANUAL_MARKER_USAGES,
    "dangerous": _NEVER_MARKER_USAGES,
}
_CLI_CLEANUP_COMMAND_PRODUCERS = {
    ("agent", "thread-delete"): {("agent", "chat")},
    ("agent", "threads"): {("agent", "chat")},
    ("dialogs", "archive"): {("dialogs", "unarchive")},
    ("dialogs", "delete-message"): {
        ("dialogs", "send"),
        ("dialogs", "forward"),
        ("photo-loader", "send"),
        ("pipeline", "publish"),
    },
    ("dialogs", "edit-message"): {("dialogs", "edit-message")},
    ("dialogs", "leave"): {("dialogs", "create-channel"), ("dialogs", "create-group")},
    ("dialogs", "pin-message"): {("dialogs", "unpin-message")},
    ("dialogs", "react"): {("dialogs", "react")},
    ("dialogs", "unarchive"): {("dialogs", "archive")},
    ("dialogs", "unpin-message"): {("dialogs", "pin-message")},
    ("photo-loader", "batch-cancel"): {("photo-loader", "schedule-send")},
    ("scheduler", "clear-pending"): {("collect",), ("scheduler", "trigger")},
    ("search",): {("search",)},
}
_AUDIT_EXCLUDED_FILES = {"test_real_telegram_policy.py"}


def _live_cli_probe_env(tmp_path: Path) -> CliRealCliEnv:
    return CliRealCliEnv(
        source_root=_REPO_ROOT,
        live_root=tmp_path,
        config_path=tmp_path / "config.yaml",
        db_path=tmp_path / "data.db",
        web_port=8080,
        phones=(),
        channel_pk=None,
        channel_id=None,
        channel_username=None,
    )


def _create_live_accounts_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                phone TEXT,
                session_string TEXT,
                is_active INTEGER,
                is_primary INTEGER,
                flood_wait_until TEXT
            )
            """
        )


def test_fetch_live_accounts_prefers_accounts_not_in_flood_wait(tmp_path, monkeypatch):
    monkeypatch.delenv(CLI_REAL_TG_PHONE_ENV, raising=False)
    db_path = tmp_path / "live.db"
    _create_live_accounts_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO accounts (id, phone, session_string, is_active, is_primary, flood_wait_until)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (1, "+primary-flooded", "session", 1, 1, "2099-01-01T00:00:00+00:00"),
                (2, "+secondary-ready", "session", 1, 0, None),
                (3, "+primary-ready", "session", 1, 1, "2000-01-01T00:00:00+00:00"),
            ),
        )

    assert _fetch_live_accounts(db_path) == (
        "+primary-ready",
        "+secondary-ready",
        "+primary-flooded",
    )


def test_fetch_live_accounts_can_pin_requested_phone(tmp_path, monkeypatch):
    db_path = tmp_path / "live.db"
    _create_live_accounts_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO accounts (id, phone, session_string, is_active, is_primary, flood_wait_until)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (1, "+old", "session", 1, 1, "2099-01-01T00:00:00+00:00"),
                (2, "+fresh", "session", 1, 0, None),
            ),
        )

    monkeypatch.setenv(CLI_REAL_TG_PHONE_ENV, "+fresh")

    assert _fetch_live_accounts(db_path) == ("+fresh",)


def _create_premium_accounts_db(path: Path, rows: tuple) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                phone TEXT,
                session_string TEXT,
                is_active INTEGER,
                is_primary INTEGER,
                is_premium INTEGER,
                flood_wait_until TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO accounts "
            "(id, phone, session_string, is_active, is_primary, is_premium, flood_wait_until) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def test_first_premium_phone_returns_connected_premium(tmp_path):
    db_path = tmp_path / "live.db"
    _create_premium_accounts_db(
        db_path,
        (
            (1, "+nonpremium", "s", 1, 1, 0, None),
            (2, "+premium", "s", 1, 0, 1, None),
        ),
    )

    assert _first_premium_phone(db_path, ("+nonpremium", "+premium")) == "+premium"


def test_first_premium_phone_none_when_no_premium(tmp_path):
    db_path = tmp_path / "live.db"
    _create_premium_accounts_db(db_path, ((1, "+nonpremium", "s", 1, 1, 0, None),))

    assert _first_premium_phone(db_path, ("+nonpremium",)) is None


def test_first_premium_phone_skips_unconnected_premium(tmp_path):
    db_path = tmp_path / "live.db"
    _create_premium_accounts_db(db_path, ((1, "+premium", "s", 1, 1, 1, None),))

    # Premium exists in DB but the live env never connected it.
    assert _first_premium_phone(db_path, ("+other",)) is None


def test_first_premium_phone_missing_db_returns_none(tmp_path):
    assert _first_premium_phone(tmp_path / "nope.db", ("+x",)) is None


def test_wait_for_phone_flood_clear_returns_true_when_clear(tmp_path):
    db_path = tmp_path / "live.db"
    _create_live_accounts_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO accounts (id, phone, session_string, is_active, is_primary, flood_wait_until)"
            " VALUES (1, '+p', 's', 1, 1, NULL)"
        )

    assert wait_for_phone_flood_clear(db_path, "+p", timeout=1, poll=0.1, sleep=lambda _s: None)


def test_wait_for_phone_flood_clear_polls_until_expiry():
    # remaining() reports flood for two polls, then clears; sleep is captured, not real.
    remaining_values = iter([30.0, 10.0, 0.0])
    slept: list[float] = []
    now = [0.0]

    def remaining(_db, _phone):
        return next(remaining_values)

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    ok = wait_for_phone_flood_clear(
        Path("ignored.db"),
        "+p",
        timeout=600,
        poll=5,
        monotonic=lambda: now[0],
        sleep=sleep,
        remaining=remaining,
    )

    assert ok is True
    assert slept == [5, 5]  # poll-capped waits while flood remained


def test_wait_for_phone_flood_clear_times_out():
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    ok = wait_for_phone_flood_clear(
        Path("ignored.db"),
        "+p",
        timeout=10,
        poll=5,
        monotonic=lambda: now[0],
        sleep=sleep,
        remaining=lambda _db, _phone: 999.0,  # flood far longer than timeout
    )

    assert ok is False


def test_live_cli_account_readiness_retries_until_active_account_appears(tmp_path: Path):
    cli_env = _live_cli_probe_env(tmp_path)
    now = 0.0
    sleep_calls: list[float] = []
    fetch_calls = 0
    probe_commands: list[tuple[str, ...]] = []

    def monotonic() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        sleep_calls.append(seconds)
        now += seconds

    def fetch_accounts(_db_path: Path) -> tuple[str, ...]:
        nonlocal fetch_calls
        fetch_calls += 1
        return () if fetch_calls == 1 else ("+123",)

    def runner(args, **_kwargs) -> subprocess.CompletedProcess:
        probe_commands.append(tuple(args))
        return subprocess.CompletedProcess(args, 0, stdout="Live Telegram accounts (1):\n- +123: Test\n", stderr="")

    phones = wait_for_ready_live_cli_accounts(
        cli_env,
        wait_seconds=5,
        poll_seconds=2,
        probe_timeout_seconds=3,
        fetch_accounts=fetch_accounts,
        runner=runner,
        monotonic=monotonic,
        sleep=sleep,
    )

    assert phones == ("+123",)
    assert sleep_calls == [2]
    assert probe_commands
    assert probe_commands[0][-4:] == ("account", "info", "--phone", "+123")


def test_live_cli_account_readiness_caps_probe_timeout_to_remaining_wait(tmp_path: Path):
    cli_env = _live_cli_probe_env(tmp_path)
    now = 0.0
    probe_phones: list[str] = []
    probe_timeouts: list[float] = []
    sleep_calls: list[float] = []

    def monotonic() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        sleep_calls.append(seconds)
        now += seconds

    def runner(args, **kwargs) -> subprocess.CompletedProcess:
        nonlocal now
        probe_phones.append(args[-1])
        probe_timeouts.append(kwargs["timeout"])
        now += kwargs["timeout"]
        return subprocess.CompletedProcess(args, 1, stdout="", stderr="not ready")

    with pytest.raises(LiveCliAccountReadinessError):
        wait_for_ready_live_cli_accounts(
            cli_env,
            wait_seconds=2,
            poll_seconds=1,
            probe_timeout_seconds=60,
            fetch_accounts=lambda _db_path: ("+111", "+222"),
            runner=runner,
            monotonic=monotonic,
            sleep=sleep,
        )

    assert probe_phones == ["+111"]
    assert probe_timeouts == [2]
    assert sleep_calls == []


def test_live_cli_account_readiness_times_out_without_active_account(tmp_path: Path):
    cli_env = _live_cli_probe_env(tmp_path)

    with pytest.raises(LiveCliAccountWaitTimeoutError, match="no active connected Telegram accounts"):
        wait_for_ready_live_cli_accounts(
            cli_env,
            wait_seconds=0,
            fetch_accounts=lambda _db_path: (),
            runner=lambda args, **_kwargs: subprocess.CompletedProcess(args, 0, stdout="", stderr=""),
        )


def test_live_cli_account_readiness_fails_on_zero_exit_no_account_probe(tmp_path: Path):
    cli_env = _live_cli_probe_env(tmp_path)
    now = 0.0

    def monotonic() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    def runner(args, **_kwargs) -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(
            args,
            0,
            stdout="Live Telegram accounts not found for this request: не найдены.\n",
            stderr="",
        )

    with pytest.raises(LiveCliAccountReadinessError, match="Live Telegram accounts not found"):
        wait_for_ready_live_cli_accounts(
            cli_env,
            wait_seconds=1,
            poll_seconds=1,
            fetch_accounts=lambda _db_path: ("+123",),
            runner=runner,
            monotonic=monotonic,
            sleep=sleep,
        )


def test_live_cli_account_readiness_timeout_reports_probe_streams(tmp_path: Path):
    cli_env = _live_cli_probe_env(tmp_path)
    now = 0.0

    def monotonic() -> float:
        return now

    def runner(args, **kwargs) -> subprocess.CompletedProcess:
        nonlocal now
        now += kwargs["timeout"]
        raise subprocess.TimeoutExpired(
            args,
            kwargs["timeout"],
            output="",
            stderr="PermissionError: [Errno 1] Operation not permitted",
        )

    with pytest.raises(LiveCliAccountReadinessError) as exc_info:
        wait_for_ready_live_cli_accounts(
            cli_env,
            wait_seconds=1,
            poll_seconds=1,
            fetch_accounts=lambda _db_path: ("+123",),
            runner=runner,
            monotonic=monotonic,
            sleep=lambda _seconds: None,
        )

    message = str(exc_info.value)
    assert "timed out after 1s" in message
    assert "Operation not permitted" in message


def test_account_info_probe_requires_requested_phone_in_stdout():
    result = subprocess.CompletedProcess(
        ("account", "info"),
        0,
        stdout="Live Telegram accounts (1):\n- +999: Other\n",
        stderr="",
    )

    assert "`account info --phone +123` did not confirm" in account_info_probe_failure("+123", result)


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
        gate_enabled=lambda name, environ: False,
    )

    assert action == "skip"
    assert REAL_TG_SAFE_GATE_ENV in message


def test_real_tg_policy_skips_mutation_safe_mode_without_gate():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MUTATION_SAFE_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
        gate_enabled=lambda name, environ: False,
    )

    assert action == "skip"
    assert REAL_TG_MUTATION_SAFE_GATE_ENV in message


def test_real_tg_policy_skips_manual_mode_without_gate():
    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_MANUAL_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
        gate_enabled=lambda name, environ: False,
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


# Script body run in a clean subprocess to enumerate the CLI leaf set (#1258).
# Kept as a module string so both the subprocess probe and the in-process walk
# below share one source of truth for the leaf-derivation rule. It prints the
# leaf tuples as JSON on stdout; anything else (imports/diagnostics) goes to
# stderr so it never pollutes the JSON payload.
#
# **Version-robust walk (#1258 root cause).** ``typer.main.get_command(app)``
# returns a ``typer.core.TyperGroup`` whose base class *changed* between typer
# releases: on typer 0.25.x it subclassed the installed ``click.Group`` (so
# ``isinstance(cmd, click.Group)`` was True), but typer 0.26+ vendors its own
# click fork — ``TyperGroup`` now derives from ``typer._click.core.Command`` and
# ``isinstance(cmd, click.Group)`` / ``isinstance(param, click.Option)`` are both
# **False**. An ``isinstance``-based walk therefore silently produced an *empty*
# leaf set on typer 0.26.8 (CI) while yielding 215 leaves on typer 0.25.1 (local)
# — a "CI-only flake" that was really a deterministic version mismatch, not an
# xdist race. So the walk uses **duck typing** that holds across both typer
# lines: a node is a group iff it exposes a ``.commands`` mapping; a group is a
# runnable leaf iff it is ``invoke_without_command`` and owns a non-help option
# (detected via ``param_type_name == "option"``, which both click and typer's
# vendored option expose). The probe asserts the leaf set is non-empty so any
# future typer packaging change fails loud with the observed typer version
# instead of degrading into hundreds of spurious "not a parser leaf" errors.
_CLI_LEAF_PROBE_SCRIPT = """
import json
import sys

import typer

from src.cli.typer_commands import app

leafs = []


def _is_group(command):
    # typer 0.26+ vendors click, so ``isinstance(command, click.Group)`` is
    # unreliable; a group is anything exposing a ``.commands`` mapping.
    return getattr(command, "commands", None) is not None


def _own_non_help_options(command):
    # A group's "own" option (beyond the implicit --help) marks it as a
    # directly-runnable leaf. ``param_type_name`` is exposed by both the
    # installed click Option and typer's vendored TyperOption.
    return [
        param
        for param in getattr(command, "params", [])
        if getattr(param, "param_type_name", None) == "option" and param.name != "help"
    ]


def walk(command, prefix):
    if _is_group(command):
        if prefix and _own_non_help_options(command) and getattr(command, "invoke_without_command", False):
            leafs.append(list(prefix))
        for name, subcommand in command.commands.items():
            walk(subcommand, (*prefix, name))
    elif prefix:
        leafs.append(list(prefix))


walk(typer.main.get_command(app), ())

if not leafs:
    raise SystemExit(
        "CLI leaf walk produced an EMPTY leaf set — the walk is incompatible "
        "with the installed typer "
        + getattr(typer, "__version__", "?")
        + " (get_command(app) tree shape changed). Update _CLI_LEAF_PROBE_SCRIPT."
    )

json.dump(leafs, sys.stdout)
"""


@functools.lru_cache(maxsize=1)
def _cli_leaf_commands() -> frozenset[tuple[str, ...]]:
    """Leaf-command paths of the CLI, derived from the Typer app (#1125 Final).

    Since the argparse framework was removed in #1125, the Typer ``app`` is the
    single source of truth for the CLI surface. We walk the Click command tree
    that ``typer.main.get_command(app)`` produces and reproduce the exact leaf
    set the old argparse walker yielded:

    * a plain (non-group) command is always a leaf;
    * a group is *also* a leaf when it is directly invokable — i.e. it has its
      own parameters beyond the implicit ``--help`` *and* ``invoke_without_command``
      is set (the Typer ``@group.callback(invoke_without_command=True)`` pattern,
      e.g. ``collect`` / ``channel tag``). This mirrors argparse's "a sub-parser
      with its own non-help arguments is itself a runnable leaf".

    The empty root prefix is never recorded. Verified to yield the identical
    set of 212 leaf tuples the argparse ``build_parser()`` walker produced.

    **Why a clean subprocess (#1258).** ``src.cli.typer_commands.app`` is the
    empty ``typer.Typer()`` singleton from ``typer_app``; every command is
    registered by a ``@app.command()`` / ``add_typer`` decorator that runs while
    ``src.cli.typer_commands`` is *imported*. Deriving the leaf set in a fresh
    interpreter — which imports ``typer_commands`` to completion before walking —
    keeps this immune to any in-process import ordering and lets the result be
    memoised for the whole session (~1.3s, once).

    **The real #1258 root cause was a typer-version mismatch, not an xdist
    race.** The walk originally keyed on ``isinstance(cmd, click.Group)``. typer
    0.26+ vendors its own click fork, so ``TyperGroup`` no longer subclasses the
    installed ``click.Group`` and that ``isinstance`` returned False — yielding an
    *empty* leaf set on the CI typer (0.26.8) while local typer (0.25.1) still
    produced 215 leaves. That looked like a "CI-only flake" but was a deterministic
    environment difference. ``_CLI_LEAF_PROBE_SCRIPT`` now walks by duck typing so
    it is robust across typer lines, and raises loudly if the leaf set is empty.
    """
    proc = subprocess.run(
        [sys.executable, "-c", _CLI_LEAF_PROBE_SCRIPT],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        # The probe exits non-zero (SystemExit) when the walk is incompatible
        # with the installed typer, i.e. produced an empty leaf set. Surface its
        # stderr so the failing policy test names the real cause and typer
        # version instead of hundreds of spurious "not a parser leaf" errors.
        raise AssertionError(
            f"CLI leaf probe subprocess failed (rc={proc.returncode}).\n"
            f"stderr:\n{proc.stderr}\nstdout:\n{proc.stdout}"
        )
    return frozenset(tuple(leaf) for leaf in json.loads(proc.stdout))


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return None


def _pytest_global_timeout_seconds() -> float:
    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return float(pyproject["tool"]["pytest"]["ini_options"]["timeout"])


def _pytest_timeout_marker_seconds(node: ast.AST) -> float | None:
    if isinstance(node, ast.Call):
        marker = node.func
        timeout_args = node.args
        timeout_keywords = node.keywords
    else:
        marker = node
        timeout_args = []
        timeout_keywords = []
    parts: list[str] = []
    while isinstance(marker, ast.Attribute):
        parts.append(marker.attr)
        marker = marker.value
    if isinstance(marker, ast.Name):
        parts.append(marker.id)
    dotted = ".".join(reversed(parts))
    if dotted not in {"pytest.mark.timeout", "mark.timeout"}:
        return None
    for keyword in timeout_keywords:
        if keyword.arg != "timeout":
            continue
        if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, (int, float)):
            return float(keyword.value.value)
        return 0.0
    if not timeout_args:
        return 0.0
    value = timeout_args[0]
    if isinstance(value, ast.Constant) and isinstance(value.value, (int, float)):
        return float(value.value)
    return 0.0


def _function_timeout_marker_seconds(node: ast.FunctionDef | ast.AsyncFunctionDef) -> float | None:
    for decorator in node.decorator_list:
        timeout = _pytest_timeout_marker_seconds(decorator)
        if timeout is not None:
            return timeout
    return None


def _pytestmark_timeout_seconds(value: ast.AST) -> float | None:
    timeout = _pytest_timeout_marker_seconds(value)
    if timeout is not None:
        return timeout
    if isinstance(value, (ast.List, ast.Tuple)):
        for item in value.elts:
            timeout = _pytest_timeout_marker_seconds(item)
            if timeout is not None:
                return timeout
    return None


def _module_timeout_marker_seconds(tree: ast.Module) -> float | None:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets):
            continue
        timeout = _pytestmark_timeout_seconds(node.value)
        if timeout is not None:
            return timeout
    return None


def _has_pytest_timeout_marker(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return _function_timeout_marker_seconds(node) is not None


def _module_has_timeout_marker(tree: ast.Module) -> bool:
    return _module_timeout_marker_seconds(tree) is not None


def _live_cli_default_timeout_marker_seconds() -> float:
    return float(LIVE_CLI_DEFAULT_PYTEST_TIMEOUT_SECONDS)


def _ast_parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    return {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }


def _is_inside_finally(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current = node
    while current in parents:
        parent = parents[current]
        if isinstance(parent, ast.Try) and current in parent.finalbody:
            return True
        current = parent
    return False


def _literal_cli_call_records(path: Path) -> list[tuple[str, tuple[str, ...], int, bool]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parents = _ast_parent_map(tree)
    calls: list[tuple[str, tuple[str, ...], int, bool]] = []
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
        calls.append((helper, tuple(prefix), node.lineno, _is_inside_finally(node, parents)))
    return calls


def _literal_cli_call_records_by_test(path: Path) -> dict[str, list[tuple[str, tuple[str, ...], int, bool]]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parents = _ast_parent_map(tree)
    records: dict[str, list[tuple[str, tuple[str, ...], int, bool]]] = {}
    for fn in tree.body:
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) or not fn.name.startswith("test_"):
            continue
        calls: list[tuple[str, tuple[str, ...], int, bool]] = []
        for node in ast.walk(fn):
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
            calls.append((helper, tuple(prefix), node.lineno, _is_inside_finally(node, parents)))
        records[fn.name] = calls
    return records


def _literal_cli_string_arg_records(path: Path) -> list[tuple[str, tuple[str, ...], tuple[str, ...], int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    records: list[tuple[str, tuple[str, ...], tuple[str, ...], int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        helper = _call_name(node.func)
        if helper not in _RUN_CLI_HELPERS:
            continue
        args = node.args[1:] if helper == "cli_run_direct" else node.args
        prefix: list[str] = []
        strings: list[str] = []
        prefix_open = True
        for arg in args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                strings.append(arg.value)
                if prefix_open:
                    prefix.append(arg.value)
                continue
            prefix_open = False
        records.append((helper, tuple(prefix), tuple(strings), node.lineno))
    return records


def _literal_cli_calls(path: Path) -> list[tuple[str, tuple[str, ...], int]]:
    return [
        (helper, command, lineno)
        for helper, command, lineno, _in_finally in _literal_cli_call_records(path)
    ]


def _literal_cli_call_groups(path: Path) -> list[list[tuple[str, tuple[str, ...], int, bool]]]:
    if path.name == "conftest.py":
        return [_literal_cli_call_records(path)]
    return list(_literal_cli_call_records_by_test(path).values())


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


def _has_cleanup_producer(
    cleanup_command_case: tuple[str, ...],
    records: list[tuple[str, tuple[str, ...], int, bool]],
    leafs: set[tuple[str, ...]],
    cleanup_lineno: int,
) -> bool:
    for helper, command, lineno, _in_finally in records:
        if helper == "cli_run_direct":
            continue
        if lineno >= cleanup_lineno:
            continue
        command_case = _normalize_cli_command_case(command, leafs)
        if _is_cleanup_producer_command(cleanup_command_case, command, command_case):
            return True
    return False


def _is_cli_help_command(command: tuple[str, ...]) -> bool:
    return "-h" in command or "--help" in command


def _is_cleanup_producer_command(
    cleanup_command_case: tuple[str, ...],
    command: tuple[str, ...],
    command_case: tuple[str, ...] | None,
) -> bool:
    if command_case is None or _is_cli_help_command(command):
        return False

    if cleanup_command_case == ("scheduler", "clear-pending"):
        return command in {("collect",), ("scheduler", "trigger")}

    if cleanup_command_case in {("agent", "thread-delete"), ("agent", "threads")}:
        return command_case == ("agent", "chat") and any(arg in {"-p", "--prompt"} for arg in command[2:])

    return command_case in _CLI_CLEANUP_COMMAND_PRODUCERS.get(cleanup_command_case, set())


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
        for records in _literal_cli_call_groups(path):
            for helper, command, lineno, in_finally in records:
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
                    if not in_finally:
                        violations.append(
                            f"{path.relative_to(_REPO_ROOT)}:{lineno}: "
                            f"{command_case!r} cleanup helper call is not inside a finally block"
                        )
                    if not _has_cleanup_producer(command_case, records, leafs, lineno):
                        violations.append(
                            f"{path.relative_to(_REPO_ROOT)}:{lineno}: "
                            f"{command_case!r} cleanup helper call has no producer command in the same test"
                        )
                    continue
                if command_case not in allowed:
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}:{lineno}: {command_case!r} is not {category}-allowlisted"
                    )

    assert violations == []


def test_cli_run_direct_cleanup_policy_requires_finally_context(tmp_path):
    sample = tmp_path / "test_sample.py"
    sample.write_text(
        """
import pytest

pytestmark = pytest.mark.real_tg_safe

def test_bad_no_finally(cli_env):
    cli_run_direct(cli_env, "scheduler", "clear-pending")

def test_bad_no_producer(cli_env):
    try:
        pass
    finally:
        cli_run_direct(cli_env, "scheduler", "clear-pending")

def test_bad_help_is_not_producer(run_cli, cli_env):
    try:
        run_cli("collect", "--help")
    finally:
        cli_run_direct(cli_env, "scheduler", "clear-pending")

def test_good(run_cli, cli_env):
    try:
        run_cli("scheduler", "trigger")
    finally:
        cli_run_direct(cli_env, "scheduler", "clear-pending")

def test_bad_agent_help_is_not_producer(run_cli, cli_env):
    try:
        run_cli("agent", "chat", "--help")
    finally:
        cli_run_direct(cli_env, "agent", "thread-delete", "--thread-id", "1")

def test_good_agent_prompt(run_cli, cli_env):
    try:
        run_cli("agent", "chat", "-p", "ok")
    finally:
        cli_run_direct(cli_env, "agent", "thread-delete", "--thread-id", "1")
""",
        encoding="utf-8",
    )

    leafs = _cli_leaf_commands()
    records = _literal_cli_call_records_by_test(sample)

    assert records["test_bad_no_finally"][0][3] is False
    assert records["test_bad_no_producer"][0][3] is True
    assert not _has_cleanup_producer(
        ("scheduler", "clear-pending"),
        records["test_bad_no_producer"],
        leafs,
        records["test_bad_no_producer"][0][2],
    )
    assert not _has_cleanup_producer(
        ("scheduler", "clear-pending"),
        records["test_bad_help_is_not_producer"],
        leafs,
        records["test_bad_help_is_not_producer"][1][2],
    )
    assert _has_cleanup_producer(
        ("scheduler", "clear-pending"),
        records["test_good"],
        leafs,
        records["test_good"][1][2],
    )
    assert not _has_cleanup_producer(
        ("agent", "thread-delete"),
        records["test_bad_agent_help_is_not_producer"],
        leafs,
        records["test_bad_agent_help_is_not_producer"][1][2],
    )
    assert _has_cleanup_producer(
        ("agent", "thread-delete"),
        records["test_good_agent_prompt"],
        leafs,
        records["test_good_agent_prompt"][1][2],
    )


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
        if any(marker in content for marker in _NEVER_MARKER_USAGES):
            continue
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


def test_cli_real_tg_mutation_safe_inventory_discovers_live_targets():
    violations: list[str] = []
    mutation_safe_dir = _CLI_REAL_TG_DIR / "mutation_safe"

    for path in sorted(mutation_safe_dir.rglob("test_*.py")):
        content = path.read_text(encoding="utf-8")
        if "required_env(" in content or "mutation_safe.env" in content:
            violations.append(f"{path.relative_to(_REPO_ROOT)}: uses required mutation target env helper")
        for env_name in _OBSOLETE_MUTATION_SAFE_TARGET_ENV_NAMES:
            if env_name in content:
                violations.append(f"{path.relative_to(_REPO_ROOT)}: {env_name}")
        for fixture_name in _OBSOLETE_MUTATION_SAFE_TARGET_FIXTURE_NAMES:
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


def test_cli_real_tg_inventory_does_not_use_help_as_leaf_smoke():
    violations: list[str] = []

    for path in sorted(_CLI_REAL_TG_DIR.rglob("test_*.py")):
        for helper, command, lineno in _literal_cli_calls(path):
            if _is_cli_help_command(command):
                violations.append(
                    f"{path.relative_to(_REPO_ROOT)}:{lineno}: "
                    f"{helper} {command!r} uses CLI help instead of exercising the leaf command"
                )

    assert violations == []


def test_cli_real_tg_mutation_safe_commands_are_bounded():
    violations: list[str] = []
    leafs = _cli_leaf_commands()
    mutation_safe_dir = _CLI_REAL_TG_DIR / "mutation_safe"

    for path in sorted(mutation_safe_dir.rglob("test_*.py")):
        for _helper, prefix, strings, lineno in _literal_cli_string_arg_records(path):
            command_case = _normalize_cli_command_case(prefix, leafs)
            if command_case is None:
                continue
            if command_case == ("dialogs", "mark-read"):
                if "--max-id" not in strings:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: mark-read must set --max-id")
            if command_case == ("dialogs", "react"):
                if "--yes" not in strings:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: react must be noninteractive")
            if command_case in {
                ("dialogs", "delete-message"),
                ("dialogs", "edit-message"),
                ("dialogs", "forward"),
                ("dialogs", "send"),
            }:
                if "--yes" not in strings:
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}:{lineno}: scratch-message command must be noninteractive"
                    )
            if command_case == ("dialogs", "pin-message"):
                if "--notify" in strings:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: mutation-safe pin must not notify")
                if "--yes" not in strings:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: pin-message must be noninteractive")
            if command_case == ("dialogs", "unpin-message"):
                if "--message-id" not in strings:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: unpin-message must set --message-id")
                if "--yes" not in strings:
                    violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: unpin-message must be noninteractive")
            if command_case in {
                ("dialogs", "archive"),
                ("dialogs", "delete-message"),
                ("dialogs", "edit-message"),
                ("dialogs", "forward"),
                ("dialogs", "mark-read"),
                ("dialogs", "participants"),
                ("dialogs", "pin-message"),
                ("dialogs", "react"),
                ("dialogs", "send"),
                ("dialogs", "unarchive"),
                ("dialogs", "unpin-message"),
                ("photo-loader", "schedule-send"),
                ("photo-loader", "send"),
            } and "--phone" not in strings:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: mutation-safe command must pin --phone")
            if command_case == ("notification", "test") and "--message" not in strings:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: notification test must set --message")
            if command_case == ("photo-loader", "run-due") and "--item-id" not in strings:
                violations.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: run-due must set --item-id")

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
        module_timeout = _module_timeout_marker_seconds(tree)
        test_functions = (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")
        )
        for test_function in test_functions:
            function_timeout = _function_timeout_marker_seconds(test_function)
            if function_timeout is not None:
                pytest_timeout = function_timeout
            elif module_timeout is not None:
                pytest_timeout = module_timeout
            else:
                pytest_timeout = default_marker_timeout
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
                if timeout > global_timeout and pytest_timeout <= timeout:
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}:{call.lineno}: "
                        f"{helper} timeout={timeout:g} exceeds pytest timeout={global_timeout:g} "
                        f"but pytest timeout marker is only {pytest_timeout:g}"
                    )

    assert violations == []


def test_cli_real_tg_timeout_policy_compares_marker_value(tmp_path):
    sample = tmp_path / "test_timeout.py"
    sample.write_text(
        """
import pytest

pytestmark = pytest.mark.real_tg_safe

@pytest.mark.timeout(60)
def test_bad(run_cli):
    run_cli("filter", "analyze", timeout=120)

@pytest.mark.timeout(180)
def test_good(run_cli):
    run_cli("filter", "analyze", timeout=120)

@pytest.mark.timeout(timeout=60)
def test_keyword_bad(run_cli):
    run_cli("filter", "analyze", timeout=120)
""",
        encoding="utf-8",
    )
    tree = ast.parse(sample.read_text(encoding="utf-8"), filename=str(sample))
    bad, good, keyword_bad = [node for node in tree.body if isinstance(node, ast.FunctionDef)]

    assert _function_timeout_marker_seconds(bad) == 60
    assert _function_timeout_marker_seconds(good) == 180
    assert _function_timeout_marker_seconds(keyword_bad) == 60


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
            if _is_cli_help_command(command):
                violations.append(
                    f"{path.relative_to(_REPO_ROOT)}:{lineno}: help command cannot satisfy live CLI coverage"
                )
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
