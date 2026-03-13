from __future__ import annotations

from pathlib import Path

import pytest

from tests.conftest import (
    REAL_TG_LIVE_FIXTURE,
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

    for path in _TESTS_DIR.glob("test_*.py"):
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

    for path in _TESTS_DIR.glob("test_*.py"):
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

    for path in _TESTS_DIR.glob("test_*.py"):
        if path.name in _AUDIT_EXCLUDED_FILES:
            continue
        content = path.read_text(encoding="utf-8")
        if REAL_TG_LIVE_FIXTURE not in content:
            continue
        if not any(marker in content for marker in _SAFE_MARKER_USAGES + _MANUAL_MARKER_USAGES):
            violations.append(path.name)

    assert violations == []
