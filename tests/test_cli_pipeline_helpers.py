"""Tests for CLI pipeline helper functions and other pure-logic CLI helpers."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.pipeline import _parse_target_refs, _preview_text
from src.services.pipeline_service import PipelineTargetRef, PipelineValidationError


# --- _parse_target_refs ---


def test_parse_target_refs_single():
    refs = _parse_target_refs(["+123|456"])
    assert len(refs) == 1
    assert refs[0].phone == "+123"
    assert refs[0].dialog_id == 456


def test_parse_target_refs_multiple():
    refs = _parse_target_refs(["+1|100", "+2|200"])
    assert len(refs) == 2
    assert refs[0].phone == "+1"
    assert refs[1].dialog_id == 200


def test_parse_target_refs_negative_id():
    refs = _parse_target_refs(["+1|-100"])
    assert refs[0].dialog_id == -100


def test_parse_target_refs_no_separator():
    with pytest.raises(PipelineValidationError, match="PHONE\\|DIALOG_ID"):
        _parse_target_refs(["invalid"])


def test_parse_target_refs_non_numeric_id():
    with pytest.raises(PipelineValidationError, match="numeric"):
        _parse_target_refs(["+1|abc"])


def test_parse_target_refs_empty_list():
    refs = _parse_target_refs([])
    assert refs == []


# --- _preview_text ---


def test_preview_text_none():
    assert _preview_text(None) == "—"


def test_preview_text_empty():
    assert _preview_text("") == "—"


def test_preview_text_short():
    assert _preview_text("hello") == "hello"


def test_preview_text_long():
    text = "a" * 100
    result = _preview_text(text, limit=60)
    assert len(result) == 60
    assert result.endswith("...")


def test_preview_text_exactly_limit():
    text = "a" * 60
    assert _preview_text(text, limit=60) == text


def test_preview_text_whitespace_normalization():
    assert _preview_text("  hello   world  ") == "hello world"


# --- CLI test.py constants and helpers ---


def test_cli_test_constants():
    from src.cli.commands.test import (
        PARALLEL_SAFE_PYTEST_COMMAND,
        SERIAL_PYTEST_COMMAND,
        TELEGRAM_DIALOG_TIMEOUT,
        TELEGRAM_SEARCH_TIMEOUT,
        TELEGRAM_TIMEOUT,
        _TG_CHECKS_AFTER_POOL,
    )

    assert TELEGRAM_TIMEOUT == 30
    assert TELEGRAM_DIALOG_TIMEOUT == 120
    assert TELEGRAM_SEARCH_TIMEOUT == 120
    assert "pytest" in SERIAL_PYTEST_COMMAND
    assert "not aiosqlite_serial" in PARALLEL_SAFE_PYTEST_COMMAND
    assert "tg_iter_messages" in _TG_CHECKS_AFTER_POOL


def test_cli_test_check_result():
    from src.cli.commands.test import CheckResult, Status

    r = CheckResult("test_check", Status.PASS, "ok")
    assert r.name == "test_check"
    assert r.status == Status.PASS
    assert r.detail == "ok"


def test_cli_test_status_enum():
    from src.cli.commands.test import Status

    assert Status.PASS.value == "PASS"
    assert Status.FAIL.value == "FAIL"
    assert Status.SKIP.value == "SKIP"


# --- CLI filter helpers (additional) ---


def test_cli_filter_parse_pks_more():
    from src.cli.commands.filter import _parse_pks

    assert _parse_pks("10,20,30") == [10, 20, 30]
    assert _parse_pks("0") == [0]
    assert _parse_pks("-1,5") == [-1, 5]


# --- CLI scheduler helpers ---


def test_cli_scheduler_import():
    from src.cli.commands import scheduler as mod

    assert hasattr(mod, "run")


# --- CLI collect import ---


def test_cli_collect_import():
    from src.cli.commands import collect as mod

    assert hasattr(mod, "run")


# --- CLI messages import ---


def test_cli_messages_import():
    from src.cli.commands import messages as mod

    assert hasattr(mod, "run")


# --- CLI account import ---


def test_cli_account_import():
    from src.cli.commands import account as mod

    assert hasattr(mod, "run")
