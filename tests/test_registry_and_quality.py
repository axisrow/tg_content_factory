"""Tests for agent tool registry helpers and quality scoring service."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent.tools._registry import (
    _text_response,
    normalize_phone,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_phone,
)

# --- normalize_phone ---


def test_normalize_phone_adds_plus():
    assert normalize_phone("79991234567") == "+79991234567"


def test_normalize_phone_keeps_plus():
    assert normalize_phone("+79991234567") == "+79991234567"


def test_normalize_phone_strips():
    assert normalize_phone("  +79991234567  ") == "+79991234567"


def test_normalize_phone_empty():
    assert normalize_phone("") == ""


def test_normalize_phone_whitespace_only():
    assert normalize_phone("   ") == ""


# --- _text_response ---


def test_text_response():
    r = _text_response("hello")
    assert r["content"][0]["type"] == "text"
    assert r["content"][0]["text"] == "hello"


# --- require_confirmation ---


def test_require_confirmation_not_confirmed():
    r = require_confirmation("удалит X", {})
    assert r is not None
    assert "confirm" in r["content"][0]["text"]


def test_require_confirmation_confirmed():
    assert require_confirmation("удалит X", {"confirm": True}) is None


# --- require_pool ---


def test_require_pool_present():
    assert require_pool(MagicMock()) is None


def test_require_pool_none():
    r = require_pool(None, "Test")
    assert r is not None
    assert "Test" in r["content"][0]["text"]


# --- resolve_phone ---


async def test_resolve_phone_explicit():
    phone, err = await resolve_phone(MagicMock(), "+123")
    assert phone == "+123"
    assert err is None


async def test_resolve_phone_empty_uses_primary():
    acc = MagicMock()
    acc.is_primary = True
    acc.phone = "+999"
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[acc])
    phone, err = await resolve_phone(db, "")
    assert phone == "+999"
    assert err is None


async def test_resolve_phone_empty_no_accounts():
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[])
    phone, err = await resolve_phone(db, "")
    assert phone == ""
    assert err is not None


async def test_resolve_phone_empty_exception():
    db = MagicMock()
    db.get_accounts = AsyncMock(side_effect=Exception("fail"))
    phone, err = await resolve_phone(db, "")
    assert phone == ""
    assert err is not None


async def test_resolve_phone_no_primary_uses_first():
    acc = MagicMock()
    acc.is_primary = False
    acc.phone = "+111"
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[acc])
    phone, err = await resolve_phone(db, "")
    assert phone == "+111"


# --- require_phone_permission ---


async def test_phone_permission_no_setting():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    result = await require_phone_permission(db, "+1", "test_tool")
    assert result is None


async def test_phone_permission_allowed():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=json.dumps({"+1": {"test_tool": True}}))
    result = await require_phone_permission(db, "+1", "test_tool")
    assert result is None


async def test_phone_permission_denied():
    db = MagicMock()
    # +2 is in perms but test_tool is False for +2 (but True for +1)
    db.get_setting = AsyncMock(return_value=json.dumps({
        "+1": {"test_tool": True},
        "+2": {"test_tool": False},
    }))
    result = await require_phone_permission(db, "+2", "test_tool")
    assert result is not None
    assert "+2" in result["content"][0]["text"]


async def test_phone_permission_malformed_json():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="not json")
    result = await require_phone_permission(db, "+1", "test_tool")
    assert result is not None


async def test_phone_permission_exception():
    db = MagicMock()
    db.get_setting = AsyncMock(side_effect=Exception("fail"))
    result = await require_phone_permission(db, "+1", "test_tool")
    assert result is not None


async def test_phone_permission_phone_not_in_perms():
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=json.dumps({"+1": {"test_tool": True}}))
    # +3 is not in perms at all → defaults to allowed
    result = await require_phone_permission(db, "+3", "test_tool")
    assert result is None


# --- QualityScoringService ---


def test_quality_score_passes_threshold():
    from src.services.quality_scoring_service import QualityScore

    score = QualityScore(
        relevance=0.8, language_quality=0.9, informativeness=0.7,
        structure=0.8, overall=0.8, issues=[],
    )
    assert score.overall == 0.8


async def test_quality_scoring_passes():
    from src.services.quality_scoring_service import QualityScore, QualityScoringService

    svc = QualityScoringService(db=MagicMock(), default_threshold=0.7)
    score = QualityScore(
        relevance=0.8, language_quality=0.8, informativeness=0.8,
        structure=0.8, overall=0.8, issues=[],
    )
    assert svc.passes_threshold(score) is True
    assert svc.passes_threshold(score, threshold=0.9) is False


async def test_quality_scoring_no_provider():
    from src.services.quality_scoring_service import QualityScoringService

    svc = QualityScoringService(db=MagicMock())
    with patch("src.services.quality_scoring_service.AgentProviderService", create=True):
        # Provider raises exception
        mock_ps = MagicMock()
        mock_ps.get_provider_callable.return_value = AsyncMock(side_effect=OSError("fail"))
        svc._provider_service = mock_ps
        score = await svc.score_content("test text")
        assert score.overall == 0.5
        assert "Scoring failed" in score.issues


async def test_quality_scoring_json_parse():
    from src.services.quality_scoring_service import QualityScoringService

    svc = QualityScoringService(db=MagicMock())
    mock_callable = AsyncMock(return_value=json.dumps({
        "relevance": 0.9, "language_quality": 0.8, "informativeness": 0.7,
        "structure": 0.6, "overall": 0.75, "issues": ["minor issue"],
    }))
    mock_ps = MagicMock()
    mock_ps.get_provider_callable.return_value = mock_callable
    svc._provider_service = mock_ps
    score = await svc.score_content("test text")
    assert score.overall == 0.75
    assert score.relevance == 0.9


async def test_quality_scoring_score_and_check():
    from src.services.quality_scoring_service import QualityScoringService

    svc = QualityScoringService(db=MagicMock(), default_threshold=0.6)
    mock_callable = AsyncMock(return_value=json.dumps({"overall": 0.8}))
    mock_ps = MagicMock()
    mock_ps.get_provider_callable.return_value = mock_callable
    svc._provider_service = mock_ps
    score, passes = await svc.score_and_check("test")
    assert passes is True
