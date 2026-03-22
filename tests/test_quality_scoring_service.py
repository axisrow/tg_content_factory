"""Tests for QualityScoringService."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.services.quality_scoring_service import QualityScoringService, QualityScore


@pytest.fixture
def mock_db():
    """Mock database."""
    return MagicMock()


@pytest.fixture
def scoring_service(mock_db):
    """QualityScoringService with default threshold 0.7."""
    return QualityScoringService(mock_db, default_threshold=0.7)


# === QualityScore dataclass tests ===


def test_quality_score_defaults():
    """QualityScore can be created with all fields."""
    score = QualityScore(
        relevance=0.8,
        language_quality=0.9,
        informativeness=0.7,
        structure=0.6,
        overall=0.75,
        issues=["Poor structure"],
    )
    assert score.relevance == 0.8
    assert score.language_quality == 0.9
    assert score.informativeness == 0.7
    assert score.structure == 0.6
    assert score.overall == 0.75
    assert score.issues == ["Poor structure"]


def test_quality_score_empty_issues():
    """QualityScore can have empty issues list."""
    score = QualityScore(
        relevance=0.5,
        language_quality=0.5,
        informativeness=0.5,
        structure=0.5,
        overall=0.5,
        issues=[],
    )
    assert score.issues == []


# === passes_threshold tests ===


def test_passes_threshold_above_default(scoring_service):
    """Score above default threshold passes."""
    score = QualityScore(
        relevance=0.8, language_quality=0.8, informativeness=0.8,
        structure=0.8, overall=0.8, issues=[],
    )
    assert scoring_service.passes_threshold(score) is True


def test_passes_threshold_below_default(scoring_service):
    """Score below default threshold fails."""
    score = QualityScore(
        relevance=0.5, language_quality=0.5, informativeness=0.5,
        structure=0.5, overall=0.5, issues=[],
    )
    assert scoring_service.passes_threshold(score) is False


def test_passes_threshold_at_boundary(scoring_service):
    """Score at exactly threshold passes."""
    score = QualityScore(
        relevance=0.7, language_quality=0.7, informativeness=0.7,
        structure=0.7, overall=0.7, issues=[],
    )
    assert scoring_service.passes_threshold(score) is True


def test_passes_threshold_custom_override(scoring_service):
    """Custom threshold overrides default."""
    score = QualityScore(
        relevance=0.65, language_quality=0.65, informativeness=0.65,
        structure=0.65, overall=0.65, issues=[],
    )
    # Below default 0.7, but above custom 0.6
    assert scoring_service.passes_threshold(score, threshold=0.6) is True
    assert scoring_service.passes_threshold(score) is False


def test_passes_threshold_none_uses_default(scoring_service):
    """None threshold uses default."""
    score = QualityScore(
        relevance=0.8, language_quality=0.8, informativeness=0.8,
        structure=0.8, overall=0.8, issues=[],
    )
    assert scoring_service.passes_threshold(score, threshold=None) is True


def test_passes_threshold_high_value():
    """High overall score always passes reasonable thresholds."""
    mock_db = MagicMock()
    service = QualityScoringService(mock_db, default_threshold=0.9)
    score = QualityScore(
        relevance=0.95, language_quality=0.95, informativeness=0.95,
        structure=0.95, overall=0.95, issues=[],
    )
    assert service.passes_threshold(score) is True


def test_passes_threshold_low_value():
    """Low overall score fails reasonable thresholds."""
    mock_db = MagicMock()
    service = QualityScoringService(mock_db, default_threshold=0.3)
    score = QualityScore(
        relevance=0.1, language_quality=0.1, informativeness=0.1,
        structure=0.1, overall=0.1, issues=[],
    )
    assert service.passes_threshold(score) is False


# === score_content tests (integration with default provider) ===


@pytest.mark.asyncio
async def test_score_content_uses_default_provider(mock_db):
    """Score content uses default provider which returns DRAFT prefix."""
    service = QualityScoringService(mock_db)
    score = await service.score_content("Test content")
    # Default provider returns DRAFT prefix, JSON parse fails -> returns defaults
    # No exception, so issues is empty (not "Scoring failed")
    assert score.overall == 0.5
    assert score.issues == []


@pytest.mark.asyncio
async def test_score_content_with_model_param(mock_db):
    """Score content passes model parameter."""
    service = QualityScoringService(mock_db)
    score = await service.score_content("Test content", model="gpt-4")
    # Default provider is still used since no real API
    assert score.overall == 0.5


# === score_and_check tests ===


@pytest.mark.asyncio
async def test_score_and_check_with_default_provider(mock_db):
    """Score and check with default provider."""
    service = QualityScoringService(mock_db)
    score, passes = await service.score_and_check("Test content")
    # Default provider -> defaults -> 0.5 < 0.7 threshold
    assert score.overall == 0.5
    assert passes is False


@pytest.mark.asyncio
async def test_score_and_check_custom_threshold_lower(mock_db):
    """Custom threshold lower than score passes."""
    service = QualityScoringService(mock_db)
    score, passes = await service.score_and_check("Test", threshold=0.3)
    # Default provider -> 0.5 >= 0.3
    assert passes is True


@pytest.mark.asyncio
async def test_score_and_check_custom_threshold_higher(mock_db):
    """Custom threshold higher than score fails."""
    service = QualityScoringService(mock_db)
    score, passes = await service.score_and_check("Test", threshold=0.9)
    # Default provider -> 0.5 < 0.9
    assert passes is False


# === Edge case tests ===


@pytest.mark.asyncio
async def test_score_content_empty_string(mock_db):
    """Handles empty content string."""
    service = QualityScoringService(mock_db)
    score = await service.score_content("")
    assert score.overall == 0.5


@pytest.mark.asyncio
async def test_score_content_long_string(mock_db):
    """Handles long content string."""
    service = QualityScoringService(mock_db)
    long_content = "x" * 10000
    score = await service.score_content(long_content)
    assert score.overall == 0.5


@pytest.mark.asyncio
async def test_score_and_check_with_model(mock_db):
    """Score and check with model parameter."""
    service = QualityScoringService(mock_db)
    score, passes = await service.score_and_check("Test", model="gpt-4")
    assert score.overall == 0.5


# === Default threshold tests ===


def test_default_threshold_parameter():
    """Service uses provided default threshold."""
    mock_db = MagicMock()
    service = QualityScoringService(mock_db, default_threshold=0.8)
    assert service._default_threshold == 0.8


def test_default_threshold_default_value():
    """Service uses 0.7 as default threshold if not specified."""
    mock_db = MagicMock()
    service = QualityScoringService(mock_db)
    assert service._default_threshold == 0.7


# === QualityScore issues tests ===


def test_quality_score_multiple_issues():
    """QualityScore can have multiple issues."""
    score = QualityScore(
        relevance=0.5,
        language_quality=0.5,
        informativeness=0.5,
        structure=0.5,
        overall=0.5,
        issues=["Issue 1", "Issue 2", "Issue 3"],
    )
    assert len(score.issues) == 3


def test_quality_score_zero_values():
    """QualityScore can have zero values."""
    score = QualityScore(
        relevance=0.0,
        language_quality=0.0,
        informativeness=0.0,
        structure=0.0,
        overall=0.0,
        issues=["All zeros"],
    )
    assert score.overall == 0.0


def test_quality_score_max_values():
    """QualityScore can have max values."""
    score = QualityScore(
        relevance=1.0,
        language_quality=1.0,
        informativeness=1.0,
        structure=1.0,
        overall=1.0,
        issues=[],
    )
    assert score.overall == 1.0
