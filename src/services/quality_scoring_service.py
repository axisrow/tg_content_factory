from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database import Database

logger = logging.getLogger(__name__)

QUALITY_RUBRIC = """
Оцени качество контента по шкале от 0.0 до 1.0 по следующим критериям:

1. Релевантность (0.0-1.0): Насколько контент соответствует теме и запросу
2. Качество языка (0.0-1.0): Грамматика, стиль, читаемость
3. Информативность (0.0-1.0): Наличие полезной информации
4. Структура (0.0-1.0): Логичность изложения, форматирование

Ответ должен быть в формате JSON:
{
  "relevance": 0.8,
  "language_quality": 0.9,
  "informativeness": 0.7,
  "structure": 0.8,
  "overall": 0.8,
  "issues": ["краткое описание проблем"]
}
"""


@dataclass
class QualityScore:
    relevance: float
    language_quality: float
    informativeness: float
    structure: float
    overall: float
    issues: list[str]


class QualityScoringService:
    """Service for LLM-based content quality assessment.

    Uses rubric-based scoring to evaluate generated content before publication.
    Threshold can be configured per pipeline.
    """

    def __init__(
        self,
        db: Database,
        default_threshold: float = 0.7,
        provider_service=None,
    ):
        self._db = db
        self._default_threshold = default_threshold
        self._provider_service = provider_service

    async def score_content(
        self,
        text: str,
        model: str | None = None,
    ) -> QualityScore:
        """Score content quality using LLM.

        Args:
            text: The generated text to evaluate
            model: Optional model override

        Returns:
            QualityScore with rubric scores and overall rating
        """
        _default_score = QualityScore(
            relevance=0.5,
            language_quality=0.5,
            informativeness=0.5,
            structure=0.5,
            overall=0.5,
            issues=["Scoring failed"],
        )
        try:
            from src.services.provider_service import AgentProviderService
        except ImportError:
            logger.warning("Provider service not available for quality scoring")
            return _default_score

        try:
            provider_service = self._provider_service or AgentProviderService(self._db)
            provider_callable = provider_service.get_provider_callable(model)

            prompt = f"{QUALITY_RUBRIC}\n\nКонтент для оценки:\n{text}"

            result = await provider_callable(prompt=prompt, max_tokens=500, temperature=0.3)

            response_text = result if isinstance(result, str) else str(result)

            try:
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                if start >= 0 and end > start:
                    json_str = response_text[start:end]
                    data = json.loads(json_str)
                else:
                    data = {}
            except json.JSONDecodeError:
                data = {}

            return QualityScore(
                relevance=data.get("relevance", 0.5),
                language_quality=data.get("language_quality", 0.5),
                informativeness=data.get("informativeness", 0.5),
                structure=data.get("structure", 0.5),
                overall=data.get("overall", 0.5),
                issues=data.get("issues", []),
            )

        except (OSError, asyncio.TimeoutError) as exc:
            logger.warning("Quality scoring unavailable: %s", exc)
            return _default_score
        except Exception:
            logger.exception("Quality scoring failed")
            return _default_score

    def passes_threshold(self, score: QualityScore, threshold: float | None = None) -> bool:
        """Check if score passes the quality threshold.

        Args:
            score: The quality score to check
            threshold: Optional threshold override, or use default

        Returns:
            True if overall score >= threshold
        """
        threshold = threshold if threshold is not None else self._default_threshold
        return score.overall >= threshold

    async def score_and_check(
        self,
        text: str,
        threshold: float | None = None,
        model: str | None = None,
    ) -> tuple[QualityScore, bool]:
        """Score content and check if it passes threshold.

        Args:
            text: The generated text to evaluate
            threshold: Optional threshold override
            model: Optional model override

        Returns:
            Tuple of (QualityScore, passes_threshold)
        """
        score = await self.score_content(text, model)
        passes = self.passes_threshold(score, threshold)
        return score, passes
