from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database import Database
    from src.models import ContentPipeline

logger = logging.getLogger(__name__)


@dataclass
class Variant:
    index: int
    text: str
    score: float | None = None


@dataclass
class ABTestResult:
    run_id: int
    variants: list[Variant]
    selected_index: int | None = None


class ABTestingService:
    """Service for A/B testing generated content variants.

    Generates multiple variants and allows selection of the best one.
    """

    def __init__(self, db: Database, default_variants: int = 3):
        self._db = db
        self._default_variants = default_variants

    async def generate_variants(
        self,
        pipeline: ContentPipeline,
        base_text: str,
        num_variants: int | None = None,
    ) -> list[str]:
        """Generate multiple content variants.

        Args:
            pipeline: The pipeline configuration
            base_text: The base generated text to create variants from
            num_variants: Number of variants (default from config)

        Returns:
            List of variant texts
        """
        num_variants = num_variants or self._default_variants
        variants: list[str] = [base_text]

        try:
            from src.services.provider_service import RuntimeProviderRegistry
        except ImportError:
            logger.warning("Provider service not available for variant generation")
            return variants

        try:
            provider_service = RuntimeProviderRegistry(self._db)

            for i in range(1, num_variants):
                prompt = (
                    f"Перепиши следующий текст, сохраняя смысл, но изменив стиль и формулировки. "
                    f"Вариант {i + 1}:\n\n{base_text}"
                )

                provider_callable = provider_service.get_provider_callable(pipeline.llm_model)
                result = await provider_callable(
                    prompt=prompt,
                    max_tokens=1000,
                    temperature=0.8,
                )

                variant_text = result if isinstance(result, str) else str(result)
                variants.append(variant_text)

        except Exception:
            logger.exception("Failed to generate variants")

        return variants

    async def save_variants(
        self,
        run_id: int,
        variants: list[str],
    ) -> None:
        """Save variants to a generation run.

        Args:
            run_id: The generation run ID
            variants: List of variant texts
        """
        await self._db.repos.generation_runs.set_variants(run_id, variants)

    async def select_variant(
        self,
        run_id: int,
        variant_index: int,
    ) -> None:
        """Select a specific variant as the final content.

        Args:
            run_id: The generation run ID
            variant_index: Index of the selected variant
        """
        run = await self._db.repos.generation_runs.get(run_id)
        if run is None:
            raise ValueError(f"Run {run_id} not found")

        variants = run.variants
        if not variants:
            raise ValueError(f"Run {run_id} has no variants")

        if variant_index < 0 or variant_index >= len(variants):
            raise ValueError(f"Invalid variant index {variant_index}")

        selected_text = variants[variant_index]

        await self._db.repos.generation_runs.select_variant(run_id, variant_index, selected_text)

    async def get_variants(self, run_id: int) -> ABTestResult | None:
        """Get variants for a generation run.

        Args:
            run_id: The generation run ID

        Returns:
            ABTestResult with variants, or None if not found
        """
        run = await self._db.repos.generation_runs.get(run_id)
        if run is None:
            return None

        variants_data = run.variants
        if not variants_data:
            return ABTestResult(
                run_id=run_id,
                variants=[Variant(index=0, text=run.generated_text or "")],
            )

        variants = [
            Variant(index=i, text=text)
            for i, text in enumerate(variants_data)
        ]

        return ABTestResult(
            run_id=run_id,
            variants=variants,
            selected_index=run.selected_variant,
        )

    async def auto_select_best(
        self,
        run_id: int,
        scoring_service=None,
    ) -> int:
        """Automatically select the best variant based on quality scoring.

        Args:
            run_id: The generation run ID
            scoring_service: Optional QualityScoringService for scoring

        Returns:
            Index of the selected variant
        """
        result = await self.get_variants(run_id)
        if result is None or len(result.variants) <= 1:
            return 0

        if scoring_service is None:
            best_index = 0
            best_len = len(result.variants[0].text)
            for i, v in enumerate(result.variants):
                if len(v.text) > best_len:
                    best_len = len(v.text)
                    best_index = i
        else:
            best_index = 0
            best_score = 0.0
            for i, v in enumerate(result.variants):
                score, _ = await scoring_service.score_and_check(v.text)
                if score.overall > best_score:
                    best_score = score.overall
                    best_index = i

        await self.select_variant(run_id, best_index)
        return best_index
