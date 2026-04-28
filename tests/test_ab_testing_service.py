from __future__ import annotations

import pytest

from src.models import ContentPipeline, PipelineGenerationBackend, PipelinePublishMode
from src.services.ab_testing_service import ABTestingService


class FakeQualityScoringService:
    def __init__(self, scores: list[float]):
        self._scores = scores
        self._idx = 0

    async def score_and_check(self, text: str):
        from src.services.quality_scoring_service import QualityScore

        score = self._scores[self._idx]
        self._idx += 1
        payload = QualityScore(
            relevance=score,
            language_quality=score,
            informativeness=score,
            structure=score,
            overall=score,
            issues=[],
        )
        return payload, score >= 0.7


@pytest.mark.anyio
async def test_ab_testing_service_save_and_get_variants(db):
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt-template")
    await repo.save_result(run_id, "base")

    await service.save_variants(run_id, ["base", "variant 2", "variant 3"])

    result = await service.get_variants(run_id)

    assert result is not None
    assert [variant.text for variant in result.variants] == ["base", "variant 2", "variant 3"]
    run = await repo.get(run_id)
    assert run is not None
    assert run.variants == ["base", "variant 2", "variant 3"]


@pytest.mark.anyio
async def test_ab_testing_service_select_variant_updates_generated_text(db):
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt-template")
    await repo.save_result(run_id, "base")
    await service.save_variants(run_id, ["base", "best variant"])

    await service.select_variant(run_id, 1)

    run = await repo.get(run_id)
    assert run is not None
    assert run.generated_text == "best variant"
    assert run.selected_variant == 1


@pytest.mark.anyio
async def test_ab_testing_service_auto_select_best_uses_scoring_service(db):
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt-template")
    await repo.save_result(run_id, "base")
    await service.save_variants(run_id, ["short", "best", "mid"])

    best_index = await service.auto_select_best(
        run_id,
        scoring_service=FakeQualityScoringService([0.2, 0.95, 0.5]),
    )

    run = await repo.get(run_id)
    assert best_index == 1
    assert run is not None
    assert run.generated_text == "best"
    assert run.selected_variant == 1


@pytest.mark.anyio
async def test_ab_testing_service_generate_variants_includes_base_text(db, monkeypatch):
    pipeline = ContentPipeline(
        id=1,
        name="Digest",
        prompt_template="Summarize {source_messages}",
        llm_model="test-model",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )
    service = ABTestingService(db)

    from src.services import provider_service

    async def fake_provider(**kwargs):
        return f"variant::{kwargs['prompt'][:12]}"

    monkeypatch.setattr(
        provider_service.AgentProviderService,
        "get_provider_callable",
        lambda self, model: fake_provider,
    )

    variants = await service.generate_variants(pipeline, "base text", num_variants=3)

    assert variants[0] == "base text"
    assert len(variants) == 3


@pytest.mark.anyio
async def test_ab_testing_service_select_variant_invalid_index(db):
    """Invalid index raises ValueError."""
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt")
    await repo.save_result(run_id, "base")
    await service.save_variants(run_id, ["base", "variant"])

    with pytest.raises(ValueError, match="Invalid variant index"):
        await service.select_variant(run_id, 5)


@pytest.mark.anyio
async def test_ab_testing_service_select_variant_missing_run(db):
    """Missing run raises ValueError."""
    service = ABTestingService(db)

    with pytest.raises(ValueError, match="Run 999 not found"):
        await service.select_variant(999, 0)


@pytest.mark.anyio
async def test_ab_testing_service_get_variants_missing_run(db):
    """get_variants returns None for missing run."""
    service = ABTestingService(db)

    result = await service.get_variants(999)

    assert result is None


@pytest.mark.anyio
async def test_ab_testing_service_get_variants_no_variants_returns_base(db):
    """get_variants returns generated_text when no variants stored."""
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt")
    await repo.save_result(run_id, "base text only")

    result = await service.get_variants(run_id)

    assert result is not None
    assert len(result.variants) == 1
    assert result.variants[0].text == "base text only"


@pytest.mark.anyio
async def test_ab_testing_service_auto_select_single_variant_returns_zero(db):
    """Auto-select with single variant returns 0."""
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt")
    await repo.save_result(run_id, "only one")
    # No variants saved, means only just generated_text

    best_index = await service.auto_select_best(run_id)

    assert best_index == 0


@pytest.mark.anyio
async def test_ab_testing_service_auto_select_without_scoring_picks_longest(db):
    """Auto-select without scoring picks longest text."""
    repo = db.repos.generation_runs
    service = ABTestingService(db)
    run_id = await repo.create_run(42, "prompt")
    await repo.save_result(run_id, "short")
    await service.save_variants(run_id, ["short", "medium length", "the longest one here"])

    best_index = await service.auto_select_best(run_id, scoring_service=None)

    assert best_index == 2  # Index of "the longest one here"
