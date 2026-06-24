"""Integration tests for A/B variant generation inside ContentGenerationService.

Issue #1068: wire the (previously unused) ABTestingService into the content
generation cycle. The integration point is ContentGenerationService.generate():
after save_result and before/around quality scoring, if pipeline.ab_num_variants
> 1, generate N variants, persist them, and (optionally) auto-select the best.

These tests exercise the real in-memory ``db`` fixture (so the real
generation_runs.set_variants / select_variant code paths run) and a fake LLM
provider — no real API calls.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import (
    ContentPipeline,
    Message,
    PipelineGenerationBackend,
    PipelinePublishMode,
    SearchResult,
)
from src.services.content_generation_service import ContentGenerationService


class DummySearchEngine:
    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


def _msg() -> Message:
    return Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text="Hello world from test",
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
        channel_username="testchan",
    )


def _pipeline(*, ab_num_variants: int = 1, ab_auto_select: bool = False) -> ContentPipeline:
    return ContentPipeline(
        id=1,
        name="Test Pipeline",
        prompt_template="Use {source_messages}",
        llm_model="test-model",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        ab_num_variants=ab_num_variants,
        ab_auto_select=ab_auto_select,
    )


class FakeProviderService:
    """Returns a deterministic variant text per call so auto-select is testable.

    The base text is produced by the RAG provider callable; the variant prompts
    asking to rewrite the base text get successively longer outputs so the
    length-based auto-select picks the last variant.
    """

    def __init__(self, variant_texts: list[str]):
        self._variant_texts = list(variant_texts)
        self._idx = 0
        self.prompts: list[str] = []

    def get_provider_callable(self, model):
        async def _call(*args, **kwargs):
            prompt = kwargs.get("prompt")
            if prompt is None and args:
                prompt = args[0]
            self.prompts.append(prompt or "")
            # RAG base generation uses prompt_template containing "Use";
            # variant generation uses the "Перепиши" rewrite prompt.
            if prompt and "Перепиши" in prompt:
                text = self._variant_texts[self._idx]
                self._idx += 1
                return text
            return "BASE TEXT"

        return _call

    def has_providers(self) -> bool:
        return True


class FakeQualityService:
    """Scores texts by a lookup so the best variant is deterministic."""

    def __init__(self, scores: dict[str, float]):
        self._scores = scores
        self.scored: list[str] = []

    async def score_and_check(self, text: str):
        from src.services.quality_scoring_service import QualityScore

        self.scored.append(text)
        value = self._scores.get(text, 0.0)
        return (
            QualityScore(
                relevance=value,
                language_quality=value,
                informativeness=value,
                structure=value,
                overall=value,
                issues=[],
            ),
            value >= 0.7,
        )

    async def score_content(self, text: str, model=None):
        from src.services.quality_scoring_service import QualityScore

        value = self._scores.get(text, 0.0)
        return QualityScore(
            relevance=value,
            language_quality=value,
            informativeness=value,
            structure=value,
            overall=value,
            issues=[],
        )


@pytest.mark.anyio
async def test_generate_creates_variants_when_ab_enabled(db):
    """ab_num_variants=3 → generation_runs.variants holds 3 entries (base + 2)."""
    engine = DummySearchEngine([_msg()])
    provider = FakeProviderService(["VARIANT TWO", "VARIANT THREE LONGER"])
    service = ContentGenerationService(db, engine, provider_service=provider)

    run = await service.generate(_pipeline(ab_num_variants=3))

    assert run.variants is not None
    assert len(run.variants) == 3
    assert run.variants[0] == "BASE TEXT"
    assert run.variants[1] == "VARIANT TWO"
    assert run.variants[2] == "VARIANT THREE LONGER"
    # No auto-select → generated_text stays the base, selected_variant unset.
    assert run.generated_text == "BASE TEXT"
    assert run.selected_variant is None


@pytest.mark.anyio
async def test_generate_no_variants_when_ab_disabled(db):
    """Default ab_num_variants=1 → no variant generation, variants stays None."""
    engine = DummySearchEngine([_msg()])
    provider = FakeProviderService([])
    service = ContentGenerationService(db, engine, provider_service=provider)

    run = await service.generate(_pipeline(ab_num_variants=1))

    assert run.variants is None
    assert run.selected_variant is None
    # Only the base generation prompt ran — no rewrite prompts.
    assert all("Перепиши" not in p for p in provider.prompts)


@pytest.mark.anyio
async def test_generate_auto_selects_best_variant_by_quality(db):
    """ab_auto_select=True → best variant by quality score becomes generated_text."""
    engine = DummySearchEngine([_msg()])
    provider = FakeProviderService(["VARIANT TWO", "VARIANT THREE"])
    quality = FakeQualityService(
        {"BASE TEXT": 0.3, "VARIANT TWO": 0.95, "VARIANT THREE": 0.5}
    )
    service = ContentGenerationService(
        db, engine, provider_service=provider, quality_service=quality
    )

    run = await service.generate(_pipeline(ab_num_variants=3, ab_auto_select=True))

    assert run.variants == ["BASE TEXT", "VARIANT TWO", "VARIANT THREE"]
    assert run.selected_variant == 1
    assert run.generated_text == "VARIANT TWO"


@pytest.mark.anyio
async def test_generate_dry_run_skips_variants(db):
    """dry_run must not generate variants (saves ×N token cost)."""
    engine = DummySearchEngine([_msg()])
    provider = FakeProviderService(["VARIANT TWO", "VARIANT THREE"])
    service = ContentGenerationService(db, engine, provider_service=provider)

    run = await service.generate(_pipeline(ab_num_variants=3), dry_run=True)

    assert run.variants is None
    assert all("Перепиши" not in p for p in provider.prompts)


@pytest.mark.anyio
async def test_generate_quality_scored_on_selected_variant(db):
    """When auto-select picks a variant, quality_score reflects the SELECTED text,
    not the base — scoring must run after the variant is chosen."""
    engine = DummySearchEngine([_msg()])
    provider = FakeProviderService(["WINNER VARIANT"])
    quality = FakeQualityService({"BASE TEXT": 0.2, "WINNER VARIANT": 0.9})
    service = ContentGenerationService(
        db, engine, provider_service=provider, quality_service=quality
    )

    run = await service.generate(_pipeline(ab_num_variants=2, ab_auto_select=True))

    assert run.generated_text == "WINNER VARIANT"
    assert run.quality_score == 0.9


class _RecordingImageService:
    """Records the text it was asked to render an image for."""

    def __init__(self):
        self.rendered_for: list[str] = []

    async def generate(self, model, text):
        self.rendered_for.append(text)
        return "http://img/example.png"


@pytest.mark.anyio
async def test_image_generated_from_selected_variant_not_base(db):
    """Image generation must run AFTER A/B auto-select so the image matches the
    finally-published text, not a discarded base variant (review: Codex)."""
    engine = DummySearchEngine([_msg()])
    provider = FakeProviderService(["WINNER VARIANT"])
    quality = FakeQualityService({"BASE TEXT": 0.2, "WINNER VARIANT": 0.9})
    image_service = _RecordingImageService()
    service = ContentGenerationService(
        db,
        engine,
        provider_service=provider,
        quality_service=quality,
        image_service=image_service,
    )
    pipeline = _pipeline(ab_num_variants=2, ab_auto_select=True)
    pipeline = pipeline.model_copy(update={"image_model": "test:img"})

    run = await service.generate(pipeline)

    assert run.generated_text == "WINNER VARIANT"
    # The image was rendered for the selected variant, never the base text.
    assert image_service.rendered_for == ["WINNER VARIANT"]
    assert run.image_url == "http://img/example.png"
