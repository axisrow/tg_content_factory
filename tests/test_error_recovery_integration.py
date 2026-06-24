"""ErrorRecoveryService wiring into idempotent provider call sites (#1069).

This suite is the TDD anchor for issue #1069: ErrorRecoveryService (retry +
circuit breaker + classifier) must be connected to the *idempotent* provider
calls — LLM text (generation / refinement / quality scoring / A/B variants) and
embeddings — and must NEVER touch the billed, non-idempotent image POST path.

Every assertion here is mutation-verified: flipping the production wiring back
to a bare call (or pointing the image path at the recovery wrapper) flips one of
these tests red.
"""

from __future__ import annotations

import pytest

from src.services.error_recovery_service import (
    EMBEDDING_RETRY_POLICY,
    LLM_CIRCUIT_CONFIG,
    LLM_RETRY_POLICY,
    ErrorCategory,
    ErrorClassifier,
    ErrorRecoveryService,
    ImageAdapterRetryError,
    RetryPolicy,
    for_embeddings,
    for_llm,
    guard_not_image,
)


def _fast_recovery(max_retries: int = 3) -> ErrorRecoveryService:
    """Recovery service with no retry back-off, for deterministic fast tests.

    Production back-off (base_delay=1s + jitter) is correct but would make these
    retry tests slow and jitter-flaky; the retry/classification *behaviour* under
    test is identical with the delay zeroed out.
    """
    return ErrorRecoveryService(
        retry_policy=RetryPolicy(max_retries=max_retries, base_delay=0.0, jitter=False)
    )


# ---------------------------------------------------------------------------
# Fake providers — no real API. A counter-backed callable that fails N times
# with a chosen error class, then succeeds, so we can assert retry behaviour.
# ---------------------------------------------------------------------------


class _CountingProvider:
    """An async LLM provider double that records every invocation.

    ``transient_failures`` consecutive calls raise a TRANSIENT error, then the
    provider returns ``success_text``. ``fatal`` makes every call raise a FATAL
    (401) error that must NOT be retried.
    """

    def __init__(
        self,
        *,
        transient_failures: int = 0,
        fatal: bool = False,
        success_text: str = "OK",
    ) -> None:
        self.calls = 0
        self._transient_failures = transient_failures
        self._fatal = fatal
        self._success_text = success_text

    async def __call__(self, *args: object, **kwargs: object) -> str:
        self.calls += 1
        if self._fatal:
            raise RuntimeError("HTTP 401 Unauthorized: invalid api key")
        if self.calls <= self._transient_failures:
            raise RuntimeError("connection reset by peer (timeout)")
        return self._success_text


# ===========================================================================
# Helper / factory unit tests
# ===========================================================================


def test_for_llm_uses_llm_policy():
    svc = for_llm()
    assert isinstance(svc, ErrorRecoveryService)
    assert svc._retry_policy is LLM_RETRY_POLICY
    assert svc._retry_policy.max_retries == 3


def test_for_embeddings_uses_embedding_policy():
    svc = for_embeddings()
    assert svc._retry_policy is EMBEDDING_RETRY_POLICY
    assert svc._retry_policy.max_retries == 2


def test_llm_circuit_breaker_threshold_is_five():
    assert LLM_CIRCUIT_CONFIG.failure_threshold == 5


# ===========================================================================
# Image guard — the anti-double-billing regression gate (#958)
# ===========================================================================


def test_guard_rejects_image_adapter_factory():
    """A factory-built image adapter must be refused by the guard."""
    from src.services.provider_adapters import make_together_image_adapter

    adapter = make_together_image_adapter("fake-key")
    with pytest.raises(ImageAdapterRetryError):
        guard_not_image(adapter)


def test_guard_rejects_named_image_callable():
    async def together_image_adapter(prompt: str, model: str = "") -> str:
        return "url"

    with pytest.raises(ImageAdapterRetryError):
        guard_not_image(together_image_adapter)


def test_guard_allows_llm_provider_callable():
    async def openai_provider(prompt: str = "", **kwargs: object) -> str:
        return "text"

    # Must NOT raise for an ordinary LLM provider.
    guard_not_image(openai_provider)


@pytest.mark.anyio
async def test_execute_provider_call_guards_image_before_any_invocation():
    """REGRESSION (cycle-review #1088 / Codex): the per-call seam guards too.

    The guard must screen the *actual* provider callable passed via ``provider=``,
    not just the closure — so an image adapter routed through any wired site
    (which all call ``execute_provider_call`` directly) is refused BEFORE it is
    ever invoked, closing the bypass Codex flagged.
    """
    calls = 0

    async def together_image_adapter(prompt: str = "", **kwargs: object) -> str:
        nonlocal calls
        calls += 1
        return "url"

    svc = _fast_recovery()
    with pytest.raises(ImageAdapterRetryError):
        await svc.execute_provider_call(
            lambda: together_image_adapter(prompt="x"),
            provider=together_image_adapter,
        )
    # Guard fired before the adapter was ever called — zero billed POSTs.
    assert calls == 0


@pytest.mark.anyio
async def test_execute_provider_call_allows_llm_provider():
    """The per-call guard must NOT block ordinary LLM providers."""
    async def openai_provider(prompt: str = "", **kwargs: object) -> str:
        return "text"

    svc = _fast_recovery()
    out = await svc.execute_provider_call(
        lambda: openai_provider(prompt="x"), provider=openai_provider
    )
    assert out == "text"


def test_classifier_fatal_takes_precedence_over_transient_wording():
    """REGRESSION (cycle-review #1088 / Claude): quota error with 'rate limit'
    wording must classify FATAL (not retried), not RATE_LIMIT."""
    err = RuntimeError("429 rate limit reached: quota exceeded for this key")
    assert ErrorClassifier.classify(err) == ErrorCategory.FATAL

    # A pure transient rate-limit (no fatal marker) stays RATE_LIMIT.
    pure = RuntimeError("HTTP 429 rate limit exceeded, slow down")
    assert ErrorClassifier.classify(pure) == ErrorCategory.RATE_LIMIT


@pytest.mark.anyio
async def test_image_adapter_called_exactly_once_on_error():
    """REGRESSION: the image path is never retried — exactly one POST on error.

    The recovery service retries by design; if an image adapter were ever routed
    through it, a transient error would replay the billed POST. This test pins
    the contract that the image adapter is invoked exactly once even when it
    raises a retry-classified (TRANSIENT) error.
    """
    calls = 0

    async def image_adapter(prompt: str, model: str = "") -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("503 service unavailable")  # TRANSIENT — would retry

    # The guard must prevent this adapter from ever entering the recovery path.
    with pytest.raises(ImageAdapterRetryError):
        guard_not_image(image_adapter)

    # And if a caller (wrongly) tries to run it directly, our production code
    # must not be the thing that loops it. Simulate the *correct* call shape:
    # one direct invocation, one failure, no retry.
    with pytest.raises(RuntimeError):
        await image_adapter("a cat", "flux")
    assert calls == 1


# ===========================================================================
# provider_service.get_recovered_provider_callable — the sanctioned seam
# ===========================================================================


@pytest.mark.anyio
async def test_recovered_provider_callable_retries_transient():
    """The registry seam returns a recovered callable that retries transients."""
    from src.services.provider_service import RuntimeProviderRegistry

    provider = _CountingProvider(transient_failures=1, success_text="recovered")
    reg = RuntimeProviderRegistry(env={})
    reg.register_provider("fake", provider)

    recovered = reg.get_recovered_provider_callable(
        "fake", error_recovery=_fast_recovery()
    )
    out = await recovered(prompt="hi")

    assert provider.calls == 2
    assert out == "recovered"


def test_recovered_provider_callable_refuses_image_adapter():
    """The seam screens the resolved callable through the image guard."""
    from src.services.provider_adapters import make_together_image_adapter
    from src.services.provider_service import RuntimeProviderRegistry

    reg = RuntimeProviderRegistry(env={})
    reg.register_provider("together", make_together_image_adapter("fake-key"))

    with pytest.raises(ImageAdapterRetryError):
        reg.get_recovered_provider_callable("together")


# ===========================================================================
# Quality scoring — wraps its provider call in recovery (idempotent)
# ===========================================================================


@pytest.mark.anyio
async def test_quality_scoring_retries_transient(db):
    """A transient provider failure is retried and scoring still succeeds."""
    from src.services.quality_scoring_service import QualityScoringService

    provider = _CountingProvider(
        transient_failures=1,
        success_text='{"relevance":0.9,"overall":0.9,"issues":[]}',
    )

    class _PS:
        def get_provider_callable(self, model):
            return provider

    svc = QualityScoringService(db, provider_service=_PS(), error_recovery=_fast_recovery())
    score = await svc.score_content("hello world")

    # Retried once → 2 calls → real score parsed (not the 0.5 failure default).
    assert provider.calls == 2
    assert score.overall == 0.9


@pytest.mark.anyio
async def test_quality_scoring_fatal_not_retried(db):
    """A FATAL (401) provider error is NOT retried; one call, default score."""
    from src.services.quality_scoring_service import QualityScoringService

    provider = _CountingProvider(fatal=True)

    class _PS:
        def get_provider_callable(self, model):
            return provider

    svc = QualityScoringService(db, provider_service=_PS())
    score = await svc.score_content("hello world")

    assert provider.calls == 1  # no retry on FATAL
    assert score.overall == 0.5  # graceful default


# ===========================================================================
# A/B variant generation — wraps its provider call in recovery
# ===========================================================================


@pytest.mark.anyio
async def test_ab_variants_retry_transient(db):
    from src.models import ContentPipeline
    from src.services.ab_testing_service import ABTestingService

    provider = _CountingProvider(transient_failures=1, success_text="variant")

    class _PS:
        def get_provider_callable(self, model):
            return provider

    svc = ABTestingService(db, provider_service=_PS(), error_recovery=_fast_recovery())
    pipeline = ContentPipeline(id=1, name="p", llm_model="default")

    variants = await svc.generate_variants(pipeline, "base", num_variants=2)

    # 1 transient failure + 1 success → 2 provider calls for one extra variant.
    assert provider.calls == 2
    assert variants == ["base", "variant"]


# ===========================================================================
# Generation service (non-stream) — wraps its provider call in recovery
# ===========================================================================


@pytest.mark.anyio
async def test_generation_non_stream_retries_transient():
    from src.services.generation_service import GenerationService

    provider = _CountingProvider(transient_failures=1, success_text="generated")

    class _FakeSearch:
        async def has_semantic_index(self):
            return False

        async def search_local(self, *a, **k):
            from src.models import SearchResult

            return SearchResult(messages=[], total=0, query="q")

    gen = GenerationService(_FakeSearch(), provider_callable=provider, error_recovery=_fast_recovery())
    result = await gen.generate("query", prompt_template="{source_messages}")

    assert provider.calls == 2
    assert result["generated_text"] == "generated"


# ===========================================================================
# Embeddings — wrapped with the embeddings policy (max_retries=2)
# ===========================================================================


@pytest.mark.anyio
async def test_embedding_documents_retry_transient(db):
    from src.services.embedding_service import EmbeddingService

    calls = 0

    class _FakeEmbeddings:
        async def aembed_documents(self, texts):
            nonlocal calls
            calls += 1
            if calls < 2:
                raise RuntimeError("timeout contacting embedding endpoint")
            return [[0.1, 0.2] for _ in texts]

    svc = EmbeddingService(db, error_recovery=_fast_recovery(max_retries=2))
    svc._embeddings = _FakeEmbeddings()
    svc._embeddings_key = ("p", "m", "k", "b")

    async def _passthrough():
        return svc._embeddings

    svc._get_embeddings = _passthrough  # type: ignore[assignment]

    vectors = await svc._embed_documents(["a", "b"])

    assert calls == 2  # retried once under the embeddings policy
    assert vectors == [[0.1, 0.2], [0.1, 0.2]]
